#include "dbmw/async/dbmw_async.h"
#include "dbmw/common/logger.h"
#include "dbmw/dbmw.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

// ============================================================================
// dbmw v0.2.0 异步执行管线。
//
// 结构总览：
//   - 引擎全局状态：执行器 / 完成调度器 / 在途计数（排水用）/ 停机标志。
//   - detail::OpState：Handle 的实现载体（状态机 + 取消路由 + 完成一次性标记）。
//   - detail::AsyncEngine：全部静态成员。DataSource 与 Handle 授予 friend，
//     引擎得以复用同步路径的全部私有原语（闸门/熔断/路由/缓存/写缓冲），
//     保证两条路径语义同源（设计 R1：异步 vs 同步结果一致性）。
//
// 不变量：
//   I1 完成回调绝不在池锁内、绝不在发起调用的栈上执行（经完成调度器投递）。
//   I2 结果值语义整体 move（shared_ptr 装箱穿越 std::function 的可拷贝约束）。
//   I3 治理闸门（审计/限流）只在调用线程执行一次。
//   I4 同步路径零改动（本文件不触碰同步循环，重试经 postAfter 外置）。
// ============================================================================

namespace dbmw::async {
    namespace {
        // ---- 引擎全局状态 ----
        //
        // gMtx 保护执行器指针的替换与在途计数；gDrainCv 用于 shutdown 排水
        // （等待在途操作归零）。短临界区，不与任何用户回调嵌套。
        std::mutex gMtx;
        std::condition_variable gDrainCv;
        std::shared_ptr<IExecutor> gExecutor;
        std::shared_ptr<IExecutor> gCompletion;
        std::size_t gInFlight = 0;
        std::atomic<bool> gStopping{false};
        std::atomic<std::int64_t> gDefaultTimeoutMs{0};

        // 完成队列过载/停止时的保底调度器。用单独线程保证用户回调
        // 绝不回退到发起调用的栈上执行，同时避免每次过载都新建线程。
        class CompletionFallback final {
        public:
            CompletionFallback() : worker_([this] { run(); }) {}
            ~CompletionFallback() {
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    stopping_ = true;
                }
                cv_.notify_one();
                if (worker_.joinable()) worker_.join();
            }

            void post(std::function<void()> task) {
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    queue_.push_back(std::move(task));
                }
                cv_.notify_one();
            }

        private:
            void run() {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lk(mtx_);
                        cv_.wait(lk, [this] { return stopping_ || !queue_.empty(); });
                        if (queue_.empty() && stopping_) return;
                        task = std::move(queue_.front());
                        queue_.pop_front();
                    }
                    guardedRun(task);
                }
            }

            static void guardedRun(const std::function<void()> &task) {
                try {
                    if (task) task();
                } catch (const std::exception &e) {
                    DBMW_LOG_ERROR(std::string("async completion callback threw: ") + e.what());
                } catch (...) {
                    DBMW_LOG_ERROR("async completion callback threw an unknown exception");
                }
            }

            std::mutex mtx_;
            std::condition_variable cv_;
            std::deque<std::function<void()> > queue_;
            bool stopping_ = false;
            std::thread worker_;
        };

        CompletionFallback &completionFallback() {
            static CompletionFallback dispatcher;
            return dispatcher;
        }

        std::shared_ptr<IExecutor> workerExecutor() {
            std::lock_guard<std::mutex> lk(gMtx);
            return gExecutor;
        }

        std::shared_ptr<IExecutor> completionExecutor() {
            std::lock_guard<std::mutex> lk(gMtx);
            return gCompletion;
        }

        // ---- 在途操作注册表（计数 + 排水通知）----

        void registryAdd() {
            std::lock_guard<std::mutex> lk(gMtx);
            ++gInFlight;
        }

        void registryRemove() {
            std::lock_guard<std::mutex> lk(gMtx);
            if (gInFlight > 0) --gInFlight;
            gDrainCv.notify_all();
        }

        // ---- 投递辅助 ----

        // 把任务投给执行器；投不出去（停机/队列满）就当场执行。
        // 丢回调会让 future 永久悬空——宁可退化为内联执行，绝不静默丢弃。
        void postOrRun(const std::shared_ptr<IExecutor> &ex, const std::function<void()> &task) {
            if (ex && ex->tryPost(task)) return; // 拷贝提交；失败时 task 未被消耗
            task();
        }

        void postCompletion(const std::shared_ptr<IExecutor> &ex,
                            const std::function<void()> &task) {
            if (ex && ex->tryPost(task)) return;
            completionFallback().post(task);
        }

        // 池的 AsyncIo：post/deliver 都指向 worker 执行器（借出结果回来后
        // 还要在 worker 上继续跑 step2 的阻塞 IO，不能占用完成调度器）。
        //
        // post 用 tryPost + 内联兜底：池内部任务（建连/ping）的完成绝不能被
        // 静默丢弃——丢回调会让 future 永久悬空。队列满时退化为在当前线程
        // （发起借出的 worker）上执行，代价由该 worker 承担而非丢失任务。
        // 注意不能借道 postAfter：到期任务如今在 timer 线程内联执行，
        // 而建连是阻塞 IO，会卡死所有定时器（超时检查/重试退避）。
        core::AsyncIo makePoolIo(const std::shared_ptr<IExecutor> &ex) {
            core::AsyncIo io;
            io.post = [ex](std::function<void()> task) {
                postOrRun(ex, std::move(task));
            };
            io.deliver = [ex](const std::function<void()> &task) {
                postOrRun(ex, task);
            };
            return io;
        }

        // 引擎步骤的兜底护栏：任何异常只记日志，绝不让它逃逸到
        // 池的 deliver 内联路径或定时线程（那里没有 runGuarded 保护）。
        template<class F>
        void guarded(F &&f) {
            try {
                f();
            } catch (const std::exception &e) {
                DBMW_LOG_ERROR(std::string("async engine step threw: ") + e.what());
            } catch (...) {
                DBMW_LOG_ERROR("async engine step threw an unknown exception");
            }
        }

        std::shared_ptr<core::DataSource> resolve(const std::string &name) {
            return DBMW::dataSource(name); // 空串 = 默认数据源；不存在返回 null
        }
    } // namespace

    namespace detail {
        // Handle 的实现载体（设计 §8.4）。
        struct OpState {
            std::atomic<Handle::State> state{Handle::State::Queued};
            std::atomic<bool> userCancelled{false};
            std::atomic<bool> timedOut{false};
            // 超时任务的 cancel 是否送达驱动（未送达时结果 message 附提示）。
            std::atomic<bool> cancelDelivered{false};
            // 完成一次性标记：finish 路径（正常完成 / 超时改写 / Overloaded /
            // 取消）共用，CAS 保证用户回调恰好投递一次。
            std::atomic<bool> finished{false};

            // 取消路由：worker 借到连接后装载，语句结束/finish 前清空。
            // mutex 保护指针的装载/读取（cancel 可来自任意线程）。
            std::mutex sessionMtx;
            core::Session *pinnedSession = nullptr; // 非拥有；Session 生命期在本操作内
        };

        namespace {
            // 单语句操作的完整上下文：跨越 step1（借连接）与 step2（执行），
            // 经 shared_ptr 保持在 postAfter 重试间隔内存活。
            enum class RetryMode {
                ReadRetries, // query：attempts = max(1, retry.max_attempts)
                WriteRetries, // execute/生成键：retry.retry_writes ? max : 1
                Single // queryEach/executeBatch：不重试（副作用/流不可重放）
            };

            struct StatementPolicy {
                bool isWrite = false; // 成功后在 root 上 markWrite（缓存失效）
                RetryMode retry = RetryMode::Single;
                bool cacheable = false; // 仅 query 族（非流式）
                bool fallbackOnlyIfNoRows = false; // queryEach 的组回退条件
                bool allowWriteBuffer = false; // execute/executeBatch（组路径）
            };

            template<class R>
            struct StatementOp {
                std::shared_ptr<OpState> op;
                std::shared_ptr<core::DataSource> root; // 用户面对的数据源（组或叶子）
                std::vector<std::shared_ptr<core::DataSource> > targets; // 路由候选
                std::size_t targetIdx = 0;
                int attempt = 0; // 当前目标内已开始的尝试数
                std::chrono::milliseconds borrowTimeout{-1};

                std::string sql;
                common::Params params;
                std::string cacheKey; // 可缓存读：调用线程算好，成功后复用

                // 在已借到的会话上执行一次（不含借出/治理/重试）。
                std::function<void(core::Session &, R &)> attemptFn;
                // 写缓冲补发任务构造器（仅组写路径；补发只打主库，与同步一致）。
                std::function<std::function<common::Status()>(
                    const std::shared_ptr<core::DataSource> &primary)> bufferedMaker;
                std::function<void(R &&)> cb;

                StatementPolicy policy;
            };

            // 会话/事务操作上下文（§8.5：整段复用同步实现）。
            struct SessionOp {
                std::shared_ptr<OpState> op;
                std::shared_ptr<core::DataSource> root;
                bool transactional = true;
                common::TransactionOptions txOpts;
                core::SessionFn fn;
                std::function<void(OpResult &&)> cb;
                std::chrono::milliseconds borrowTimeout{-1};
            };
        }

        // ====================================================================
        // AsyncEngine：全部静态成员。friend 关系：
        //   - DataSource → 访问 preGate/beforeAttempt/afterAttempt/retryDelay/
        //     readTarget/writeTargets/markWrite/writeBuffer_/pool()/makeSession/
        //     cache 系列等私有原语（与同步路径同源）。
        //   - Handle → 构造 Handle（私有构造函数）。
        // ====================================================================
        class AsyncEngine {
        public:
            AsyncEngine() = delete;

            // ---- 结果投递（I1 + I2）----
            // R 经 shared_ptr 装箱穿越 std::function 的可拷贝约束，
            // 投递到完成调度器；回调只取一次（move）。

            template<class R>
            static void deliverResult(std::function<void(R &&)> cb, R result) {
                auto resultBox = std::make_shared<R>(std::move(result));
                auto cbBox = std::make_shared<std::function<void(R &&)> >(std::move(cb));
                postCompletion(completionExecutor(), [resultBox, cbBox] {
                    (*cbBox)(std::move(*resultBox));
                });
            }

            // 已完成（未注册）的哨兵 Handle：错误快速路径的返回值。
            static Handle doneHandle() {
                auto op = std::make_shared<OpState>();
                op->state.store(Handle::State::Done);
                op->finished.store(true);
                return Handle(std::move(op));
            }

            template<class R>
            static Handle failNow(std::function<void(R &&)> cb, common::Status st) {
                R r;
                r.status = std::move(st);
                deliverResult(std::move(cb), std::move(r));
                return doneHandle();
            }

            // ---- 完成路径：改写超时/取消、置 Done、注销、投递 ----

            template<class R>
            static void finishStatement(const std::shared_ptr<StatementOp<R> > &ctx, R result) {
                const auto &op = ctx->op;
                bool expected = false;
                if (!op->finished.compare_exchange_strong(expected, true)) return; // 已完成

                // 状态改写（统一判定，§8.3）：
                //  - timedOut      → QueryTimeout（retryable），cancel 未送达时附提示；
                //  - userCancelled → Cancelled（如实反映"用户已放弃"，
                //                    驱动不支持取消时语句可能已实际跑完）。
                common::Status &st = result.status;
                if (op->timedOut.load()) {
                    auto timeout = common::Status::error(
                        common::ErrorCode::QueryTimeout,
                        "statement exceeded async timeout");
                    timeout.retryable = true;
                    if (!op->cancelDelivered.load()) {
                        timeout.message +=
                                " (could not cancel; driver may not support it - "
                                "timeout is best-effort)";
                    }
                    st = std::move(timeout);
                } else if (op->userCancelled.load()) {
                    st = common::Status::error(
                        common::ErrorCode::Cancelled,
                        st.ok()
                            ? "operation cancelled (statement may have completed; "
                            "cancel is best-effort)"
                            : "operation cancelled: " + st.message);
                }

                {
                    std::lock_guard<std::mutex> lk(op->sessionMtx);
                    op->pinnedSession = nullptr;
                }
                op->state.store(Handle::State::Done);
                registryRemove();
                deliverResult(std::move(ctx->cb), std::move(result));
            }

            // ---- 语句级超时（§8.3，D7）----

            static void armTimeout(const std::shared_ptr<OpState> &op,
                                   const std::chrono::milliseconds timeout) {
                // 持有 op 强引用：任务未到期即被停机丢弃也只多活一个对象。
                workerExecutor()->postAfter([op] {
                    guarded([&] {
                        if (op->finished.load()) return; // 已完成，无事可做（常态）
                        op->timedOut.store(true);
                        // best-effort：钉住的会话转发 cancel（Session::cancel 保证不抛）。
                        core::Session *s = nullptr;
                        {
                            std::lock_guard<std::mutex> lk(op->sessionMtx);
                            s = op->pinnedSession;
                        }
                        if (s) op->cancelDelivered.store(s->cancel().ok());
                    });
                }, timeout);
            }

            // ---- 提交入口（调用线程；§8.2 步骤 1–6）----

            template<class R>
            static Handle submitStatement(
                const std::shared_ptr<core::DataSource> &root, const std::string &sql,
                const common::Params &params, const common::OperationType gateType,
                const StatementPolicy &policy,
                std::function<void(core::Session &, R &)> attemptFn,
                std::function<std::function<common::Status()>(
                    const std::shared_ptr<core::DataSource> &primary)> bufferedMaker,
                std::function<void(R &&)> cb, const Options &opts) {
                if (!cb) {
                    return failNow<R>([](R &&) {
                                      },
                                      common::Status::error(common::ErrorCode::ConfigError,
                                                            "null callback"));
                }
                if (!root) {
                    return failNow<R>(std::move(cb),
                                      common::Status::error(common::ErrorCode::ConfigError,
                                                            "datasource not found"));
                }
                const auto ex = workerExecutor();
                if (!ex) {
                    return failNow<R>(std::move(cb),
                                      common::Status::error(
                                          common::ErrorCode::ConfigError,
                                          "async not enabled: call DBMW::init first "
                                          "or async::setExecutor"));
                }
                if (gStopping.load()) {
                    return failNow<R>(std::move(cb),
                                      common::Status::error(common::ErrorCode::PoolClosed,
                                                            "dbmw is shutting down"));
                }

                // I3：治理闸门（审计+限流）只在调用线程执行一次。
                if (const auto g = root->preGate(sql, gateType); !g.ok()) {
                    return failNow<R>(std::move(cb), g);
                }
                // 熔断快速失败（只读探测，不消耗半开令牌；逐尝试闸门在 worker 上）。
                if (root->isCircuitOpen()) {
                    return failNow<R>(std::move(cb),
                                      common::Status::error(
                                          common::ErrorCode::CircuitOpen,
                                          "datasource '" + root->name() + "' circuit is open"));
                }

                // 路由（纯内存，无 IO）：
                std::vector<std::shared_ptr<core::DataSource> > targets;
                if (policy.isWrite) {
                    targets = root->writeTargets(); // 组：failover 候选（可空）；叶子：空表
                    if (targets.empty() && !root->primary_) targets.push_back(root);
                } else {
                    auto t = root->readTarget(); // 组：读路由；叶子：null
                    if (!t) t = root;
                    targets.push_back(t);
                    // 读回退：目标非主且允许回退时，主库作为第二候选（逐尝试语义
                    // 与同步 fallbackToPrimary 一致：目标重试耗尽后才转移）。
                    if (root->primary_ && root->fallbackToPrimary_ && t != root->primary_)
                        targets.push_back(root->primary_);
                }

                auto ctx = std::make_shared<StatementOp<R> >();
                ctx->op = std::make_shared<OpState>();
                ctx->root = root;
                ctx->targets = std::move(targets);
                ctx->borrowTimeout = opts.borrowTimeout;
                ctx->sql = sql;
                ctx->params = params;
                ctx->attemptFn = std::move(attemptFn);
                ctx->bufferedMaker = std::move(bufferedMaker);
                ctx->cb = std::move(cb);
                ctx->policy = policy;

                // 结果缓存：只做在叶子目标上，key 带叶子自己的名字（与同步一致）。
                if (policy.cacheable) {
                    if constexpr (std::is_same_v<R, QueryResult>) {
                        common::ResultSet cached;
                        std::string key;
                        if (ctx->targets.front()->cacheLookup(sql, params, cached, key)) {
                            QueryResult r;
                            r.status = common::Status::OK();
                            r.rows = std::move(cached);
                            deliverResult(std::move(ctx->cb), std::move(r));
                            return doneHandle(); // 缓存命中：零驱动调用
                        }
                        ctx->cacheKey = std::move(key);
                    }
                }

                registryAdd();
                const Handle handle(ctx->op);

                const auto timeout = opts.timeout > std::chrono::milliseconds(0)
                                         ? opts.timeout
                                         : std::chrono::milliseconds(
                                             gDefaultTimeoutMs.load(std::memory_order_relaxed));
                if (timeout > std::chrono::milliseconds(0)) armTimeout(ctx->op, timeout);

                if (!ex->tryPost([ctx] { step1Statement(ctx); })) {
                    // 有界队列满：显式背压（Overloaded，可重试）。
                    R r;
                    r.status = common::Status::error(
                        common::ErrorCode::Overloaded,
                        "async executor queue full (queue_size reached)");
                    r.status.retryable = true;
                    finishStatement(ctx, std::move(r)); // 含注销与 Done 置位
                }
                return handle;
            }

            // ---- step1：借连接（worker）----

            template<class R>
            static void step1Statement(const std::shared_ptr<StatementOp<R> > &ctx) {
                guarded([&] {
                    const auto &op = ctx->op;
                    Handle::State expected = Handle::State::Queued;
                    op->state.compare_exchange_strong(expected, Handle::State::Running);

                    if (op->finished.load()) return;
                    if (op->userCancelled.load()) {
                        R r;
                        r.status = common::Status::error(
                            common::ErrorCode::Cancelled,
                            "operation cancelled before start");
                        finishStatement(ctx, std::move(r));
                        return;
                    }

                    // 组的写候选全部耗尽（writeTargets 为空）：
                    // 与同步 dispatchWrite 的"组不可写"同义，交失败决策
                    // （可能入写缓冲）。attempt 置最大值跳过目标内重试。
                    if (ctx->targetIdx >= ctx->targets.size()) {
                        R r;
                        r.status = common::Status::error(
                            common::ErrorCode::CircuitOpen,
                            "group '" + ctx->root->name() + "': no writable primary available");
                        r.status.retryable = true;
                        ctx->attempt = std::numeric_limits<int>::max();
                        handleAttemptFailure(ctx, std::move(r));
                        return;
                    }

                    const auto target = ctx->targets[ctx->targetIdx];
                    ++ctx->attempt; // 本目标的第 attempt 次尝试开始

                    // 熔断闸门（逐尝试，与同步一致；含半开探测令牌）。
                    if (const auto gate = target->beforeAttempt(); !gate.ok()) {
                        // 同步语义：闸门失败直接返回，不重试——但组层会把它当
                        // CircuitOpen 转移到下一候选。attempt 置最大值跳过目标内重试。
                        R r;
                        r.status = gate;
                        ctx->attempt = std::numeric_limits<int>::max();
                        handleAttemptFailure(ctx, std::move(r));
                        return;
                    }

                    const auto pool = target->pool();
                    if (!pool) {
                        R r;
                        r.status = common::Status::error(
                            common::ErrorCode::PoolClosed,
                            "datasource '" + target->name() + "' has been shut down");
                        handleAttemptFailure(ctx, std::move(r));
                        return;
                    }

                    // 异步借出：池满时挂等待者队列，本 worker 立即释放（§7.1）。
                    pool->borrowAsync(ctx->borrowTimeout, makePoolIo(workerExecutor()),
                                      [ctx, target](std::unique_ptr<
                                                        core::ConnectionPool::Handle> h,
                                                    common::Status st) {
                                          step2Statement(ctx, target, std::move(h),
                                                         std::move(st));
                                      });
                });
            }

            // ---- step2：执行（worker，经池 io.deliver 回来）----

            template<class R>
            static void step2Statement(const std::shared_ptr<StatementOp<R> > &ctx,
                                       const std::shared_ptr<core::DataSource> &target,
                                       std::unique_ptr<core::ConnectionPool::Handle> h,
                                       common::Status borrowStatus) {
                guarded([&] {
                    const auto &op = ctx->op;
                    if (op->finished.load()) return; // 超时后池才交付：连接随 h 析构归还

                    if (!h) {
                        // 借出失败（池关闭/耗尽/建连失败）。同步路径同样计入熔断。
                        target->afterAttempt(borrowStatus);
                        handleAttemptFailure(ctx, [&] {
                            R r;
                            r.status = std::move(borrowStatus);
                            return r;
                        }());
                        return;
                    }

                    if (op->userCancelled.load()) {
                        // 借到了但用户已放弃：不执行，直接取消收尾（连接随会话归还）。
                        R r;
                        r.status = common::Status::error(common::ErrorCode::Cancelled,
                                                         "operation cancelled before execution");
                        finishStatement(ctx, std::move(r));
                        return;
                    }

                    // 审计已在调用线程过（I3），与同步单语句路径一致传默认上下文。
                    auto session = target->makeSession(std::move(h));
                    {
                        std::lock_guard<std::mutex> lk(op->sessionMtx);
                        op->pinnedSession = session.get(); // 取消路由
                    }

                    R r;
                    try {
                        ctx->attemptFn(*session, r);
                    } catch (const std::exception &e) {
                        r.status = common::Status::error(
                            common::ErrorCode::Unknown,
                            std::string("async attempt threw: ") + e.what());
                    } catch (...) {
                        r.status = common::Status::error(
                            common::ErrorCode::Unknown,
                            "async attempt threw an unknown exception");
                    }

                    {
                        std::lock_guard<std::mutex> lk(op->sessionMtx);
                        op->pinnedSession = nullptr;
                    }
                    target->afterAttempt(r.status); // 熔断计数（与同步同源）

                    if (r.status.ok()) {
                        // 成功后动作：组/叶子的 markWrite（缓存失效 + RAW 标记）
                        // 记在 root 上——同步组路径同样由 dispatchWrite 记在组上。
                        if (ctx->policy.isWrite) ctx->root->markWrite();
                        if (ctx->policy.cacheable) {
                            if constexpr (std::is_same_v<R, QueryResult>) {
                                if (!ctx->cacheKey.empty())
                                    target->cacheStore(ctx->cacheKey, r.rows);
                            }
                        }
                        finishStatement(ctx, std::move(r));
                        return;
                    }
                    handleAttemptFailure(ctx, std::move(r));
                });
            }

            // ---- 失败决策：目标内重试 / 转移下一候选 / 写缓冲 / 收尾 ----

            template<class R>
            static void handleAttemptFailure(const std::shared_ptr<StatementOp<R> > &ctx,
                                             R r) {
                const auto &op = ctx->op;
                if (op->finished.load()) return;
                const auto &st = r.status;

                // 用户已取消：不再重试/转移（避免取消后还放大流量）。
                if (op->userCancelled.load()) {
                    finishStatement(ctx, std::move(r));
                    return;
                }

                const auto target = ctx->targetIdx < ctx->targets.size()
                                        ? ctx->targets[ctx->targetIdx]
                                        : nullptr;

                // 1) 目标内重试（同步语义：仅 retryable 才重试）。
                if (st.retryable && target
                    && ctx->attempt < maxAttempts(ctx->policy, *target)) {
                    const auto delay = target->retryDelay(ctx->attempt);
                    scheduleNext(ctx, delay);
                    return;
                }

                // 2) 转移下一候选（读回退 / 写 failover）。条件与同步一致：
                //    retryable || connectionBroken || CircuitOpen；
                //    queryEach 的组回退仅在零行时发生（已交付过行绝不重放）。
                const bool transferable = ctx->policy.isWrite
                    ? core::DataSource::safeToFailoverWrite(st)
                    : (st.retryable || st.connectionBroken ||
                       st.code == common::ErrorCode::CircuitOpen);
                // rows 成员只在 EachResult 上（fallbackOnlyIfNoRows 仅 queryEach
                // 置位），编译期裁剪避免对其他 R 实例化失败。
                bool rowsOk = true;
                if (ctx->policy.fallbackOnlyIfNoRows) {
                    if constexpr (std::is_same_v<R, EachResult>) rowsOk = r.rows == 0;
                }
                if (transferable && rowsOk && ctx->targetIdx + 1 < ctx->targets.size()) {
                    ++ctx->targetIdx;
                    ctx->attempt = 0;
                    scheduleNext(ctx, std::chrono::milliseconds(0));
                    return;
                }

                // 3) 写缓冲（组路径、execute/executeBatch、候选全部耗尽）。
                //    要求生成键的写不入缓冲：补发时拿不到键（与同步一致，
                //    该族根本不构造 bufferedMaker）。
                if (transferable && ctx->policy.isWrite && ctx->policy.allowWriteBuffer &&
                    ctx->root->primary_
                    && ctx->root->writeBuffer_ && ctx->root->writeBuffer_->enabled()
                    && ctx->bufferedMaker) {
                    if (ctx->root->writeBuffer_->enqueue(ctx->bufferedMaker(
                        ctx->root->primary_))) {
                        auto accepted = common::Status::error(
                            common::ErrorCode::Buffered,
                            "group '" + ctx->root->name()
                            + "': write accepted into buffer, not yet committed");
                        accepted.retryable = false;
                        r.status = std::move(accepted);
                        finishStatement(ctx, std::move(r));
                        return;
                    }
                }
                finishStatement(ctx, std::move(r));
            }

            // 重试/转移经 postAfter 重投递（D6：worker 绝不睡眠等待）。
            template<class R>
            static void scheduleNext(const std::shared_ptr<StatementOp<R> > &ctx,
                                     const std::chrono::milliseconds delay) {
                const auto ex = workerExecutor();
                if (!ex || gStopping.load()) {
                    R r;
                    r.status = common::Status::error(common::ErrorCode::PoolClosed,
                                                     "dbmw is shutting down");
                    finishStatement(ctx, std::move(r));
                    return;
                }
                ex->postAfter([ctx] { step1Statement(ctx); }, delay);
            }

            static int maxAttempts(const StatementPolicy &policy,
                                   const core::DataSource &target) {
                switch (policy.retry) {
                    case RetryMode::ReadRetries:
                        return std::max(1, target.retry_.max_attempts);
                    case RetryMode::WriteRetries:
                        return target.retry_.retry_writes
                                   ? std::max(1, target.retry_.max_attempts)
                                   : 1;
                    case RetryMode::Single:
                        return 1;
                }
                return 1;
            }

            // ---- 会话/事务（§8.5：最薄的一层）----

            static Handle submitSessionOp(std::shared_ptr<core::DataSource> root,
                                          const bool transactional,
                                          const common::TransactionOptions &txOpts,
                                          const core::SessionFn &fn,
                                          std::function<void(OpResult &&)> cb,
                                          const Options &opts) {
                if (!cb) {
                    return failNow<OpResult>([](OpResult &&) {
                                             },
                                             common::Status::error(
                                                 common::ErrorCode::ConfigError,
                                                 "null callback"));
                }
                if (!root) {
                    return failNow<OpResult>(std::move(cb),
                                             common::Status::error(
                                                 common::ErrorCode::ConfigError,
                                                 "datasource not found"));
                }
                const auto ex = workerExecutor();
                if (!ex) {
                    return failNow<OpResult>(std::move(cb),
                                             common::Status::error(
                                                 common::ErrorCode::ConfigError,
                                                 "async not enabled: call DBMW::init first "
                                                 "or async::setExecutor"));
                }
                if (gStopping.load()) {
                    return failNow<OpResult>(std::move(cb),
                                             common::Status::error(
                                                 common::ErrorCode::PoolClosed,
                                                 "dbmw is shutting down"));
                }

                // I3：会话入口只限流不审计（语句要等回调跑起来才存在，
                // 审计下沉到 Session 逐条把关——与同步 gateSession 同源）。
                if (const auto g = root->gateSession(); !g.ok()) {
                    return failNow<OpResult>(std::move(cb), g);
                }

                auto ctx = std::make_shared<SessionOp>();
                ctx->op = std::make_shared<OpState>();
                ctx->root = std::move(root);
                ctx->transactional = transactional;
                ctx->txOpts = txOpts;
                ctx->fn = fn;
                ctx->cb = std::move(cb);
                ctx->borrowTimeout = opts.borrowTimeout;

                registryAdd();
                const Handle handle(ctx->op);
                if (!ex->tryPost([ctx] { runSessionOp(ctx); })) {
                    OpResult r;
                    r.status = common::Status::error(
                        common::ErrorCode::Overloaded,
                        "async executor queue full (queue_size reached)");
                    r.status.retryable = true;
                    ctx->op->finished.store(true);
                    ctx->op->state.store(Handle::State::Done);
                    registryRemove();
                    deliverResult(std::move(ctx->cb), std::move(r));
                }
                return handle;
            }

            static void runSessionOp(const std::shared_ptr<SessionOp> &ctx) {
                guarded([&] {
                    const auto &op = ctx->op;
                    Handle::State expected = Handle::State::Queued;
                    op->state.compare_exchange_strong(expected, Handle::State::Running);

                    if (op->finished.load()) return;
                    if (op->userCancelled.load()) {
                        op->finished.store(true);
                        op->state.store(Handle::State::Done);
                        registryRemove();
                        OpResult r;
                        r.status = common::Status::error(common::ErrorCode::Cancelled,
                                                         "operation cancelled before start");
                        deliverResult(std::move(ctx->cb), std::move(r));
                        return;
                    }

                    // 整段复用同步实现：闸门、begin、看门狗线程、cancel、回滚、
                    // commit、markWrite、read-after-write 全部原样（在 worker 上
                    // 阻塞即职责）。事务句柄的 cancel 只在启动前生效——运行中
                    // 不改写结果，避免"提交成功却报 Cancelled"诱发重复重放。
                    common::Status st;
                    try {
                        if (ctx->transactional) {
                            st = ctx->root->transactionInternal(
                                ctx->txOpts, ctx->fn, ctx->borrowTimeout,
                                ctx->root->readOnly_);
                        } else {
                            st = ctx->root->withSessionInternal(
                                ctx->fn, ctx->borrowTimeout, nullptr,
                                ctx->root->readOnly_);
                        }
                    } catch (const std::exception &e) {
                        st = common::Status::error(
                            common::ErrorCode::Unknown,
                            std::string("async session op threw: ") + e.what());
                    } catch (...) {
                        st = common::Status::error(
                            common::ErrorCode::Unknown,
                            "async session op threw an unknown exception");
                    }

                    op->finished.store(true);
                    op->state.store(Handle::State::Done);
                    registryRemove();
                    OpResult r;
                    r.status = std::move(st);
                    deliverResult(std::move(ctx->cb), std::move(r));
                });
            }

            // ---- 写缓冲补发的友元桥 ----
            //
            // friend 关系不传播进 lambda 闭包体：bufferedMaker 的 lambda 定义在
            // 门面自由函数里，直接调 primary->executeUngated(...) 会撞私有访问。
            // 经引擎静态成员（友元上下文）中转一层。
            static common::Status bufferedReplayExecute(
                const std::shared_ptr<core::DataSource> &primary,
                const std::string &sql, const common::Params &params) {
                std::int64_t ignored = 0;
                return primary->executeUngated(sql, params, ignored);
            }

            static common::Status bufferedReplayBatch(
                const std::shared_ptr<core::DataSource> &primary,
                const std::string &sql, const common::ParamBatch &batch) {
                common::BatchResult ignored;
                return primary->executeBatchUngated(sql, batch, ignored);
            }
        };

        // ---- 引擎内部接线（dbmw.cpp 调用；声明见 dbmw_async.h）----

        void initEngine(const config::AsyncConfig &cfg) {
            std::lock_guard<std::mutex> lk(gMtx);
            gStopping.store(false); // 支持 init → shutdown → 再 init
            gDefaultTimeoutMs.store(
                std::max(0, cfg.statement_timeout_ms), std::memory_order_relaxed);
            if (!cfg.enabled) return; // 未启用且未注入执行器：异步调用返回 ConfigError
            if (gExecutor) return; // 热加载不重建线程池（在途操作仍持旧执行器）
            gExecutor = makeThreadPoolExecutor(cfg.threads,
                                               static_cast<std::size_t>(cfg.queue_size));
            gCompletion = gExecutor; // 默认复用；setCompletionExecutor 可覆盖
        }

        void drainAndStop(const std::chrono::milliseconds grace) {
            gStopping.store(true); // 1) 新异步操作立即以 PoolClosed 拒绝

            // 2) 等在途操作归零（回调已投递出去，但投递本身还需执行器活着，
            //    所以先排空再停执行器）。
            {
                std::unique_lock<std::mutex> lk(gMtx);
                const auto deadline = std::chrono::steady_clock::now() + grace;
                while (gInFlight > 0) {
                    if (gDrainCv.wait_until(lk, deadline) == std::cv_status::timeout) break;
                }
            }

            std::shared_ptr<IExecutor> completion, main;
            {
                std::lock_guard<std::mutex> lk(gMtx);
                completion = gCompletion;
                main = gExecutor;
                gCompletion.reset();
                gExecutor.reset();
            }
            // 3) 完成调度器：已投递的回调排空（worker 会先跑完队列再退出）。
            if (completion) completion->shutdown(std::chrono::milliseconds(0));
            // 4) 主执行器：worker 排空任务（池排在执行器之后——在途操作归还
            //    连接前，池必须活着；池的关闭仍由 DatabaseManager::shutdown 做）。
            if (main && main != completion) main->shutdown(grace);
        }

        std::size_t inFlight() {
            std::lock_guard<std::mutex> lk(gMtx);
            return gInFlight;
        }
    } // namespace detail

    // ========================================================================
    // Handle 实现
    // ========================================================================

    Handle::State Handle::state() const {
        if (!s_) return State::Done; // 无效句柄视作已结束
        return s_->state.load(std::memory_order_acquire);
    }

    common::Status Handle::cancel() const {
        if (!s_) {
            return common::Status::error(common::ErrorCode::ConfigError,
                                         "invalid handle (default-constructed or moved-from)");
        }
        const auto st = s_->state.load(std::memory_order_acquire);
        if (st == State::Done) {
            return common::Status::error(common::ErrorCode::QueryError,
                                         "operation already finished");
        }
        s_->userCancelled.store(true, std::memory_order_release);
        if (st == State::Running) {
            core::Session *s = nullptr;
            {
                std::lock_guard<std::mutex> lk(s_->sessionMtx);
                s = s_->pinnedSession;
            }
            if (s) return s->cancel(); // 尽力转发（不抛；结果如实上报）
            return common::Status::OK(); // 尚未钉住会话：已标记，启动/执行前生效
        }
        return common::Status::OK(); // Queued：标记取消，启动时直接以 Cancelled 完成
    }

    // ========================================================================
    // 门面：回调式
    // ========================================================================

    Handle query(const std::string &sql, QueryCallback cb, const Options opts) {
        return query(std::string(), sql, std::move(cb), opts);
    }

    Handle query(const std::string &sql, const common::Params &params,
                 QueryCallback cb, const Options opts) {
        return query(std::string(), sql, params, std::move(cb), opts);
    }

    Handle query(const std::string &dataSource, const std::string &sql,
                 QueryCallback cb, const Options opts) {
        return query(dataSource, sql, common::Params{}, std::move(cb), opts);
    }

    Handle query(const std::string &dataSource, const std::string &sql,
                 const common::Params &params, QueryCallback cb, const Options opts) {
        detail::StatementPolicy policy;
        policy.isWrite = false;
        policy.retry = detail::RetryMode::ReadRetries;
        policy.cacheable = true;
        return detail::AsyncEngine::submitStatement<QueryResult>(
            resolve(dataSource), sql, params, common::OperationType::Query, policy,
            [sql, params](const core::Session &s, QueryResult &r) {
                // 空参数走无参重载：与同步门面一致，未实现原生绑定的
                // 驱动（allowsLiteralInterpolation=false）也能执行纯文本 SQL。
                if (params.empty()) r.status = s.query(sql, r.rows);
                else r.status = s.query(sql, params, r.rows);
            },
            {}, std::move(cb), opts);
    }

    Handle execute(const std::string &sql, ExecCallback cb, const Options opts) {
        return execute(std::string(), sql, std::move(cb), opts);
    }

    Handle execute(const std::string &sql, const common::Params &params,
                   ExecCallback cb, const Options opts) {
        return execute(std::string(), sql, params, std::move(cb), opts);
    }

    Handle execute(const std::string &dataSource, const std::string &sql,
                   ExecCallback cb, const Options opts) {
        return execute(dataSource, sql, common::Params{}, std::move(cb), opts);
    }

    Handle execute(const std::string &dataSource, const std::string &sql,
                   const common::Params &params, ExecCallback cb, const Options opts) {
        detail::StatementPolicy policy;
        policy.isWrite = true;
        policy.retry = detail::RetryMode::WriteRetries;
        policy.allowWriteBuffer = true;
        return detail::AsyncEngine::submitStatement<ExecResult>(
            resolve(dataSource), sql, params, common::OperationType::Execute, policy,
            [sql, params](const core::Session &s, ExecResult &r) {
                if (params.empty()) r.status = s.execute(sql, r.affected);
                else r.status = s.execute(sql, params, r.affected);
            },
            [sql, params](const std::shared_ptr<core::DataSource> &primary) {
                // 补发只打主库：写缓冲的语义就是"等主恢复后补上"（与同步一致）。
                return std::function<common::Status()>(
                    [primary, sql, params] {
                        return detail::AsyncEngine::bufferedReplayExecute(primary, sql, params);
                    });
            },
            std::move(cb), opts);
    }

    Handle execute(const std::string &sql, const common::Params &params,
                   ExecKeysCallback cb, const Options opts) {
        return execute(std::string(), sql, params, std::move(cb), opts);
    }

    Handle execute(const std::string &dataSource, const std::string &sql,
                   const common::Params &params, ExecKeysCallback cb, const Options opts) {
        detail::StatementPolicy policy;
        policy.isWrite = true;
        policy.retry = detail::RetryMode::WriteRetries;
        // 不入写缓冲：补发是在后台"事后重放"，那一刻拿不到生成键。
        policy.allowWriteBuffer = false;
        return detail::AsyncEngine::submitStatement<ExecKeysResult>(
            resolve(dataSource), sql, params, common::OperationType::Execute, policy,
            [sql, params](const core::Session &s, ExecKeysResult &r) {
                if (params.empty()) r.status = s.execute(sql, r.affected, r.keys);
                else r.status = s.execute(sql, params, r.affected, r.keys);
            },
            {}, std::move(cb), opts);
    }

    Handle queryEach(const std::string &sql, const common::Params &params,
                     const common::RowCallback &rowCb, EachCallback done,
                     const Options opts) {
        return queryEach(std::string(), sql, params, rowCb, std::move(done), opts);
    }

    Handle queryEach(const std::string &dataSource, const std::string &sql,
                     const common::Params &params, const common::RowCallback &rowCb,
                     EachCallback done, const Options opts) {
        detail::StatementPolicy policy;
        policy.isWrite = false;
        policy.retry = detail::RetryMode::Single; // 已交付过行绝不自动重放
        policy.fallbackOnlyIfNoRows = true; // 组回退仅在零行时
        return detail::AsyncEngine::submitStatement<EachResult>(
            resolve(dataSource), sql, params, common::OperationType::Stream, policy,
            [sql, params, rowCb](const core::Session &s, EachResult &r) {
                r.status = s.queryEach(sql, params, rowCb, r.rows);
            },
            {}, std::move(done), opts);
    }

    Handle executeBatch(const std::string &sql, const common::ParamBatch &batch,
                        BatchCallback cb, const Options opts) {
        return executeBatch(std::string(), sql, batch, std::move(cb), opts);
    }

    Handle executeBatch(const std::string &dataSource, const std::string &sql,
                        const common::ParamBatch &batch, BatchCallback cb,
                        const Options opts) {
        detail::StatementPolicy policy;
        policy.isWrite = true;
        policy.retry = detail::RetryMode::Single; // 批量写默认不重试
        policy.allowWriteBuffer = true;
        return detail::AsyncEngine::submitStatement<BatchResult>(
            resolve(dataSource), sql, {}, common::OperationType::Batch, policy,
            [sql, batch](const core::Session &s, BatchResult &r) {
                r.status = s.executeBatch(sql, batch, r.batch);
            },
            [sql, batch](const std::shared_ptr<core::DataSource> &primary) {
                return std::function<common::Status()>(
                    [primary, sql, batch] {
                        return detail::AsyncEngine::bufferedReplayBatch(
                            primary, sql, batch);
                    });
            },
            std::move(cb), opts);
    }

    Handle transaction(const core::SessionFn &fn, OpCallback cb, const Options opts) {
        return detail::AsyncEngine::submitSessionOp(
            resolve(std::string()), true, common::TransactionOptions{}, fn,
            std::move(cb), opts);
    }

    Handle transaction(const std::string &dataSource, const core::SessionFn &fn,
                       OpCallback cb, const Options opts) {
        return detail::AsyncEngine::submitSessionOp(
            resolve(dataSource), true, common::TransactionOptions{}, fn,
            std::move(cb), opts);
    }

    Handle transaction(const common::TransactionOptions &txOpts, const core::SessionFn &fn,
                       OpCallback cb, const Options opts) {
        return detail::AsyncEngine::submitSessionOp(
            resolve(std::string()), true, txOpts, fn, std::move(cb), opts);
    }

    Handle transaction(const std::string &dataSource, const common::TransactionOptions &txOpts,
                       const core::SessionFn &fn, OpCallback cb, const Options opts) {
        return detail::AsyncEngine::submitSessionOp(
            resolve(dataSource), true, txOpts, fn, std::move(cb), opts);
    }

    Handle withSession(const core::SessionFn &fn, OpCallback cb, const Options opts) {
        return detail::AsyncEngine::submitSessionOp(
            resolve(std::string()), false, common::TransactionOptions{}, fn,
            std::move(cb), opts);
    }

    Handle withSession(const std::string &dataSource, const core::SessionFn &fn,
                       OpCallback cb, const Options opts) {
        return detail::AsyncEngine::submitSessionOp(
            resolve(dataSource), false, common::TransactionOptions{}, fn,
            std::move(cb), opts);
    }

    // ========================================================================
    // 门面：future 式（便利形态；无 Handle、无取消）
    // ========================================================================

    namespace {
        template<class R>
        std::future<R> makeFuturePair(std::shared_ptr<std::promise<R> > &promiseOut) {
            auto promise = std::make_shared<std::promise<R> >();
            auto future = promise->get_future();
            promiseOut = promise;
            return future;
        }
    } // namespace

    std::future<QueryResult> query(const std::string &sql) {
        std::shared_ptr<std::promise<QueryResult> > p;
        auto f = makeFuturePair(p);
        query(sql, QueryCallback([p](QueryResult &&r) { p->set_value(std::move(r)); }),
              {});
        return f;
    }

    std::future<QueryResult> query(const std::string &sql, const common::Params &params) {
        return query(std::string(), sql, params);
    }

    std::future<QueryResult> query(const std::string &dataSource, const std::string &sql,
                                   const common::Params &params) {
        std::shared_ptr<std::promise<QueryResult> > p;
        auto f = makeFuturePair(p);
        query(dataSource, sql, params,
              QueryCallback([p](QueryResult &&r) { p->set_value(std::move(r)); }), {});
        return f;
    }

    std::future<ExecResult> execute(const std::string &sql) {
        return execute(sql, common::Params{});
    }

    std::future<ExecResult> execute(const std::string &sql, const common::Params &params) {
        return execute(std::string(), sql, params);
    }

    std::future<ExecResult> execute(const std::string &dataSource, const std::string &sql,
                                    const common::Params &params) {
        std::shared_ptr<std::promise<ExecResult> > p;
        auto f = makeFuturePair(p);
        execute(dataSource, sql, params,
                ExecCallback([p](ExecResult &&r) { p->set_value(std::move(r)); }), {});
        return f;
    }

    std::future<ExecKeysResult> executeKeys(const std::string &sql,
                                            const common::Params &params) {
        return executeKeys(std::string(), sql, params);
    }

    std::future<ExecKeysResult> executeKeys(const std::string &dataSource,
                                            const std::string &sql,
                                            const common::Params &params) {
        std::shared_ptr<std::promise<ExecKeysResult> > p;
        auto f = makeFuturePair(p);
        execute(dataSource, sql, params,
                ExecKeysCallback([p](ExecKeysResult &&r) { p->set_value(std::move(r)); }),
                {});
        return f;
    }

    std::future<EachResult> queryEach(const std::string &sql, const common::Params &params,
                                      const common::RowCallback &rowCb) {
        std::shared_ptr<std::promise<EachResult> > p;
        auto f = makeFuturePair(p);
        queryEach(std::string(), sql, params, rowCb,
                  EachCallback([p](EachResult &&r) { p->set_value(std::move(r)); }), {});
        return f;
    }

    std::future<BatchResult> executeBatch(const std::string &sql,
                                          const common::ParamBatch &batch) {
        std::shared_ptr<std::promise<BatchResult> > p;
        auto f = makeFuturePair(p);
        executeBatch(std::string(), sql, batch,
                     BatchCallback([p](BatchResult &&r) { p->set_value(std::move(r)); }),
                     {});
        return f;
    }

    std::future<OpResult> transaction(const core::SessionFn &fn) {
        std::shared_ptr<std::promise<OpResult> > p;
        auto f = makeFuturePair(p);
        transaction(std::string(), fn,
                    OpCallback([p](OpResult &&r) { p->set_value(std::move(r)); }), {});
        return f;
    }

    std::future<OpResult> transaction(const std::string &dataSource,
                                      const common::TransactionOptions &txOpts,
                                      const core::SessionFn &fn) {
        std::shared_ptr<std::promise<OpResult> > p;
        auto f = makeFuturePair(p);
        transaction(dataSource, txOpts, fn,
                    OpCallback([p](OpResult &&r) { p->set_value(std::move(r)); }), {});
        return f;
    }

    // ========================================================================
    // 执行器管理
    // ========================================================================

    void setExecutor(std::shared_ptr<IExecutor> ex) {
        std::lock_guard<std::mutex> lk(gMtx);
        gExecutor = std::move(ex);
        if (!gCompletion) gCompletion = gExecutor;
    }

    void setCompletionExecutor(std::shared_ptr<IExecutor> ex) {
        std::lock_guard<std::mutex> lk(gMtx);
        gCompletion = std::move(ex);
    }

    ExecutorStats stats() {
        const auto ex = workerExecutor();
        return ex ? ex->stats() : ExecutorStats{};
    }
} // namespace dbmw::async
