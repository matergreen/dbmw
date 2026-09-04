#include "dbmw/core/connection_pool.h"
#include "dbmw/common/logger.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>
#include <vector>


namespace dbmw::core {
    namespace {
        // 统一的"投递完成回调"包装：保证结果只经 io.deliver 走，
        // 绝不在池锁内、绝不在发起调用的栈上执行（异步不变量 I1）。
        //
        // 装箱说明：io.deliver 收 std::function（要求可拷贝），
        // 而 Handle 是 move-only，因此经 shared_ptr 装箱转移所有权——
        // 一次堆分配换"投递通道保持简单的 std::function 形态"。
        void deliverBorrowResult(const AsyncIo &io,
                                 std::function<void(std::unique_ptr<ConnectionPool::Handle>,
                                                    common::Status)> complete,
                                 std::unique_ptr<ConnectionPool::Handle> handle,
                                 common::Status status) {
            io.deliver([handle = std::make_shared<std::unique_ptr<ConnectionPool::Handle>>(
                               std::move(handle)),
                        complete = std::move(complete),
                        status = std::move(status)]() mutable {
                complete(std::move(*handle), std::move(status));
            });
        }

        std::string poolExhaustedMessage(const std::string &poolName, int maxConn,
                                         int total, int borrowed,
                                         std::chrono::milliseconds waited) {
            return "pool '" + poolName + "' exhausted: waited "
                + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                    waited).count()) + "ms (max=" + std::to_string(maxConn)
                + ", total=" + std::to_string(total)
                + ", borrowed=" + std::to_string(borrowed) + ")";
        }
    }

    void ConnectionPool::State::returnConn(
        std::unique_ptr<IDatabaseConnection> conn,
        const std::chrono::steady_clock::time_point createdAt,
        const std::chrono::steady_clock::time_point borrowedAt,
        const bool reusable) {
        if (!conn) return;
        const auto now = std::chrono::steady_clock::now();
        const bool leaked = leakDetectionThreshold > std::chrono::milliseconds(0) &&
                            now - borrowedAt >= leakDetectionThreshold;
        if (leaked) {
            DBMW_LOG_WARN("pool [" + poolName + "] possible connection leak: borrowed for "
                          + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - borrowedAt).count()) + "ms");
        }
        bool doClose = false;
        // 归还直接交接：池满时有异步等待者在等，连接不过 idle 队列直接转交。
        // 交接后的投递与过期等待者的清理都必须在锁外执行，
        // 但过期状态（含 total/borrowed 快照）必须在锁内组装，避免无锁读。
        std::optional<AsyncWaiter> handoff;
        std::unique_ptr<ConnectionPool::Handle> handoffHandle;
        std::vector<std::pair<AsyncWaiter, common::Status>> expiredWaiters;
        {
            std::lock_guard lk(mtx);
            if (leaked) ++leakWarnings;
            if (borrowed > 0) --borrowed;
            const bool expired = maxLifetime > std::chrono::milliseconds(0) &&
                                 now - createdAt >= maxLifetime;
            // 非池化模式不复用连接：归还即关闭，idle 队列始终为空。
            if (closed || expired || !reusable || !pooled) {
                doClose = true;
                ++connectionsClosed;
                if (!reusable) ++invalidatedConnections;
                else if (expired) ++lifetimeEvictions;
                if (total > 0) --total;
            } else {
                // 顺带清理已过期的等待者（deadline 到点）。
                for (auto it = asyncWaiters.begin(); it != asyncWaiters.end();) {
                    if (it->deadline <= now) {
                        if (metricsEnabled) ++borrowTimeouts;
                        const auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(
                            now - it->enqueuedAt);
                        auto waiter = std::move(*it);
                        it = asyncWaiters.erase(it);
                        expiredWaiters.emplace_back(
                            std::move(waiter),
                            common::Status::error(
                                common::ErrorCode::PoolExhausted,
                                poolExhaustedMessage(poolName, maxConnections,
                                                     total, borrowed, waited)));
                    } else {
                        ++it;
                    }
                }
                if (!asyncWaiters.empty()) {
                    // 直接交接（设计 §7.2）：这条连接刚被正常使用过，
                    // 比闲置连接更可信，跳过 idle 队列、跳过借出 ping。
                    // 公平性策略：异步等待者优先于同步 cv 等待者。
                    handoff = std::move(asyncWaiters.front());
                    asyncWaiters.pop_front();
                    ++borrowed; // 无缝转交：连接仍处于"借用中"，total 不变
                    if (metricsEnabled) {
                        ++borrowSuccesses;
                        maxBorrowed = std::max(maxBorrowed, borrowed);
                    }
                    handoffHandle = std::unique_ptr<ConnectionPool::Handle>(
                        new ConnectionPool::Handle(weak_from_this(), std::move(conn),
                                                   createdAt, now));
                } else {
                    idle.push(IdleConnection{std::move(conn), createdAt, now});
                }
            }
        }
        // —— 锁外：投递 ——
        if (handoff && handoffHandle) {
            deliverBorrowResult(handoff->io, std::move(handoff->complete),
                                std::move(handoffHandle), common::Status::OK());
            // 直接交接没有产生新的 idle、没有腾出名额，同步等待者无需唤醒。
            return;
        }
        for (auto &w: expiredWaiters) {
            deliverBorrowResult(w.first.io, std::move(w.first.complete), nullptr,
                                std::move(w.second));
        }
        if (doClose) conn->close(); // close 可能涉及网络，放在锁外
        cv.notify_one();
    }

    ConnectionPool::ConnectionPool(std::unique_ptr<driver::IDriver> driver,
                                   config::DataSourceConfig cfg,
                                   const int minConn, const int maxConn,
                                   std::chrono::milliseconds borrowTimeout,
                                   std::chrono::milliseconds idleTimeout,
                                   std::chrono::milliseconds maxLifetime,
                                   std::chrono::milliseconds leakDetectionThreshold,
                                   std::chrono::milliseconds validationInterval,
                                   const bool metricsEnabled,
                                   const bool pooled)
        : state_(std::make_shared<State>()),
          driver_(std::move(driver)), cfg_(std::move(cfg)),
          minConn_(std::max(minConn, 0)),
          maxConn_(std::max(maxConn, 1)),
          borrowTimeout_(borrowTimeout),
          idleTimeout_(std::max(idleTimeout, std::chrono::milliseconds(0))),
          maxLifetime_(std::max(maxLifetime, std::chrono::milliseconds(0))) {
        if (minConn_ > maxConn_) minConn_ = maxConn_;
        state_->poolName = cfg_.name;
        state_->metricsEnabled = metricsEnabled;
        state_->minConnections = minConn_;
        state_->maxConnections = maxConn_;
        state_->maxLifetime = maxLifetime_;
        state_->leakDetectionThreshold = std::max(leakDetectionThreshold, std::chrono::milliseconds(0));
        state_->validationInterval = std::max(validationInterval, std::chrono::milliseconds(0));
        state_->pooled = pooled;

        // 预热到 min 条连接（失败只记日志，不致命）。
        // 非池化模式没有"池"可预热，跳过——每次 borrow 现建即可。
        for (int i = 0; pooled && i < minConn_; ++i) {
            auto code = common::ErrorCode::Ok;
            std::string err;
            auto conn = createConnection(code, err);
            if (!conn) {
                {
                    std::lock_guard<std::mutex> lk(state_->mtx);
                    ++state_->connectionCreateFailures;
                }
                DBMW_LOG_WARN("pool [" + name() + "] warmup failed: " + err);
                break; // 首条都建不上，后续也没必要重试
            }
            bool overflow = false;
            {
                std::lock_guard<std::mutex> lk(state_->mtx);
                if (state_->total >= maxConn_) overflow = true;
                else {
                    const auto now = std::chrono::steady_clock::now();
                    state_->idle.push(State::IdleConnection{std::move(conn), now, now});
                    ++state_->total;
                    ++state_->connectionsCreated;
                }
            }
            if (overflow) {
                conn->close();
                break;
            }
        }
    }

    ConnectionPool::~ConnectionPool() {
        shutdown(std::chrono::milliseconds(0));
    }

    ConnectionPool::Handle::~Handle() {
        if (!conn_) return;
        if (const auto st = state_.lock()) {
            st->returnConn(std::move(conn_), createdAt_, borrowedAt_,
                           reusable_.load()); // 池还在：归还
        } else {
            conn_->close(); // 池已销毁：只能自行关闭，绝不触碰已释放内存
        }
    }

    std::unique_ptr<ConnectionPool::Handle> ConnectionPool::borrow(std::string &error) const
    {
        auto code = common::ErrorCode::Ok;
        return borrow(code, error);
    }

    std::unique_ptr<ConnectionPool::Handle> ConnectionPool::borrow(common::ErrorCode &code,
                                                                  std::string &error,
                                                                  std::chrono::milliseconds timeout) const
    {
        if (timeout < std::chrono::milliseconds(0)) timeout = borrowTimeout_;
        if (timeout < std::chrono::milliseconds(0)) timeout = std::chrono::milliseconds(0);

        code = common::ErrorCode::Ok;
        error.clear();

        const auto borrowStarted = std::chrono::steady_clock::now();
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock<std::mutex> lk(state_->mtx);
        const bool metrics = state_->metricsEnabled;
        if (metrics) ++state_->borrowRequests;
        // 等待耗时在所有退出路径都计入（含超时/失败），否则拥堵时指标反而"好看"（P1-3）。
        // pool_metrics 关闭时不累加任何仅用于上报的计数（P2-7）。
        const auto recordWait = [&] {
            if (!metrics) return;
            const auto waited = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - borrowStarted);
            state_->totalBorrowWait += waited;
            state_->maxBorrowWait = std::max(state_->maxBorrowWait, waited);
        };
        const auto recordSuccess = [&] {
            recordWait();
            if (!metrics) return;
            ++state_->borrowSuccesses;
            state_->maxBorrowed = std::max(state_->maxBorrowed, state_->borrowed);
        };

        // 非池化模式：不查空闲队列、不排队等待、不受 max 名额限制，
        // 直接新建一条连接；用完由 Handle 析构归还，returnConn 见 !pooled 即关闭。
        // 这样上层（DataSource / Session）完全不必区分两种模式。
        if (!state_->pooled) {
            if (state_->closed) {
                code = common::ErrorCode::PoolClosed;
                error = "datasource '" + name() + "' is closed";
                recordWait();
                return nullptr;
            }
            lk.unlock();
            auto conn = createConnection(code, error);
            lk.lock();
            if (!conn) {
                // createConnection 已填充 code/error（ConnectionFailed / DriverDisabled 等）
                ++state_->connectionCreateFailures;
                recordWait();
                return nullptr;
            }
            if (state_->closed) {
                conn->close();
                code = common::ErrorCode::PoolClosed;
                error = "datasource '" + name() + "' is closed";
                recordWait();
                return nullptr;
            }
            ++state_->total; // 让 stats() 的 total 反映当前在用的直连数
            ++state_->borrowed;
            ++state_->connectionsCreated;
            recordSuccess();
            const auto now = std::chrono::steady_clock::now();
            return std::unique_ptr<Handle>(new Handle(state_, std::move(conn), now, now));
        }

        for (;;) {
            if (state_->closed) {
                code = common::ErrorCode::PoolClosed;
                error = "pool '" + name() + "' is closed";
                recordWait();
                return nullptr;
            }

            // 1) 复用空闲连接：持锁取出 -> 锁外 ping 校验 -> 回锁判定
            if (!state_->idle.empty()) {
                auto item = std::move(state_->idle.front());
                state_->idle.pop();

                lk.unlock();
                const auto now = std::chrono::steady_clock::now();
                const bool expired = maxLifetime_ > std::chrono::milliseconds(0) &&
                                     now - item.createdAt >= maxLifetime_;
                // 节流：距上次校验不足 validationInterval 时跳过 ping，省一次 RTT。
                // 大多数连接刚被用过，毫秒级内失效概率极低；真正坏掉的连接由
                // 心跳线程按 heartbeat_interval_ms 独立体检兜底。
                const bool needsPing = state_->validationInterval <=
                                           std::chrono::milliseconds(0) ||
                                       (now - item.lastValidated) >=
                                           state_->validationInterval;
                const bool alive = !expired && item.conn &&
                                   (!needsPing || item.conn->ping().ok());
                if (alive) item.lastValidated = now; // 校验通过或免校验都刷新时间戳
                if (!alive && item.conn) item.conn->close();
                lk.lock();

                if (alive) {
                    if (state_->closed) {
                        code = common::ErrorCode::PoolClosed;
                        error = "pool '" + name() + "' is closed";
                        item.conn->close();
                        recordWait();
                        return nullptr;
                    }
                    ++state_->borrowed;
                    recordSuccess();
                    // 注意：Handle 构造函数是私有的，std::make_unique 无法访问，
                    // 这里必须直接 new。
                    return std::unique_ptr<Handle>(new Handle(
                        state_, std::move(item.conn), item.createdAt,
                        std::chrono::steady_clock::now()));
                }

                // 失效连接出局
                --state_->total;
                ++state_->connectionsClosed;
                if (expired) ++state_->lifetimeEvictions;
                else ++state_->validationFailures;
                if (state_->total < 0) state_->total = 0;
                // 腾出了一个名额，唤醒一个等待者，否则它要空等到自己的 deadline。
                state_->cv.notify_one();
                continue;
            }

            // 2) 无空闲且未达上限：先预定名额，再到锁外建立连接
            if (state_->total < maxConn_) {
                ++state_->total;

                lk.unlock();
                auto conn = createConnection(code, error);
                lk.lock();

                if (conn) {
                    if (state_->closed) {
                        code = common::ErrorCode::PoolClosed;
                        error = "pool '" + name() + "' is closed";
                        conn->close();
                        --state_->total;
                        recordWait();
                        return nullptr;
                    }
                    ++state_->borrowed;
                    ++state_->connectionsCreated;
                    recordSuccess();
                    const auto now = std::chrono::steady_clock::now();
                    return std::unique_ptr<Handle>(new Handle(
                        state_, std::move(conn), now, now));
                }

                // createConnection 已填充 code/error（ConnectionFailed / DriverDisabled 等）
                --state_->total;
                ++state_->connectionCreateFailures;
                if (state_->total < 0) state_->total = 0;
                // 预定名额已释放，唤醒一个等待者去接手，避免它干等到超时。
                state_->cv.notify_one();
                recordWait();
                return nullptr;
            }

            // 3) 池已满：等待归还，或超时报错（不再无限阻塞）
            if (timeout == std::chrono::milliseconds(0)) {
                if (metrics) ++state_->borrowTimeouts;
                code = common::ErrorCode::PoolExhausted;
                error = "pool '" + name() + "' exhausted: waited "
                        + std::to_string(timeout.count()) + "ms (max="
                        + std::to_string(maxConn_) + ", total="
                        + std::to_string(state_->total) + ", borrowed="
                        + std::to_string(state_->borrowed) + ")";
                recordWait();
                return nullptr;
            }
            ++state_->waiting;
            if (metrics) state_->maxWaiting = std::max(state_->maxWaiting, state_->waiting);
            const auto waitResult = state_->cv.wait_until(lk, deadline);
            --state_->waiting;
            if (waitResult == std::cv_status::timeout) {
                if (metrics) ++state_->borrowTimeouts;
                code = common::ErrorCode::PoolExhausted;
                error = "pool '" + name() + "' exhausted: waited "
                        + std::to_string(timeout.count()) + "ms (max="
                        + std::to_string(maxConn_) + ", total="
                        + std::to_string(state_->total) + ", borrowed="
                        + std::to_string(state_->borrowed) + ")";
                recordWait();
                return nullptr;
            }
        }
    }

    // 清理已过期的异步等待者：锁内弹出并组装错误（快照 total/borrowed），
    // 锁外投递 PoolExhausted。由健康检查（心跳线程）、borrowAsync 入队前顺带调用。
    void ConnectionPool::expireWaiters() const {
        std::vector<std::pair<State::AsyncWaiter, common::Status>> expired;
        {
            std::lock_guard<std::mutex> lk(state_->mtx);
            if (state_->asyncWaiters.empty()) return;
            const auto now = std::chrono::steady_clock::now();
            for (auto it = state_->asyncWaiters.begin();
                 it != state_->asyncWaiters.end();) {
                if (it->deadline <= now) {
                    if (state_->metricsEnabled) ++state_->borrowTimeouts;
                    const auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - it->enqueuedAt);
                    auto waiter = std::move(*it);
                    it = state_->asyncWaiters.erase(it);
                    expired.emplace_back(
                        std::move(waiter),
                        common::Status::error(
                            common::ErrorCode::PoolExhausted,
                            poolExhaustedMessage(name(), maxConn_, state_->total,
                                                 state_->borrowed, waited)));
                } else {
                    ++it;
                }
            }
        }
        for (auto &w: expired) {
            deliverBorrowResult(w.first.io, std::move(w.first.complete), nullptr,
                                std::move(w.second));
        }
    }

    void ConnectionPool::borrowAsync(const std::chrono::milliseconds timeout,
                                     const AsyncIo &io,
                                     std::function<void(std::unique_ptr<Handle>,
                                                        common::Status)> complete) const {
        if (!io.usable() || !complete) {
            // 非法调用：没有投递通道就没有安全的失败路径，静默忽略并记日志。
            DBMW_LOG_WARN("pool [" + name() + "] borrowAsync called with unusable AsyncIo");
            return;
        }
        auto effective = timeout;
        if (effective < std::chrono::milliseconds(0)) effective = borrowTimeout_;
        if (effective < std::chrono::milliseconds(0)) effective = std::chrono::milliseconds(0);

        // 入队前顺带清理已过期的等待者（搭调用便车，等不及心跳）。
        expireWaiters();

        const auto now = std::chrono::steady_clock::now();
        const auto deadline = now + effective;

        std::unique_lock<std::mutex> lk(state_->mtx);
        if (state_->metricsEnabled) ++state_->borrowRequests;

        if (state_->closed) {
            lk.unlock();
            deliverBorrowResult(io, std::move(complete), nullptr,
                                common::Status::error(common::ErrorCode::PoolClosed,
                                                      "pool '" + name() + "' is closed"));
            return;
        }

        // 非池化模式：无队列、无名额限制，直接投建连任务（连接工厂语义）。
        if (!state_->pooled) {
            lk.unlock();
            postCreateTask(io, std::move(complete), false);
            return;
        }

        // 1) idle 有连接：优先复用。
        if (!state_->idle.empty()) {
            auto item = std::move(state_->idle.front());
            state_->idle.pop();
            const bool expired = maxLifetime_ > std::chrono::milliseconds(0) &&
                                 now - item.createdAt >= maxLifetime_;
            const bool needsPing = state_->validationInterval <= std::chrono::milliseconds(0) ||
                                   (now - item.lastValidated) >= state_->validationInterval;
            if (expired) {
                // 寿命到期：出局并继续走建连/入队。
                --state_->total;
                ++state_->connectionsClosed;
                ++state_->lifetimeEvictions;
                if (state_->total < 0) state_->total = 0;
                lk.unlock();
                if (item.conn) item.conn->close(); // 网络操作，锁外
                borrowAsync(effective, io, std::move(complete)); // 递归：继续判定
                return;
            }
            if (!needsPing) {
                // 新鲜连接：免 ping 直接交付（与同步 borrow 的节流策略一致）。
                ++state_->borrowed;
                if (state_->metricsEnabled) {
                    ++state_->borrowSuccesses;
                    state_->maxBorrowed = std::max(state_->maxBorrowed, state_->borrowed);
                }
                lk.unlock();
                auto h = std::unique_ptr<Handle>(
                    new Handle(state_, std::move(item.conn), item.createdAt, now));
                deliverBorrowResult(io, std::move(complete), std::move(h),
                                    common::Status::OK());
                return;
            }
            // 待校验：投 ping 任务，校验在锁外执行。
            // conn 经 shared_ptr 装箱：io.post 收 std::function（要求可拷贝），
            // 而 unique_ptr 不可拷贝——装箱后在任务内再 move 出来构造 Handle。
            lk.unlock();
            auto connBox = std::make_shared<std::unique_ptr<IDatabaseConnection>>(
                std::move(item.conn));
            io.post([self = shared_from_this(), st = state_, connBox,
                     createdAt = item.createdAt,
                     io, complete = std::move(complete),
                     effective]() mutable {
                auto &conn = *connBox;
                const bool alive = static_cast<bool>(conn) && conn->ping().ok();
                const auto finishedAt = std::chrono::steady_clock::now();
                std::unique_ptr<Handle> handle;
                bool closed = false;
                {
                    std::lock_guard<std::mutex> lk2(st->mtx);
                    if (st->closed) {
                        closed = true;
                    } else if (alive) {
                        ++st->borrowed;
                        if (st->metricsEnabled) {
                            ++st->borrowSuccesses;
                            st->maxBorrowed = std::max(st->maxBorrowed, st->borrowed);
                        }
                        handle = std::unique_ptr<Handle>(
                            new Handle(st->weak_from_this(), std::move(conn),
                                       createdAt, finishedAt));
                    }
                }
                if (closed) {
                    if (conn) conn->close();
                    deliverBorrowResult(io, std::move(complete), nullptr,
                                        common::Status::error(
                                            common::ErrorCode::PoolClosed,
                                            "pool is closed"));
                    return;
                }
                if (handle) {
                    deliverBorrowResult(io, std::move(complete), std::move(handle),
                                        common::Status::OK());
                    return;
                }
                // ping 失败：连接出局，名额归还后递归继续（建连或入队）。
                {
                    std::lock_guard<std::mutex> lk2(st->mtx);
                    --st->total;
                    ++st->connectionsClosed;
                    ++st->validationFailures;
                    if (st->total < 0) st->total = 0;
                }
                if (conn) conn->close();
                self->borrowAsync(effective, io, std::move(complete));
            });
            return;
        }

        // 2) idle 空但未达上限：预定名额，投建连任务。
        if (state_->total < maxConn_) {
            ++state_->total;
            lk.unlock();
            postCreateTask(io, std::move(complete), true);
            return;
        }

        // 3) 池满：不等待 → 立即失败；否则挂入等待者队列（不占任何线程）。
        if (effective == std::chrono::milliseconds(0)) {
            if (state_->metricsEnabled) ++state_->borrowTimeouts;
            const auto msg = poolExhaustedMessage(name(), maxConn_,
                                                  state_->total, state_->borrowed,
                                                  std::chrono::milliseconds(0));
            lk.unlock();
            deliverBorrowResult(io, std::move(complete), nullptr,
                                common::Status::error(common::ErrorCode::PoolExhausted, msg));
            return;
        }
        state_->asyncWaiters.push_back(
            State::AsyncWaiter{now, deadline, std::move(complete), io});
    }

    // 建连任务：占位语义由 slotReserved 区分（池化模式调用方已 ++total）。
    // 成功交付 Handle；失败回滚名额并交付错误。全程锁外做网络 IO。
    void ConnectionPool::postCreateTask(
        const AsyncIo &io,
        std::function<void(std::unique_ptr<Handle>, common::Status)> complete,
        const bool slotReserved) const {
        io.post([self = shared_from_this(), st = state_, io,
                 complete = std::move(complete), slotReserved]() mutable {
            common::ErrorCode code = common::ErrorCode::Ok;
            std::string err;
            auto conn = self->createConnection(code, err);
            std::unique_ptr<Handle> handle;
            common::Status status;
            bool doClose = false;
            {
                std::lock_guard<std::mutex> lk(st->mtx);
                if (st->closed) {
                    if (conn) doClose = true;
                    status = common::Status::error(common::ErrorCode::PoolClosed,
                                                   "pool '" + st->poolName + "' is closed");
                    if (slotReserved && st->total > 0) --st->total;
                } else if (!conn) {
                    if (slotReserved) {
                        --st->total;
                        ++st->connectionCreateFailures;
                        if (st->total < 0) st->total = 0;
                    } else {
                        ++st->connectionCreateFailures;
                    }
                    status = common::Status::error(code, err);
                    if (code == common::ErrorCode::ConnectionFailed) {
                        status.retryable = true;
                        status.connectionBroken = true;
                    }
                } else {
                    if (!slotReserved) ++st->total; // 非池化：total 反映在用直连数
                    ++st->borrowed;
                    ++st->connectionsCreated;
                    if (st->metricsEnabled) {
                        ++st->borrowSuccesses;
                        st->maxBorrowed = std::max(st->maxBorrowed, st->borrowed);
                    }
                    const auto createdAt = std::chrono::steady_clock::now();
                    handle = std::unique_ptr<Handle>(
                        new Handle(st->weak_from_this(), std::move(conn),
                                   createdAt, createdAt));
                    status = common::Status::OK();
                }
            }
            if (doClose && conn) conn->close();
            deliverBorrowResult(io, std::move(complete), std::move(handle),
                                std::move(status));
        });
    }

    void ConnectionPool::healthCheck() const
    {
        // 非池化模式没有空闲连接可体检，也不维持 min 水位，心跳直接空转。
        if (!state_->pooled) return;

        // 顺带清理已过期的异步等待者：心跳线程按 interval 跑，
        // 正好是"池耗尽超时"所需的检查粒度，零新增线程。
        expireWaiters();

        // 阶段 1：逐条取出空闲连接在锁外探测。
        //        一次只取一条，保证心跳期间业务线程仍能借到其余连接；
        //        健康连接放回队尾，实现轮转，避免总探测同一批。
        const size_t snapshot = idleCount();
        for (size_t i = 0; i < snapshot; ++i) {
            State::IdleConnection item;
            bool retireForIdle = false;
            {
                std::lock_guard<std::mutex> lk(state_->mtx);
                if (state_->closed || state_->idle.empty()) break;
                item = std::move(state_->idle.front());
                state_->idle.pop();
                retireForIdle = idleTimeout_ > std::chrono::milliseconds(0) &&
                                std::chrono::steady_clock::now() - item.returnedAt >= idleTimeout_ &&
                                state_->total > minConn_;
            }

            const auto now = std::chrono::steady_clock::now();
            const bool expired = maxLifetime_ > std::chrono::milliseconds(0) &&
                                 now - item.createdAt >= maxLifetime_;
            const bool alive = !retireForIdle && !expired && item.conn && item.conn->ping().ok();

            {
                std::lock_guard<std::mutex> lk(state_->mtx);
                if (alive && !state_->closed) {
                    state_->idle.push(std::move(item));
                } else {
                    if (item.conn) item.conn->close();
                    --state_->total;
                    ++state_->connectionsClosed;
                    if (retireForIdle) ++state_->idleEvictions;
                    else if (expired) ++state_->lifetimeEvictions;
                    else ++state_->validationFailures;
                    if (state_->total < 0) state_->total = 0;
                }
            }
        }

        // 阶段 2：算出缺口，预定名额后到锁外建连（connect 是慢 IO，绝不在持锁时做）
        int need = 0;
        {
            std::lock_guard<std::mutex> lk(state_->mtx);
            if (state_->closed) return;
            const int want = minConn_ - state_->total;
            const int room = maxConn_ - state_->total;
            need = std::min(std::max(want, 0), std::max(room, 0));
            state_->total += need; // 预定名额，防止并发过度建连
        }

        std::vector<State::IdleConnection> fresh;
        fresh.reserve(static_cast<size_t>(need));
        for (int i = 0; i < need; ++i) {
            auto code = common::ErrorCode::Ok;
            std::string err;
            auto conn = createConnection(code, err);
            if (!conn) {
                {
                    std::lock_guard<std::mutex> lk(state_->mtx);
                    ++state_->connectionCreateFailures;
                }
                DBMW_LOG_WARN("pool [" + name() + "] heartbeat refill failed: " + err);
                break; // 建连失败通常不会立刻恢复，停止本轮补充
            }
            const auto now = std::chrono::steady_clock::now();
            fresh.push_back(State::IdleConnection{std::move(conn), now, now});
        }

        // 阶段 3：回写入池，并把未用掉的名额还回去
        {
            std::lock_guard<std::mutex> lk(state_->mtx);
            state_->total -= (need - static_cast<int>(fresh.size()));
            state_->connectionsCreated += fresh.size();
            if (state_->total < 0) state_->total = 0;
            for (auto &item: fresh) {
                if (state_->closed) item.conn->close();
                else state_->idle.push(std::move(item));
            }
            if (!fresh.empty()) state_->cv.notify_all();
        }
    }

    void ConnectionPool::shutdown(const std::chrono::milliseconds grace) const
    {
        std::vector<std::pair<State::AsyncWaiter, common::Status>> waiters;
        std::unique_lock<std::mutex> lk(state_->mtx);
        state_->closed = true;
        state_->cv.notify_all(); // 唤醒所有等待借出的线程，让它们快速失败
        // 异步等待者：整体出队，锁外逐个交付 PoolClosed。
        for (auto &w: state_->asyncWaiters) {
            waiters.emplace_back(
                std::move(w),
                common::Status::error(common::ErrorCode::PoolClosed,
                                      "pool '" + name() + "' is closed"));
        }
        state_->asyncWaiters.clear();

        while (!state_->idle.empty()) {
            auto item = std::move(state_->idle.front());
            state_->idle.pop();
            lk.unlock();
            item.conn->close();
            lk.lock();
            ++state_->connectionsClosed;
        }

        if (grace > std::chrono::milliseconds(0)) {
            const auto deadline = std::chrono::steady_clock::now() + grace;
            while (state_->borrowed > 0) {
                if (state_->cv.wait_until(lk, deadline) == std::cv_status::timeout) break;
            }
        }

        // 仍未归还的连接由各自的 Handle 析构时关闭（State::returnConn 见 closed 即关闭）
        state_->total = state_->borrowed;
        lk.unlock();

        // 锁外投递 PoolClosed（I1 不变量：回调绝不在池锁内执行）。
        for (auto &w: waiters) {
            deliverBorrowResult(w.first.io, std::move(w.first.complete), nullptr,
                                std::move(w.second));
        }
    }

    size_t ConnectionPool::idleCount() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        return state_->idle.size();
    }

    size_t ConnectionPool::totalCount() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        return static_cast<size_t>(state_->total);
    }

    size_t ConnectionPool::borrowedCount() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        return static_cast<size_t>(state_->borrowed);
    }

    bool ConnectionPool::closed() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        return state_->closed;
    }

    ConnectionPool::Stats ConnectionPool::stats() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        Stats out;
        out.minConnections = static_cast<size_t>(std::max(0, state_->minConnections));
        out.maxConnections = static_cast<size_t>(std::max(0, state_->maxConnections));
        out.idle = state_->idle.size();
        out.total = static_cast<size_t>(std::max(0, state_->total));
        out.borrowed = static_cast<size_t>(std::max(0, state_->borrowed));
        out.waiting = static_cast<size_t>(std::max(0, state_->waiting));
        // 水位仪表（与 idle/waiting 同类），不受 pool_metrics 开关影响：
        // 关闭 metrics 只屏蔽"仅用于上报"的累计计数，不屏蔽当前状态。
        out.asyncWaiting = state_->asyncWaiters.size();
        if (!state_->metricsEnabled) return out;
        out.connectionsCreated = state_->connectionsCreated;
        out.connectionsClosed = state_->connectionsClosed;
        out.borrowTimeouts = state_->borrowTimeouts;
        out.validationFailures = state_->validationFailures;
        out.leakWarnings = state_->leakWarnings;
        out.maxBorrowed = static_cast<size_t>(std::max(0, state_->maxBorrowed));
        out.maxWaiting = static_cast<size_t>(std::max(0, state_->maxWaiting));
        out.borrowRequests = state_->borrowRequests;
        out.borrowSuccesses = state_->borrowSuccesses;
        out.connectionCreateFailures = state_->connectionCreateFailures;
        out.invalidatedConnections = state_->invalidatedConnections;
        out.idleEvictions = state_->idleEvictions;
        out.lifetimeEvictions = state_->lifetimeEvictions;
        out.totalBorrowWait = state_->totalBorrowWait;
        out.maxBorrowWait = state_->maxBorrowWait;
        return out;
    }

    std::unique_ptr<IDatabaseConnection> ConnectionPool::createConnection(common::ErrorCode &code,
                                                                        std::string &error) const {
        code = common::ErrorCode::Ok;
        error.clear();

        auto conn = driver_->createConnection();
        if (!conn) {
            code = common::ErrorCode::ConnectionFailed;
            error = "driver '" + std::string(driver_->name()) + "' returned a null connection";
            return nullptr;
        }

        if (const auto st = conn->connect(cfg_); !st.ok()) {
            code = st.code; // 原样透出 ConnectionFailed / DriverDisabled / ConfigError
            error = st.message;
            return nullptr;
        }
        return conn;
    }
} // namespace dbmw::core
