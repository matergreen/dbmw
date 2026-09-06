#include "dbmw/core/database_manager.h"

#include <algorithm>

#include "dbmw/driver/driver_factory.h"
#include "dbmw/common/logger.h"
#include "dbmw/common/observer.h"
#include "dbmw/common/sql_analyze.h"
#include "dbmw/core/sql_auditor.h"
#include "dbmw/core/query_cache.h"
#include "dbmw/core/stats_reporter.h"
#include "dbmw/core/interceptor.h"            // M1 SPI：拦截器埋点

#include <chrono>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <type_traits>
#include <utility>
#include <thread>
#include <unordered_set>
#include <variant>


namespace dbmw::core {
    namespace {
        // M1 SPI 不变量 I9：拦截器埋点严格走"最外层入口"模型。
        //
        //   公开入口清单：
        //     · DataSource::query / execute / executeBatch / queryEach / openCursor
        //       （12+1 个 SQL 执行重载）埋点。
        //     · DataSource::transaction（4 个事务重载）**不**直接调拦截器，
        //       只 push ContextScope；事务回调内的 Session 子语句自己触发布点。
        //     · Session::query / execute / queryEach / executeBatch /
        //       executePrepared / openCursor / prepare 公开入口全部埋点——
        //       业务拿到 Session 实例，它就是该业务的最外层入口；事务回调里
        //       的 Session::query 同样要埋，traceId/spanId 经栈顶 SqlContext 透传。
        //     · Session::begin / commit / rollback 是事务控制 SQL，**不**埋——
        //       事务层模型由事务拦截器（M5+）独立负责，不放进 SQL 拦截器。
        //
        //   *Ungated 系列（queryUngated/executeUngated/.../openCursorUngated）
        //   全部不埋：它们是组→叶子转发 / 驱动调用的内部细节，埋了等于把一次
        //   业务调用发多次回调。
        //
        // 这是为什么下面的 4 个 transaction 入口只做 ctx push、不发拦截器回调：
        // 事务回调里的 Session 子语句会拿到栈顶 ctx，自己埋。视图层 (`detail::`)
        // 的递归防护 + 栈深度限制 (`kMaxDepth=64`) 把拦截器自身引发的回环
        // 自动拦在第二层之外，业务无感。

        // M1 SPI 埋点统一助手：
        //   在被调用的 SQL 路径上自动处理
        //   beforeExecution（拒绝即中断）→ guard 构造 → 执行 fn →
        //   收集 duration / status / result → afterExecution；guard 析构即
        //   onCompletion（恰好一次）。
        //   不构造、不包装、绝不重抛；拦截器异常已被 detail 层 try/catch 吞掉（I11）。
        //
        // M5：把幂等声明叠加到既有写重试配置上（见 docs/roadmap-design-v0.4.0.md §7）。
        //
        // 决策优先级（高→低）：
        //   1. NonIdempotent → 绝不重试写（即便 retry_writes=true，attempts 强制为 1）。
        //   2. Idempotent    → 允许重试写，即使 retry_writes=false 也按 max_attempts 重试。
        //   3. Unspecified   → 走既有逻辑（retry_writes 配置决定）。
        //
        // 声明只影响"是否重试写"，不改变读路径（读本身可重放）与事务内不重试
        // 这条不变量（I4）——事务内语句不进 executeUngated 的重试循环。
        // 上下文取自线程栈顶 ContextScope；栈空时为 default（Unspecified），
        // 与"不声明即保持现状"完全一致。
        int resolveWriteAttempts(const config::RetryConfig &retry) {
            const auto idem = common::ContextScope::current().idempotency;
            if (idem == common::Idempotency::NonIdempotent) return 1;
            if (idem == common::Idempotency::Idempotent) return std::max(1, retry.max_attempts);
            return retry.retry_writes ? std::max(1, retry.max_attempts) : 1;
        }

        // 调用方负责：构造 ctx / 调用 runOnRoute / 构造 view / 决定 result 与
        // affected 是否对外暴露（nullptr/0 = 不暴露）。
        template <typename Fn>
        common::Status runWithInterceptors(ExecutionView &view,
                                           common::ResultSet *result,
                                           std::int64_t *affected,
                                           Fn &&fn) {
            if (auto st = detail::runBeforeExecution(view); !st.ok()) {
                view.status = st;
                view.result = nullptr;
                return st;
            }
            auto guard = detail::makeInterceptorGuard(view);
            const auto t0 = std::chrono::steady_clock::now();
            // M6（§8.3 + §3.5）：把 onRoute 决策（含 shadow / targetDataSource）
            // 压入线程栈顶，让 readTarget / writeTargets / dispatchWrite / cacheLookup
            // 在路由期能读到。这是和 M5 同源的设计——M5 的 idempotency 由调用方
            // 显式 push ContextScope 透传；M6 的 shadow 由 onRoute 写入 routeCtx，
            // 由本层把 routeCtx 装上栈，使两条注入路径都通过 ContextScope::current()
            // 呈现给下游。RAII 还原栈帧，异常路径同样安全。
            const common::ContextScope scope(view.ctx);
            auto st = std::forward<Fn>(fn)();
            view.duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - t0);
            view.status = st;
            view.result = st.ok() ? result : nullptr;
            if (st.ok() && affected) view.affected = *affected;
            detail::runAfterExecution(view);
            return st;
        }

        // 退避抖动用的真随机数，返回 [0, range) 内的值。
        //
        // 抖动唯一的目的就是把并发重试打散。用 std::hash<std::thread::id>
        // 这类确定性输入会得到"每个线程一个恒定值"的假随机：同一批客户端会以
        // 完全相同的节奏同步重试，故障恢复瞬间仍然惊群。这里用 random_device
        // 播种的线程私有发生器，保证每次调用结果不同。
        std::int64_t randomJitter(const std::int64_t range) {
            if (range <= 0) return 0;
            static thread_local std::mt19937_64 engine = [] {
                std::uint64_t seed = std::random_device{}();
                seed ^= static_cast<std::uint64_t>(
                    std::chrono::steady_clock::now().time_since_epoch().count());
                seed ^= static_cast<std::uint64_t>(
                    std::hash<std::thread::id>{}(std::this_thread::get_id()));
                return std::mt19937_64(seed);
            }();
            return std::uniform_int_distribution<std::int64_t>(0, range - 1)(engine);
        }

        // 预编译语句缓存的进程级开关与每连接上限。
        //
        // 与 QueryCache 一样只能是全局配置：预编译句柄绑在**物理连接**上，
        // 而连接是池化的、会在不同调用方之间流转，策略没法挂在某次调用上。
        // 由 DatabaseManager::init 下发，热加载时随之更新。
        std::atomic<bool> gPreparedEnabled{true};
        std::atomic<int> gPreparedMaxPerConn{0};

        void configurePreparedCache(const config::PreparedCacheConfig &cfg) {
            gPreparedEnabled.store(cfg.enabled);
            gPreparedMaxPerConn.store(cfg.max_per_connection);
        }

        // 预编译路径是否可用：全局开关打开，且这条连接所属驱动支持服务端预备。
        bool preparedPathUsable(const IDatabaseConnection &conn) {
            return gPreparedEnabled.load(std::memory_order_relaxed) && conn.supportsPrepared();
        }

        template<typename Fn>
        common::Status observe(const std::string &dataSource,
                               const common::OperationType type,
                               std::uint64_t &rows, Fn &&fn) {
            const auto start = std::chrono::steady_clock::now();
            common::Status status = fn();
            common::OperationEvent event;
            event.dataSource = dataSource;
            event.type = type;
            event.duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start);
            event.status = status;
            // SQLSTATE/错误类别可用于指标标签；完整驱动消息可能包含 SQL，
            // 默认不送入观测回调，避免遥测系统成为敏感数据旁路。
            event.status.message.clear();
            event.rowCount = rows;
            common::Observability::emit(event);
            return status;
        }

        template<typename Fn>
        common::Status observeSql(const std::string &dataSource,
                                  const common::OperationType type,
                                  const std::string &sql,
                                  const common::Params &params,
                                  IDatabaseConnection *connection,
                                  std::uint64_t &rows, Fn &&fn) {
            const auto start = std::chrono::steady_clock::now();
            common::Status status = fn();
            common::OperationEvent event;
            event.dataSource = dataSource;
            event.type = type;
            event.duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start);
            event.status = status;
            event.status.message.clear();
            event.rowCount = rows;
            common::SqlRenderer renderer;
            if (connection) {
                renderer = [connection, &sql, &params](
                    const common::SqlRenderOptions &options, std::string &out) {
                    return connection->renderSqlForLogging(sql, params, options, out);
                };
            }
            common::Observability::emitSql(std::move(event), sql, renderer);
            return status;
        }

        // 执行用户回调并兜住异常：事务场景下异常必须转成失败，否则无法触发回滚。
        common::Status runGuarded(Session &s, const SessionFn &fn) {
            // M1 SPI（I9）：**不**主动 push 一层空 SqlContext。回调进入即继承
            // 调用方的栈顶 ctx——同步路径下是业务方在入口装的 traceId/tenantId，
            // 异步路径下被 async_engine.cpp 装回的 entryCtx 接管。Session 子语句
            // 的拦截器视图 `ContextScope::current()` 因此与调用方一致，跨路径
            // 形态统一（设计 §M1 I9 + §M2 R1）。
            // 旧版本曾 push 空 ctx，导致业务 traceId 在事务回调内被遮蔽——已纠正。
            try {
                return fn(s);
            } catch (const std::exception &e) {
                return common::Status::error(common::ErrorCode::TxError,
                                             std::string("exception in session: ") + e.what());
            } catch (...) {
                return common::Status::error(common::ErrorCode::TxError,
                                             "unknown exception in session");
            }
        }

        // timeout < 0 表示沿用连接池自身的默认超时。
        constexpr std::chrono::milliseconds kUsePoolDefault{-1};

        // 按配置造一个限流器；未启用或没设总量上限时返回 nullptr。
        //
        // 每个数据源/组各拿一个独立实例（各自的令牌桶），而不是全进程共用一个：
        // 一个把报表库刷爆的查询不该顺带把交易库的配额也吃掉。代价是"经组访问"
        // 与"直接按名访问同一个叶子"分别计数，两条入口的总量可能叠加超过单条
        // 配置值——限流限的是入口，这一点必须在文档里讲清楚。
        //
        // global_qps <= 0 时 RateLimiter 内部一律放行，那就干脆不创建：
        // 让 DataSource 的 rateLimiter_ 保持空指针，热路径上连一次虚调用都省掉。
        std::shared_ptr<RateLimiter> makeRateLimiter(const config::RateLimitConfig &cfg) {
            if (!cfg.enabled || cfg.global_qps <= 0) return nullptr;
            return std::make_shared<RateLimiter>(
                static_cast<double>(cfg.global_qps),
                static_cast<double>(cfg.per_fingerprint_qps),
                cfg.burst, cfg.fingerprint_mode);
        }

        // 查询缓存 key：原始 SQL + 带类型标记且长度前缀的参数序列。
        //
        // 两个反直觉但必须如此的决定：
        //
        // 1) 用原始 SQL，不用结构模板。模板会把字面量折成 '?'，于是
        //    "WHERE id=1" 和 "WHERE id=2" 得到同一个 key——不带绑定参数的
        //    内联 SQL 会互相读到对方的结果。模板适合做慢 SQL 聚合和审计指纹，
        //    绝不能当缓存 key。代价只是同一语义的不同写法各占一条，属于少命中。
        //
        // 2) 不用 valueToString 拼参数。它把整数 1 和字符串 "1" 都渲染成 "1"，
        //    Blob 只取前 16 字节，不同的参数会撞成同一个 key。缓存返回错数据
        //    比缓存不命中严重得多，所以这里逐类型加标签并对变长值加长度前缀。
        std::string cacheKey(const std::string &sql, const common::Params &params) {
            std::string key = sql;
            key.push_back('\x1e');
            key += std::to_string(params.size());
            for (const auto &param: params) {
                key.push_back('\x1f');
                std::visit([&key](const auto &value) {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, std::nullptr_t>) {
                        key.push_back('n');
                    } else if constexpr (std::is_same_v<T, bool>) {
                        key.push_back('b');
                        key.push_back(value ? '1' : '0');
                    } else if constexpr (std::is_same_v<T, std::int64_t>) {
                        key.push_back('i');
                        key += std::to_string(value);
                    } else if constexpr (std::is_same_v<T, std::uint64_t>) {
                        key.push_back('u');
                        key += std::to_string(value);
                    } else if constexpr (std::is_same_v<T, double>) {
                        // 按位序列化：十进制文本化会丢精度，
                        // 两个不相等的 double 可能打印出同一串字符。
                        std::uint64_t bits = 0;
                        std::memcpy(&bits, &value, sizeof(bits));
                        key.push_back('d');
                        key += std::to_string(bits);
                    } else if constexpr (std::is_same_v<T, common::Timestamp>) {
                        key.push_back('t');
                        key += std::to_string(value.time_since_epoch().count());
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        key.push_back('s');
                        key += std::to_string(value.size());
                        key.push_back(':');
                        key += value;
                    } else if constexpr (std::is_same_v<T, common::Decimal> ||
                                         std::is_same_v<T, common::Date> ||
                                         std::is_same_v<T, common::Time> ||
                                         std::is_same_v<T, common::Uuid> ||
                                         std::is_same_v<T, common::Json>) {
                        if constexpr (std::is_same_v<T, common::Decimal>) key.push_back('m');
                        else if constexpr (std::is_same_v<T, common::Date>) key.push_back('a');
                        else if constexpr (std::is_same_v<T, common::Time>) key.push_back('o');
                        else if constexpr (std::is_same_v<T, common::Uuid>) key.push_back('g');
                        else key.push_back('j');
                        key += std::to_string(value.value.size());
                        key.push_back(':');
                        key += value.value;
                    } else {
                        key.push_back('x');
                        key += std::to_string(value.size());
                        key.push_back(':');
                        key.append(reinterpret_cast<const char *>(value.data()), value.size());
                    }
                }, param);
            }
            return key;
        }
    }

    // -----------------------------------------------------------------------
    // Session
    // -----------------------------------------------------------------------
    Session::~Session() {
        cleanupOpenTransaction();
    }

    void Session::cleanupOpenTransaction() noexcept {
        if (!txOpen_ || !h_) return;
        // 析构函数绝不能抛异常，这里把驱动的任何异常都吞掉。
        try {
            if ((*h_)->rollback().ok()) {
                txOpen_ = false;
                return; // 已干净回滚，连接可以复用
            }
        } catch (...) {
            // 落到下面作废连接
        }
        txOpen_ = false;
        // 回滚没成功，这条连接的事务状态未知，不能再回到池里。
        h_->invalidate();
    }

    common::Status Session::auditStatement(const std::string &sql,
                                           const common::OperationType type) const {
        if (!audit_.enabled) return common::Status::OK();
        return SqlAuditor::check(sql, type, audit_.readOnly);
    }

    common::Status Session::runPreparedQuery(const std::string &sql,
                                             const common::Params &params,
                                             common::ResultSet &out) const {
        IDatabaseConnection *conn = h_->get();
        if (!preparedPathUsable(*conn)) return conn->query(sql, params, out);

        // 幂等性由驱动保证：prepare() 内部按 (SQL, 参数类型签名) 查本连接缓存，
        // 命中就直接返回既有句柄，所以这里每次调用只多一次哈希表查找。
        PreparedStatementHandle handle;
        // 预编译失败（含驱动返回 NotSupported）**不作**为业务失败：
        // 退回直接执行，避免"驱动声称支持、但这条语句预备不了"时调用方拿不到结果。
        if (const auto st = conn->prepare(sql, params, handle); !st.ok())
            return conn->query(sql, params, out);
        return conn->executePrepared(handle, params, out);
    }

    common::Status Session::runPreparedExec(const std::string &sql,
                                            const common::Params &params,
                                            std::int64_t &affected,
                                            common::GeneratedKeys *keys) const {
        IDatabaseConnection *conn = h_->get();
        // 要生成键时不走预编译：executePrepared 拿不到 RETURNING 出来的结果集，
        // 为了省一次 prepare 而让调用方静默拿不到主键，是本末倒置。
        if (keys || !preparedPathUsable(*conn)) {
            return keys ? conn->execute(sql, params, affected, *keys)
                        : conn->execute(sql, params, affected);
        }
        PreparedStatementHandle handle;
        if (const auto st = conn->prepare(sql, params, handle); !st.ok())
            return conn->execute(sql, params, affected);
        return conn->executePrepared(handle, params, affected);
    }

    common::Status Session::query(const std::string &sql, common::ResultSet &out) const
    {
        if (const auto a = auditStatement(sql, common::OperationType::Query); !a.ok()) return a;
        // M1 SPI 埋点：会话内 SQL 入口（I9）。栈顶 ctx 来自外层 ContextScope
        // （事务回调内：runGuarded 已 push；业务裸调 withSession：业务方自己 push）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Query, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Query,
                           /*params*/ nullptr, /*result*/ &out,
                           /*affected*/ 0, std::chrono::microseconds{0},
                           common::Status::OK(), /*cached*/ false,
                           /*depth*/ 0, ctx};
        return runWithInterceptors(view, &out, nullptr, [&] {
            std::uint64_t rows = 0;
            const common::Params params;
            const auto status = observeSql(dataSource_, common::OperationType::Query, sql, params,
                                           h_->get(), rows, [&] {
                const auto result = (*h_)->query(sql, out);
                rows = out.rowCount();
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::query(const std::string &sql, const common::Params &params,
                                  common::ResultSet &out) const
    {
        if (const auto a = auditStatement(sql, common::OperationType::Query); !a.ok()) return a;
        // M1 SPI 埋点：会话内 SQL 入口（I9）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Query, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Query,
                           &params, &out, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, &out, nullptr, [&] {
            std::uint64_t rows = 0;
            const auto status = observeSql(dataSource_, common::OperationType::Query, sql, params,
                                           h_->get(), rows, [&] {
                const auto result = runPreparedQuery(sql, params, out);
                rows = out.rowCount();
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::execute(const std::string &sql, std::int64_t &affected) const
    {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
        // M1 SPI 埋点：会话内 SQL 入口（I9）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Execute,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            std::uint64_t rows = 0;
            const common::Params params;
            const auto status = observeSql(dataSource_, common::OperationType::Execute, sql, params,
                                           h_->get(), rows, [&] {
                const auto result = (*h_)->execute(sql, affected);
                rows = affected > 0 ? static_cast<std::uint64_t>(affected) : 0;
                if (result.ok()) didWrite_ = true;
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::execute(const std::string &sql, const common::Params &params,
                                    std::int64_t &affected) const
    {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
        // M1 SPI 埋点：会话内 SQL 入口（I9）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Execute,
                           &params, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            std::uint64_t rows = 0;
            const auto status = observeSql(dataSource_, common::OperationType::Execute, sql, params,
                                           h_->get(), rows, [&] {
                const auto result = runPreparedExec(sql, params, affected, nullptr);
                rows = affected > 0 ? static_cast<std::uint64_t>(affected) : 0;
                if (result.ok()) didWrite_ = true;
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::queryEach(const std::string &sql, const common::Params &params,
                                      const common::RowCallback &callback,
                                      std::uint64_t &rows) const {
        if (const auto a = auditStatement(sql, common::OperationType::Stream); !a.ok()) return a;
        // M1 SPI 埋点：会话内流式 SQL 入口（I9）。流式无 ResultSet，view.result=nullptr。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Stream, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Stream,
                           &params, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, nullptr, [&] {
            std::uint64_t observedRows = 0;
            std::exception_ptr callbackError;
            const common::RowCallback guardedCallback = [&](const common::Row &row) {
                try {
                    return callback(row);
                } catch (...) {
                    callbackError = std::current_exception();
                    return false;
                }
            };
            const auto status = observeSql(dataSource_, common::OperationType::Stream, sql, params,
                                           h_->get(), observedRows, [&] {
                auto result = (*h_)->queryEach(sql, params, guardedCallback, rows);
                if (result.ok() && callbackError) {
                    try {
                        std::rethrow_exception(callbackError);
                    } catch (const std::exception &e) {
                        result = common::Status::error(
                            common::ErrorCode::QueryError,
                            std::string("stream callback threw: ") + e.what());
                    } catch (...) {
                        result = common::Status::error(
                            common::ErrorCode::QueryError,
                            "stream callback threw an unknown exception");
                    }
                }
                observedRows = rows;
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::executeBatch(const std::string &sql,
                                         const common::ParamBatch &batch,
                                         common::BatchResult &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Batch); !a.ok()) return a;
        // M1 SPI 埋点：会话内批量 SQL 入口（I9）。批次结果不进 view（与 DataSource 对称）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Batch, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Batch,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, nullptr, [&] {
            std::uint64_t rows = 0;
            // 批量操作可能含成千上万组参数，只记录模板，避免生成误导性的单组完整 SQL。
            const common::Params noParams;
            const auto status = observeSql(dataSource_, common::OperationType::Batch, sql, noParams,
                                           nullptr, rows, [&] {
                const auto result = (*h_)->executeBatch(sql, batch, out);
                rows = out.totalAffected() > 0
                    ? static_cast<std::uint64_t>(out.totalAffected()) : 0;
                if (result.ok()) didWrite_ = true;
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::execute(const std::string &sql, std::int64_t &affected,
                                    common::GeneratedKeys &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
        // M1 SPI 埋点：会话内 SQL 入口（I9）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Execute,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            std::uint64_t rows = 0;
            const common::Params params;
            const auto status = observeSql(dataSource_, common::OperationType::Execute, sql, params,
                                           h_->get(), rows, [&] {
                const auto result = (*h_)->execute(sql, affected, out);
                rows = affected > 0 ? static_cast<std::uint64_t>(affected) : 0;
                if (result.ok()) didWrite_ = true;
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::execute(const std::string &sql, const common::Params &params,
                                    std::int64_t &affected, common::GeneratedKeys &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
        // M1 SPI 埋点：会话内 SQL 入口（I9）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Execute,
                           &params, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            std::uint64_t rows = 0;
            const auto status = observeSql(dataSource_, common::OperationType::Execute, sql, params,
                                           h_->get(), rows, [&] {
                const auto result = runPreparedExec(sql, params, affected, &out);
                rows = affected > 0 ? static_cast<std::uint64_t>(affected) : 0;
                if (result.ok()) didWrite_ = true;
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::query(const std::string &sql, const common::StreamParams &params,
                                  common::ResultSet &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Query); !a.ok()) return a;
        // M1 SPI 埋点：会话内流式参数入口（I9）。流式参数不进 ExecutionView.params。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Query, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Query,
                           nullptr, &out, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, &out, nullptr, [&] {
            std::uint64_t rows = 0;
            // 流式参数不参与观测渲染：内容不是定值，且可能是几十 MB 的 BLOB。
            // 观测只关心模板与耗时，这里传空参数即可。
            const common::Params noParams;
            const auto status = observeSql(dataSource_, common::OperationType::Query, sql, noParams,
                                           h_->get(), rows, [&] {
                const auto result = (*h_)->query(sql, params, out);
                rows = out.rowCount();
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::execute(const std::string &sql, const common::StreamParams &params,
                                    std::int64_t &affected, common::GeneratedKeys &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
        // M1 SPI 埋点：会话内流式参数 SQL 入口（I9）。流式参数不进 ExecutionView.params。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Execute,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            std::uint64_t rows = 0;
            const common::Params noParams;
            const auto status = observeSql(dataSource_, common::OperationType::Execute, sql, noParams,
                                           h_->get(), rows, [&] {
                const auto result = (*h_)->execute(sql, params, affected, out);
                rows = affected > 0 ? static_cast<std::uint64_t>(affected) : 0;
                if (result.ok()) didWrite_ = true;
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::executeBatch(const std::string &sql,
                                         const common::StreamParamBatch &batch,
                                         common::BatchResult &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Batch); !a.ok()) return a;
        // M1 SPI 埋点：会话内批量 SQL 入口（I9）。批次结果不进 view（与 DataSource 对称）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Batch, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Batch,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, nullptr, [&] {
            std::uint64_t rows = 0;
            const common::Params noParams;
            const auto status = observeSql(dataSource_, common::OperationType::Batch, sql, noParams,
                                           nullptr, rows, [&] {
                const auto result = (*h_)->executeBatch(sql, batch, out);
                rows = out.totalAffected() > 0
                    ? static_cast<std::uint64_t>(out.totalAffected()) : 0;
                if (result.ok()) didWrite_ = true;
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::prepare(const std::string &sql, const common::Params &typesSample,
                                    PreparedStatementHandle &out) const {
        out = PreparedStatementHandle{};
        // 预备语句也要过审计：借"预备"绕开黑名单等于给拦截开了后门。
        // 分类完全由 SQL 文本决定，与这里传的 OperationType 无关（见 sql_auditor）。
        if (const auto a = auditStatement(sql, common::OperationType::Query); !a.ok()) return a;
        // M1 SPI 埋点：prepare 也是 SQL 入口（I9）。无 ResultSet / 无 affected。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Query, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Query,
                           &typesSample, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, nullptr, [&] {
            return (*h_)->prepare(sql, typesSample, out);
        });
    }

    common::Status Session::executePrepared(const PreparedStatementHandle &h,
                                            const common::Params &params,
                                            common::ResultSet &out) const {
        // M1 SPI 埋点：会话内预编译执行入口（I9）。
        // 句柄是不透明令牌，上层拿不到它对应的 SQL 文本，view.sql 用 "<prepared>" 占位——
        // 实际 SQL 文本在 prepare 那一步已经审/埋过，此处仅以占位标记触发回调。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, "<prepared>", common::OperationType::Query, ctx);
        ExecutionView view{dataSource_, "<prepared>", common::OperationType::Query,
                           &params, &out, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, &out, nullptr, [&] {
            std::uint64_t rows = 0;
            const auto status = observeSql(dataSource_, common::OperationType::Query, "<prepared>",
                                           params, h_->get(), rows, [&] {
                const auto result = (*h_)->executePrepared(h, params, out);
                rows = out.rowCount();
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    common::Status Session::executePrepared(const PreparedStatementHandle &h,
                                            const common::Params &params,
                                            std::int64_t &affected) const {
        // M1 SPI 埋点：会话内预编译执行入口（I9）。句柄不透明，view.sql = "<prepared>"。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, "<prepared>", common::OperationType::Execute, ctx);
        ExecutionView view{dataSource_, "<prepared>", common::OperationType::Execute,
                           &params, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            std::uint64_t rows = 0;
            const auto status = observeSql(dataSource_, common::OperationType::Execute, "<prepared>",
                                           params, h_->get(), rows, [&] {
                const auto result = (*h_)->executePrepared(h, params, affected);
                rows = affected > 0 ? static_cast<std::uint64_t>(affected) : 0;
                if (result.ok()) didWrite_ = true;
                return result;
            });
            if (status.connectionBroken) h_->invalidate();
            return status;
        });
    }

    // Cursor 析构：关游标（幂等），OwnsHandle 时 Handle 随 unique_ptr 析构归还连接。
    // noexcept：close 失败只记日志，绝不在析构中抛异常。定义放此处以复用已 include 的 logger.h。
    Cursor::~Cursor() noexcept {
        if (impl_) {
            try {
                impl_->close();
            } catch (...) {
                DBMW_LOG_WARN("cursor: close on destruction failed");
            }
            impl_.reset();
        }
        cursorLease_.reset();
    }

    common::Status Session::openCursor(const std::string &sql, const common::Params &params,
                                       const CursorOptions &opts,
                                       std::unique_ptr<Cursor> &out) const
    {
        // 会话内逐条审计（入口处还无 SQL）；限流已由 withSession/transaction 入口扣过，这里不重扣。
        // 传 Select（游标）而非 Query：让审计对其豁免 require_limit_select。
        if (const auto a = auditStatement(sql, common::OperationType::Select); !a.ok()) return a;
        // M1 SPI 埋点：会话内游标入口（I9）。游标无 ResultSet，view.result=nullptr。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(dataSource_, sql, common::OperationType::Select, ctx);
        ExecutionView view{dataSource_, sql, common::OperationType::Select,
                           &params, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, nullptr, [&] {
            std::unique_ptr<ICursor> impl;
            const auto status = (*h_)->openCursor(sql, params, opts, impl);
            if (!status.ok()) return status;
            if (!impl)
                return common::Status::error(common::ErrorCode::CursorError,
                                             "driver opened no cursor");
            // 借而不占：连接仍归本 Session，游标随会话其余语句共享同一条连接
            //（及若已开的事务快照）。BorrowedInSession 时 Cursor 的 handle_ 为空，
            // close() 只关服务端游标、不归还连接，连接随 Session 析构归还。
            out = std::make_unique<Cursor>(nullptr, std::move(impl), audit_,
                                           Cursor::Binding::BorrowedInSession);
            return common::Status::OK();
        });
    }

    common::Status Session::begin() {
        std::uint64_t rows = 0;
        const auto st = observe(dataSource_, common::OperationType::Begin, rows,
                                [&] { return (*h_)->begin(); });
        if (st.connectionBroken) h_->invalidate();
        if (st.ok()) txOpen_ = true;
        return st;
    }

    common::Status Session::begin(const common::TransactionOptions &options) {
        std::uint64_t rows = 0;
        const auto st = observe(dataSource_, common::OperationType::Begin, rows,
                                [&] { return (*h_)->begin(options); });
        if (st.connectionBroken) h_->invalidate();
        if (st.ok()) txOpen_ = true;
        return st;
    }

    common::Status Session::commit() {
        std::uint64_t rows = 0;
        const auto st = observe(dataSource_, common::OperationType::Commit, rows,
                                [&] { return (*h_)->commit(); });
        if (st.connectionBroken) h_->invalidate();
        txOpen_ = false; // 无论成功失败，事务都已结束
        return st;
    }

    common::Status Session::rollback() {
        std::uint64_t rows = 0;
        const auto st = observe(dataSource_, common::OperationType::Rollback, rows,
                                [&] { return (*h_)->rollback(); });
        if (st.connectionBroken) h_->invalidate();
        txOpen_ = false;
        return st;
    }

    common::Status Session::savepoint(const std::string &name) {
        std::uint64_t rows = 0;
        const auto status = observe(dataSource_, common::OperationType::Savepoint, rows,
                                    [&] { return (*h_)->savepoint(name); });
        if (status.connectionBroken) h_->invalidate();
        return status;
    }

    common::Status Session::releaseSavepoint(const std::string &name) {
        std::uint64_t rows = 0;
        const auto status = observe(dataSource_, common::OperationType::Savepoint, rows,
                                    [&] { return (*h_)->releaseSavepoint(name); });
        if (status.connectionBroken) h_->invalidate();
        return status;
    }

    common::Status Session::rollbackToSavepoint(const std::string &name) {
        std::uint64_t rows = 0;
        const auto status = observe(dataSource_, common::OperationType::Savepoint, rows,
                                    [&] { return (*h_)->rollbackToSavepoint(name); });
        if (status.connectionBroken) h_->invalidate();
        return status;
    }

    common::Status Session::cancel() const {
        std::uint64_t rows = 0;
        // 跨线程入口：事务超时看门狗会在另一条线程上调它。
        // 驱动抛出的任何异常都必须在这里收敛成 Status —— 异常一旦逃逸出线程函数
        // 就会 std::terminate 掉整个进程，那远比"取消失败"严重。
        const auto status = observe(dataSource_, common::OperationType::Cancel, rows, [&] {
            try {
                return (*h_)->cancel();
            } catch (const std::exception &e) {
                return common::Status::error(common::ErrorCode::Cancelled,
                                             std::string("driver cancel threw: ") + e.what());
            } catch (...) {
                return common::Status::error(common::ErrorCode::Cancelled,
                                             "driver cancel threw an unknown exception");
            }
        });
        if (status.connectionBroken) h_->invalidate();
        return status;
    }

    // -----------------------------------------------------------------------
    // DataSource
    // -----------------------------------------------------------------------
    std::shared_ptr<DataSource> DataSource::readTarget() const {
        if (!primary_) return nullptr;
        // M6 影子库（§8）：onRoute 置位 ctx.shadow = true 后，路由层把整组读
        // 流量切换到影子数据源。影子不存在直接短路返回 nullptr（叶子级会走到
        // 自身 primary_ 兜底？此处严格影子语义：影子没配就当影子不成立），
        // 由 queryUngated 按叶子回退到 null 走默认逻辑。
        if (shadow_ && common::ContextScope::current().shadow) return shadow_;
        if (readAfterWrite_ > std::chrono::milliseconds(0)) {
            const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count();
            const auto last = lastWriteNs_.load();
            if (last > 0 && now - last < std::chrono::duration_cast<std::chrono::nanoseconds>(
                    readAfterWrite_).count())
                return primary_;
        }
        if (replicas_.empty()) return primary_;
        // 每个线程本地轮转，避免全局原子计数器在多核间的 false sharing 写竞争。
        // replicas_ 在 init 时定下后不再变动，读取无需加锁。
        thread_local std::uint64_t tlsRound = 0;
        const auto start = (tlsRound++) % replicas_.size();
        for (std::size_t offset = 0; offset < replicas_.size(); ++offset) {
            const auto &candidate = replicas_[(start + offset) % replicas_.size()];
            if (candidate && !candidate->isCircuitOpen()) return candidate;
        }
        // 所有副本都在熔断时直接走主库，不要按权重继续把请求
        // 送给已知故障节点。这是本地健康快照，复制延迟仍由外部拓扑管理。
        return primary_;
    }

    void DataSource::markWrite() const {
        if (primary_) {
            lastWriteNs_.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
            // 写后失效（粗粒度，靠 TTL 兜底）：本组 + 主 + 各副本 + 各转移候选。
            //
            // 逐 key 精确失效需要知道"这条写影响了哪些查询"，那等于要在中间件里
            // 实现一个查询改写器。粗粒度清除会牺牲命中率，但保证不会把旧数据
            // 当成新数据发出去——缓存的正确性优先于命中率。
            QueryCache::invalidate(name_);
            QueryCache::invalidate(primary_->name_);
            for (const auto &replica: replicas_) QueryCache::invalidate(replica->name_);
            for (const auto &candidate: failoverPrimaries_)
                if (candidate) QueryCache::invalidate(candidate->name_);
            return;
        }
        // 叶子节点：按名字直接拿单数据源写入时也必须失效自己的缓存，
        // 否则 execute() 之后紧接着的 query() 会在整个 TTL 内一直读到旧结果。
        QueryCache::invalidate(name_);
    }

    common::Status DataSource::preGate(const std::string &sql,
                                       const common::OperationType type) const {
        if (const auto s = SqlAuditor::check(sql, type, readOnly_); !s.ok()) return s;
        if (rateLimiter_) {
            // 只有真正启用了按指纹限流才去算指纹——structuralTemplate 要完整扫一遍
            // SQL，在只限总量的场景下这是纯浪费。
            const std::uint64_t fp = rateLimiter_->usesFingerprint()
                ? common::sql::fingerprintTemplate(sql) : 0;
            if (!rateLimiter_->acquire(fp)) {
                auto status = common::Status::error(common::ErrorCode::RateLimited,
                                                    "datasource '" + name_ + "' rate limited");
                // 绝不能标成可重试：限流的目的是把流量压下去，
                // 让重试逻辑接着放大它，等于配了个反向的加压器。
                status.retryable = false;
                return status;
            }
        }
        return common::Status::OK();
    }

    common::Status DataSource::gateSession() const {
        if (!rateLimiter_) return common::Status::OK();
        if (!rateLimiter_->acquire(0)) {
            auto status = common::Status::error(common::ErrorCode::RateLimited,
                                                "datasource '" + name_ + "' rate limited");
            status.retryable = false;
            return status;
        }
        return common::Status::OK();
    }

    bool DataSource::isCircuitOpen() const {
        if (circuitBreaker_.failure_threshold <= 0) return false;
        return circuitOpenUntil_.load(std::memory_order_acquire) >
            std::chrono::steady_clock::now();
    }

    std::vector<std::shared_ptr<DataSource>> DataSource::writeTargets() const {
        std::vector<std::shared_ptr<DataSource>> targets;
        if (!primary_) return targets; // 叶子节点：调用方直接走自身
        // M6 影子库（§8 + I12）：影子写**绝不**走故障转移或写缓冲——影子库
        // 不可用就应该让压测停掉，而不是悄悄降级到主库污染生产数据。
        if (shadow_ && common::ContextScope::current().shadow) {
            targets.push_back(shadow_);
            return targets;
        }
        if (failoverPrimaries_.empty()) {
            // 未配置故障转移：保持原语义，写只打主库。
            targets.push_back(primary_);
            return targets;
        }
        // failoverPrimaries_ 已把主置顶；按序过滤出未熔断（且可选健康）的候选。
        targets.reserve(failoverPrimaries_.size());
        for (const auto &candidate: failoverPrimaries_) {
            if (!candidate) continue;
            if (candidate->isCircuitOpen()) continue;
            if (requireHealthy_ && candidate->pool_.expired()) continue;
            targets.push_back(candidate);
        }
        return targets;
    }

    bool DataSource::safeToFailoverWrite(const common::Status &status) {
        switch (status.code) {
            case common::ErrorCode::ConnectionFailed:
                // 借连接阶段的 ConnectionFailed 只有 code/message；语句执行中
                // 出现 SQLSTATE 08 则会携带 sqlState + connectionBroken，其提交结果不确定。
                return !status.connectionBroken && status.sqlState.empty();
            case common::ErrorCode::PoolExhausted:    // 未借到连接
            case common::ErrorCode::PoolClosed:       // 池已停止，未执行
            case common::ErrorCode::CircuitOpen:      // 熔断闸门在执行前拒绝
            case common::ErrorCode::DriverDisabled:   // 无可用驱动
                return true;
            default:
                return false;
        }
    }

    common::Status DataSource::dispatchWrite(
        const std::function<common::Status(const std::shared_ptr<DataSource> &)> &attempt,
        const std::function<common::Status()> &buffered) const {
        // M6 影子短路（§8.3 + I12）：writeTargets() 在影子模式下已经返回单元素
        // {shadow_}。这里在尝试之前直接拦一次，明确"影子写不入写缓冲"的语义——
        // 哪怕 shadow_ 失败，也只把错误回给调用方，不让压测数据补发回生产库。
        if (shadow_ && common::ContextScope::current().shadow) {
            const auto st = attempt(shadow_);
            // 影子写不走 markWrite：写缓冲都不会更新，更不该让读后写窗口切主。
            // 也不走 afterAttempt 触发的熔断计数——影子失败是影子库自己的事，
            // 不该让熔断去屏蔽生产路径。
            return st;
        }
        const auto targets = writeTargets();

        // 一个候选都没有时的默认结论：整组不可写。标成可重试，让上层的
        // 重试/熔断按"连接类故障"处理，而不是当成业务错误直接抛给调用方。
        auto status = common::Status::error(
            common::ErrorCode::CircuitOpen,
            "group '" + name_ + "': no writable primary available");
        status.retryable = true;

        for (const auto &target: targets) {
            status = attempt(target);
            if (status.ok()) {
                markWrite();
                return status;
            }
            // 只有"没能落到库上"的失败才值得换节点。
            // 唯一键冲突、语法错误这类业务失败换个节点结果一模一样，
            // 转移过去只是把同一个错误再犯一次，还凭空多了一次误写的风险。
            // 执行阶段的断线/超时存在“已提交但回包丢失”的歧义，
            // 盲目切换节点会双写。只对可证明未执行的错误做 failover。
            if (!safeToFailoverWrite(status))
                return status;
        }

        // 主与所有候选都不可用：能入写缓冲就先受理。
        if (buffered && writeBuffer_ && writeBuffer_->enabled() &&
            writeBuffer_->enqueue(buffered)) {
            // Buffered 是"已受理、未提交"，语义上既不是成功也不是可重试失败：
            // 调用方必须知道这条写还没落库（不能拿它当提交回执），
            // 同时也不该再重试（重试会造成重复写入）。
            auto accepted = common::Status::error(
                common::ErrorCode::Buffered,
                "group '" + name_ + "': write accepted into buffer, not yet committed");
            accepted.retryable = false;
            DBMW_LOG_WARN("group [" + name_ + "] no writable primary, write buffered");
            return accepted;
        }
        return status;
    }

    common::Status DataSource::beforeAttempt() const {
        if (circuitBreaker_.failure_threshold <= 0) return common::Status::OK();
        const auto now = std::chrono::steady_clock::now();
        const auto openUntil = circuitOpenUntil_.load(std::memory_order_acquire);
        if (openUntil > now) {
            return common::Status::error(common::ErrorCode::CircuitOpen,
                                         "datasource '" + name_ + "' circuit is open");
        }
        if (openUntil != std::chrono::steady_clock::time_point{}) {
            // 已到开放时间（或半开窗口）：用 CAS 只放一个线程进入探测，
            // 其余直接判熔断，避免一群请求同时去试同一个半开数据源。
            if (bool expected = false; !halfOpenInFlight_.compare_exchange_strong(expected, true,
                                                                                  std::memory_order_acq_rel)) {
                return common::Status::error(common::ErrorCode::CircuitOpen,
                                             "datasource '" + name_ + "' circuit is half-open");
            }
        }
        return common::Status::OK();
    }

    void DataSource::afterAttempt(const common::Status &status) const {
        if (circuitBreaker_.failure_threshold <= 0) return;
        if (status.ok()) {
            consecutiveFailures_.store(0, std::memory_order_release);
            halfOpenInFlight_.store(false, std::memory_order_release);
            circuitOpenUntil_.store(std::chrono::steady_clock::time_point{},
                                    std::memory_order_release);
            return;
        }
        halfOpenInFlight_.store(false, std::memory_order_release);
        if (!status.retryable && !status.connectionBroken) {
            // 请求已到达数据库，只是业务错误；说明数据源可达，关闭熔断。
            consecutiveFailures_.store(0, std::memory_order_release);
            circuitOpenUntil_.store(std::chrono::steady_clock::time_point{},
                                    std::memory_order_release);
            return;
        }
        if (const int n = ++consecutiveFailures_; n >= circuitBreaker_.failure_threshold) {
            circuitOpenUntil_.store(std::chrono::steady_clock::now()
                                    + std::chrono::milliseconds(circuitBreaker_.open_interval_ms),
                                    std::memory_order_release);
        }
    }

    std::chrono::milliseconds DataSource::retryDelay(const int attempt) const {
        if (retry_.initial_backoff_ms <= 0) return std::chrono::milliseconds(0);
        std::int64_t delay = retry_.initial_backoff_ms;
        for (int i = 1; i < attempt && delay < retry_.max_backoff_ms; ++i)
            delay = std::min<std::int64_t>(delay * 2, retry_.max_backoff_ms);
        const auto range = std::max<std::int64_t>(1, delay / 4 + 1);
        const auto jitter = randomJitter(range);
        return std::chrono::milliseconds(std::min<std::int64_t>(
            retry_.max_backoff_ms, delay + jitter));
    }

    common::Status DataSource::borrowSession(std::unique_ptr<ConnectionPool::Handle> &out,
                                             std::chrono::milliseconds timeout) const
    {
        const auto pool = pool_.lock();
        if (!pool) {
            return common::Status::error(common::ErrorCode::PoolClosed,
                                         "datasource '" + name_ + "' has been shut down");
        }
        common::ErrorCode code = common::ErrorCode::Ok;
        std::string err;
        auto h = pool->borrow(code, err, timeout);
        if (!h) {
            auto status = common::Status::error(code, err);
            if (code == common::ErrorCode::ConnectionFailed) {
                status.retryable = true;
                status.connectionBroken = true;
            }
            return status;
        }
        // 每次借出都刷一遍预编译句柄上限。
        //
        // 不能只在连接创建时下发一次：上限是全局配置，热加载之后已经存在的连接
        // 也得跟着变，否则"改了配置不生效"会一直持续到连接自然淘汰。
        // 也不能指望驱动自己去读全局配置——驱动层不知道配置模块的存在。
        //
        // 代价是每次借出多一次虚调用，而借出本身已经包含加锁 + 有效性校验，
        // 这个开销可以忽略；换来的是配置即时生效与驱动无感知。
        (*h)->setPreparedCacheLimit(gPreparedMaxPerConn.load(std::memory_order_relaxed));
        out = std::move(h);
        return common::Status::OK();
    }

    // ===== v0.2.0 异步引擎接缝：结果缓存（叶子语义，与 queryUngated 的判定同源）=====

    bool DataSource::cacheEligible() const {
        return !primary_ && QueryCache::enabled() &&
            (!QueryCache::replicaOnly() || readReplica_);
    }

    bool DataSource::cacheLookup(const std::string &sql, const common::Params &params,
                                 common::ResultSet &out, std::string &key) const {
        if (!cacheEligible()) return false;
        // M6（§8.3）：影子读**绝不**进查询缓存——影子命中会污染真实租户的缓存，
        // 下一次非影子请求可能直接拿到影子库里的数据。最保守的拦截放在这里。
        if (common::ContextScope::current().shadow) return false;
        key = cacheKey(sql, params);
        return QueryCache::get(name_, key, out);
    }

    void DataSource::cacheStore(const std::string &key, const common::ResultSet &rows) const {
        // M6（§8.3）：与 cacheLookup 同源。
        if (common::ContextScope::current().shadow) return;
        // M7（§9.2 + I10）：脱敏结果绝不进缓存。transformed 是 SPI afterExecution
        // 改写 view.result 后置位的标记。缓存存原始数据，脱敏是角色/租户视图——
        // 把脱敏结果入库会让不同权限用户读到彼此的视图（跨用户泄漏）。
        if (rows.transformed) return;
        if (!primary_ && QueryCache::enabled()) QueryCache::put(name_, key, rows);
    }

    common::Status DataSource::query(const std::string &sql, common::ResultSet &out) const
    {
        if (const auto g = preGate(sql, common::OperationType::Query); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9），不在 queryUngated 中重复。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Query, ctx);
        ExecutionView view{name_, sql, common::OperationType::Query,
                           /*params*/ nullptr, /*result*/ &out,
                           /*affected*/ 0, std::chrono::microseconds{0},
                           common::Status::OK(), /*cached*/ false,
                           /*depth*/ 0, ctx};
        return runWithInterceptors(view, &out, nullptr, [&] {
            return queryUngated(sql, out);
        });
    }

    common::Status DataSource::queryUngated(const std::string &sql, common::ResultSet &out) const
    {
        if (primary_) {
            const auto target = readTarget();
            auto status = target->queryUngated(sql, out);
            if (target != primary_ && fallbackToPrimary_ &&
                (status.retryable || status.connectionBroken ||
                 status.code == common::ErrorCode::CircuitOpen)) {
                out.clear();
                status = primary_->queryUngated(sql, out);
            }
            return status;
        }
        // 结果缓存只做在叶子节点上，key 里带的是叶子自己的名字：
        // 同一条 SQL 打到主和打到副本是两条独立缓存项，写后失效才能按节点精确清除。
        // cache_on_replica_only 打开时只缓存副本读——读主库通常正是为了读到
        // 刚写进去的数据，给它加缓存等于把强一致读悄悄降级成最终一致。
        // M6（§8.3）：影子读不进查询缓存——会污染真实租户的缓存。
        const bool caching = QueryCache::enabled() &&
            (!QueryCache::replicaOnly() || readReplica_) &&
            !common::ContextScope::current().shadow;
        std::string key;
        if (caching) {
            key = cacheKey(sql, common::Params{});
            if (QueryCache::get(name_, key, out)) return common::Status::OK();
        }
        common::Status status;
        const int attempts = std::max(1, retry_.max_attempts);
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
            if (attempt > 1) out.clear();
            std::unique_ptr<ConnectionPool::Handle> h;
            status = borrowSession(h, kUsePoolDefault);
            if (status.ok()) {
                Session s(std::move(h), name_);
                status = s.query(sql, out);
            }
            afterAttempt(status);
            if (status.ok()) {
                // M7（§9.2 + I10）：与 DataSource::cacheStore 的守卫同源——
                // SPI afterExecution 改写后置位 transformed，硬拦截入缓存。
                // 性能上早于 QueryCache::put，避免一次 hash 计算 + 拷贝。
                if (caching && !out.transformed) QueryCache::put(name_, key, out);
                return status;
            }
            if (!status.retryable || attempt == attempts) return status;
            std::this_thread::sleep_for(retryDelay(attempt));
        }
        return status;
    }

    common::Status DataSource::query(const std::string &sql, const common::Params &params,
                                     common::ResultSet &out) const
    {
        if (const auto g = preGate(sql, common::OperationType::Query); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9），不在 queryUngated 中重复。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Query, ctx);
        ExecutionView view{name_, sql, common::OperationType::Query,
                           &params, &out, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, &out, nullptr, [&] {
            return queryUngated(sql, params, out);
        });
    }

    common::Status DataSource::queryUngated(const std::string &sql, const common::Params &params,
                                            common::ResultSet &out) const
    {
        if (primary_) {
            const auto target = readTarget();
            auto status = target->queryUngated(sql, params, out);
            if (target != primary_ && fallbackToPrimary_ &&
                (status.retryable || status.connectionBroken ||
                 status.code == common::ErrorCode::CircuitOpen)) {
                out.clear();
                status = primary_->queryUngated(sql, params, out);
            }
            return status;
        }
        const bool caching = QueryCache::enabled() &&
            (!QueryCache::replicaOnly() || readReplica_) &&
            !common::ContextScope::current().shadow;
        std::string key;
        if (caching) {
            key = cacheKey(sql, params);
            if (QueryCache::get(name_, key, out)) return common::Status::OK();
        }
        common::Status status;
        const int attempts = std::max(1, retry_.max_attempts);
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
            if (attempt > 1) out.clear();
            std::unique_ptr<ConnectionPool::Handle> h;
            status = borrowSession(h, kUsePoolDefault);
            if (status.ok()) {
                Session s(std::move(h), name_);
                status = s.query(sql, params, out);
            }
            afterAttempt(status);
            if (status.ok()) {
                // M7（§9.2 + I10）：与 DataSource::cacheStore 的守卫同源——
                // SPI afterExecution 改写后置位 transformed，硬拦截入缓存。
                // 性能上早于 QueryCache::put，避免一次 hash 计算 + 拷贝。
                if (caching && !out.transformed) QueryCache::put(name_, key, out);
                return status;
            }
            if (!status.retryable || attempt == attempts) return status;
            std::this_thread::sleep_for(retryDelay(attempt));
        }
        return status;
    }

    common::Status DataSource::execute(const std::string &sql, std::int64_t &affected) const
    {
        if (const auto g = preGate(sql, common::OperationType::Execute); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{name_, sql, common::OperationType::Execute,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            return executeUngated(sql, affected);
        });
    }

    common::Status DataSource::executeUngated(const std::string &sql,
                                              std::int64_t &affected) const
    {
        if (primary_) {
            std::function<common::Status()> buffered;
            if (writeBuffer_ && writeBuffer_->enabled()) {
                // 补发跑在后台线程上，调用方的 sql 那时早就析构了，必须拷一份；
                // 同时抓一个 primary_ 的强引用，否则组先销毁会让补发踩到悬垂对象。
                // 补发只打主库：写缓冲的语义就是"等主恢复后补上"，
                // 把积压的写散到候选上会让两边的写入顺序彻底对不上。
                buffered = [primary = primary_, bufferedSql = sql] {
                    std::int64_t ignored = 0;
                    return primary->executeUngated(bufferedSql, ignored);
                };
            }
            return dispatchWrite(
                [&sql, &affected](const std::shared_ptr<DataSource> &target) {
                    affected = 0;
                    return target->executeUngated(sql, affected);
                },
                buffered);
        }
        common::Status status;
        const int attempts = resolveWriteAttempts(retry_);
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
            affected = 0;
            std::unique_ptr<ConnectionPool::Handle> h;
            status = borrowSession(h, kUsePoolDefault);
            if (status.ok()) {
                Session s(std::move(h), name_);
                status = s.execute(sql, affected);
            }
            afterAttempt(status);
            if (status.ok()) {
                markWrite();
                return status;
            }
            if (!status.retryable || attempt == attempts) return status;
            std::this_thread::sleep_for(retryDelay(attempt));
        }
        return status;
    }

    common::Status DataSource::execute(const std::string &sql, const common::Params &params,
                                       std::int64_t &affected) const
    {
        if (const auto g = preGate(sql, common::OperationType::Execute); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{name_, sql, common::OperationType::Execute,
                           &params, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            return executeUngated(sql, params, affected);
        });
    }

    common::Status DataSource::executeUngated(const std::string &sql,
                                              const common::Params &params,
                                              std::int64_t &affected) const
    {
        if (primary_) {
            std::function<common::Status()> buffered;
            if (writeBuffer_ && writeBuffer_->enabled()) {
                buffered = [primary = primary_, bufferedSql = sql, bufferedParams = params] {
                    std::int64_t ignored = 0;
                    return primary->executeUngated(bufferedSql, bufferedParams, ignored);
                };
            }
            return dispatchWrite(
                [&sql, &params, &affected](const std::shared_ptr<DataSource> &target) {
                    affected = 0;
                    return target->executeUngated(sql, params, affected);
                },
                buffered);
        }
        common::Status status;
        const int attempts = resolveWriteAttempts(retry_);
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
            affected = 0;
            std::unique_ptr<ConnectionPool::Handle> h;
            status = borrowSession(h, kUsePoolDefault);
            if (status.ok()) {
                Session s(std::move(h), name_);
                status = s.execute(sql, params, affected);
            }
            afterAttempt(status);
            if (status.ok()) {
                markWrite();
                return status;
            }
            if (!status.retryable || attempt == attempts) return status;
            std::this_thread::sleep_for(retryDelay(attempt));
        }
        return status;
    }

    // -----------------------------------------------------------------------
    // 生成键（GeneratedKeys）
    //
    // 与写缓冲的关系：要求生成键的写**不入写缓冲**。
    //
    // 补发是在后台线程上"事后重放"的，那一刻拿不到、也不可能拿得到生成键。
    // 缓冲一条"目的就是取回主键"的 INSERT，等于递给调用方一张没有 ID 的受理回执：
    // 它既没法继续干活，又不能重试（Buffered 不可重试，重试会重复写入）。
    // 这里如实返回可重试的"组不可用"，让调用方把整段工作单元重来一遍。
    //
    // 与重试的关系：生成键走**常规重试**。它不改变 SQL、不消耗一次性资源，
    // 重试的只是"把同一条语句再发一次"，与既有 execute 完全一致。
    // -----------------------------------------------------------------------

    common::Status DataSource::execute(const std::string &sql, std::int64_t &affected,
                                       common::GeneratedKeys &out) const {
        if (const auto g = preGate(sql, common::OperationType::Execute); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{name_, sql, common::OperationType::Execute,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            return executeUngated(sql, affected, out);
        });
    }

    common::Status DataSource::executeUngated(const std::string &sql, std::int64_t &affected,
                                              common::GeneratedKeys &out) const {
        if (primary_) {
            return dispatchWrite(
                [&sql, &affected, &out](const std::shared_ptr<DataSource> &target) {
                    affected = 0;
                    out.clear();
                    return target->executeUngated(sql, affected, out);
                },
                {}); // 不入写缓冲：见本节开头
        }
        common::Status status;
        const int attempts = resolveWriteAttempts(retry_);
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
            affected = 0;
            out.clear();
            std::unique_ptr<ConnectionPool::Handle> h;
            status = borrowSession(h, kUsePoolDefault);
            if (status.ok()) {
                Session s(std::move(h), name_);
                status = s.execute(sql, affected, out);
            }
            afterAttempt(status);
            if (status.ok()) {
                markWrite();
                return status;
            }
            if (!status.retryable || attempt == attempts) return status;
            std::this_thread::sleep_for(retryDelay(attempt));
        }
        return status;
    }

    common::Status DataSource::execute(const std::string &sql, const common::Params &params,
                                       std::int64_t &affected,
                                       common::GeneratedKeys &out) const {
        if (const auto g = preGate(sql, common::OperationType::Execute); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{name_, sql, common::OperationType::Execute,
                           &params, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            return executeUngated(sql, params, affected, out);
        });
    }

    common::Status DataSource::executeUngated(const std::string &sql,
                                              const common::Params &params,
                                              std::int64_t &affected,
                                              common::GeneratedKeys &out) const {
        if (primary_) {
            return dispatchWrite(
                [&sql, &params, &affected, &out](const std::shared_ptr<DataSource> &target) {
                    affected = 0;
                    out.clear();
                    return target->executeUngated(sql, params, affected, out);
                },
                {}); // 不入写缓冲：见本节开头
        }
        common::Status status;
        const int attempts = resolveWriteAttempts(retry_);
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
            affected = 0;
            out.clear();
            std::unique_ptr<ConnectionPool::Handle> h;
            status = borrowSession(h, kUsePoolDefault);
            if (status.ok()) {
                Session s(std::move(h), name_);
                status = s.execute(sql, params, affected, out);
            }
            afterAttempt(status);
            if (status.ok()) {
                markWrite();
                return status;
            }
            if (!status.retryable || attempt == attempts) return status;
            std::this_thread::sleep_for(retryDelay(attempt));
        }
        return status;
    }

    // -----------------------------------------------------------------------
    // 大参数流式（StreamParams）
    //
    // 三条硬约束，都是"流是一次性的"这一个事实推出来的：
    //
    // 1) **不重试**。第一次尝试就把流读走了，重放只会拿到半截内容甚至空值。
    //    这种错误是静默的——写入截断的 BLOB 不会报错，事后再也查不回来。
    //    宁可直接报错，也不要给调用方一个"成功但数据不对"的结果。
    //    需要重试语义的调用方请自己把参数物化成 Value，改用 Params 重载。
    //
    // 2) **不入写缓冲**。补发发生在后台线程，那时 StreamSource 引用的
    //    istream/文件句柄早已随调用栈销毁，补发出去的内容无从谈起。
    //
    // 3) **不进结果缓存**。流式内容不是定值，参与不了 cacheKey
    //    （cacheKey 要求同参数必得同结果），与游标的处理一致。
    // -----------------------------------------------------------------------

    common::Status DataSource::query(const std::string &sql, const common::StreamParams &params,
                                     common::ResultSet &out) const {
        if (const auto g = preGate(sql, common::OperationType::Query); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。流式参数不进 ExecutionView.params。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Query, ctx);
        ExecutionView view{name_, sql, common::OperationType::Query,
                           nullptr, &out, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, &out, nullptr, [&] {
            return queryUngated(sql, params, out);
        });
    }

    common::Status DataSource::queryUngated(const std::string &sql,
                                            const common::StreamParams &params,
                                            common::ResultSet &out) const {
        if (primary_) {
            const auto target = readTarget();
            auto status = target->queryUngated(sql, params, out);
            if (target != primary_ && fallbackToPrimary_ &&
                (status.retryable || status.connectionBroken ||
                 status.code == common::ErrorCode::CircuitOpen)) {
                out.clear();
                status = primary_->queryUngated(sql, params, out);
            }
            return status;
        }
        if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
        std::unique_ptr<ConnectionPool::Handle> h;
        auto status = borrowSession(h, kUsePoolDefault);
        if (status.ok()) {
            Session s(std::move(h), name_);
            status = s.query(sql, params, out);
        }
        afterAttempt(status);
        return status; // 不重试：流不可重放，见本节开头
    }

    common::Status DataSource::execute(const std::string &sql, const common::StreamParams &params,
                                       std::int64_t &affected,
                                       common::GeneratedKeys &out) const {
        if (const auto g = preGate(sql, common::OperationType::Execute); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。流式参数不进 ExecutionView.params。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Execute, ctx);
        ExecutionView view{name_, sql, common::OperationType::Execute,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, &affected, [&] {
            return executeUngated(sql, params, affected, out);
        });
    }

    common::Status DataSource::executeUngated(const std::string &sql,
                                              const common::StreamParams &params,
                                              std::int64_t &affected,
                                              common::GeneratedKeys &out) const {
        if (primary_) {
            return dispatchWrite(
                [&sql, &params, &affected, &out](const std::shared_ptr<DataSource> &target) {
                    affected = 0;
                    out.clear();
                    return target->executeUngated(sql, params, affected, out);
                },
                {}); // 不入写缓冲：见本节开头
        }
        if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
        affected = 0;
        out.clear();
        std::unique_ptr<ConnectionPool::Handle> h;
        auto status = borrowSession(h, kUsePoolDefault);
        if (status.ok()) {
            Session s(std::move(h), name_);
            status = s.execute(sql, params, affected, out);
        }
        afterAttempt(status);
        if (status.ok()) markWrite();
        return status; // 不重试，同上
    }

    common::Status DataSource::executeBatch(const std::string &sql,
                                            const common::StreamParamBatch &batch,
                                            common::BatchResult &out) const {
        if (const auto g = preGate(sql, common::OperationType::Batch); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。批次结果（每行 affected）不进 view，
        // BatchResult 含 vector，写入 view.result 会让接口误把它当 ResultSet 处理。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Batch, ctx);
        ExecutionView view{name_, sql, common::OperationType::Batch,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, nullptr, [&] {
            return executeBatchUngated(sql, batch, out);
        });
    }

    common::Status DataSource::executeBatchUngated(const std::string &sql,
                                                   const common::StreamParamBatch &batch,
                                                   common::BatchResult &out) const {
        if (primary_) {
            return dispatchWrite(
                [&sql, &batch, &out](const std::shared_ptr<DataSource> &target) {
                    out.clear();
                    return target->executeBatchUngated(sql, batch, out);
                },
                {}); // 不入写缓冲：见本节开头
        }
        if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
        std::unique_ptr<ConnectionPool::Handle> h;
        auto status = borrowSession(h, kUsePoolDefault);
        if (status.ok()) {
            Session s(std::move(h), name_);
            status = s.executeBatch(sql, batch, out);
        }
        afterAttempt(status);
        if (status.ok()) markWrite();
        return status; // 批量写本就不重试；流式更不能重试
    }

    common::Status DataSource::queryEach(const std::string &sql,
                                         const common::Params &params,
                                         const common::RowCallback &callback,
                                         std::uint64_t &rows) const {
        if (const auto g = preGate(sql, common::OperationType::Stream); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。流式无 ResultSet，view.result=nullptr。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Stream, ctx);
        ExecutionView view{name_, sql, common::OperationType::Stream,
                           &params, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, nullptr, [&] {
            return queryEachUngated(sql, params, callback, rows);
        });
    }

    common::Status DataSource::queryEachUngated(const std::string &sql,
                                                const common::Params &params,
                                                const common::RowCallback &callback,
                                                std::uint64_t &rows) const {
        if (primary_) {
            const auto target = readTarget();
            auto status = target->queryEachUngated(sql, params, callback, rows);
            if (rows == 0 && target != primary_ && fallbackToPrimary_ &&
                (status.retryable || status.connectionBroken ||
                 status.code == common::ErrorCode::CircuitOpen))
                status = primary_->queryEachUngated(sql, params, callback, rows);
            return status;
        }
        // 流式读不进缓存：行是边读边交付给回调的，中间件手里从来没有完整结果集，
        // 要缓存就得先整体物化——那恰恰是 queryEach 存在的意义所要避免的。
        if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
        std::unique_ptr<ConnectionPool::Handle> h;
        auto status = borrowSession(h, kUsePoolDefault);
        if (status.ok()) {
            Session session(std::move(h), name_);
            status = session.queryEach(sql, params, callback, rows);
        }
        afterAttempt(status);
        // 已向调用方交付过行时绝不自动重放，避免重复副作用。
        return status;
    }

    common::Status DataSource::executeBatch(const std::string &sql,
                                            const common::ParamBatch &batch,
                                            common::BatchResult &out) const {
        if (const auto g = preGate(sql, common::OperationType::Batch); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。批次结果不进 view（见 StreamParamBatch 版注释）。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Batch, ctx);
        ExecutionView view{name_, sql, common::OperationType::Batch,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, nullptr, [&] {
            return executeBatchUngated(sql, batch, out);
        });
    }

    common::Status DataSource::openCursor(const std::string &sql, const common::Params &params,
                                          const CursorOptions &opts,
                                          std::unique_ptr<Cursor> &out) const
    {
        // 能力总开关：关闭后直接返回 NotSupported，避免把“功能未启用”伪装成运行时错误。
        if (!cursorEnabled_)
            return common::Status::error(common::ErrorCode::NotSupported,
                                         "cursors are disabled for this datasource");
        // 滚动游标仅当配置允许时开放；其余驱动（PG/MySQL）本就前向，拒绝避免误导。
        if (opts.scrollable && !cursorScrollable_)
            return common::Status::error(common::ErrorCode::NotSupported,
                                         "scrollable cursors are disabled for this datasource");
        // 调用方用驱动默认 batch_size 时，用配置里的 default_batch_size 兜底。
        CursorOptions effective = opts;
        if (effective.batch_size == 256 && defaultBatchSize_ != 256)
            effective.batch_size = defaultBatchSize_;
        // 公开入口只过一次闸门：审计 + 限流。游标**不缓存**（流式结果不可直接缓存，
        // 且可能跨事务快照）。传 Select（游标）而非 Query：SqlAuditor::check 据此对游标
        // 豁免 require_limit_select——游标本就是分批消费，强制 LIMIT 会废掉其全量扫描用法。
        if (const auto g = preGate(sql, common::OperationType::Select); !g.ok()) return g;
        // M1 SPI 埋点：顶层入口（I9）。游标无 ResultSet，view.result=nullptr；
        // affected 非游标语义，亦不暴露。
        common::SqlContext ctx = common::ContextScope::current();
        detail::runOnRoute(name_, sql, common::OperationType::Select, ctx);
        ExecutionView view{name_, sql, common::OperationType::Select,
                           &params, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, ctx};
        return runWithInterceptors(view, nullptr, nullptr, [&] {
            return openCursorUngated(sql, params, effective, out);
        });
    }

    common::Status DataSource::openCursorUngated(const std::string &sql, const common::Params &params,
                                                 const CursorOptions &opts,
                                                 std::unique_ptr<Cursor> &out) const
    {
        if (primary_) {
            // 组：路由到读目标（副本/主/候选），失败且可重试时回退主。
            const auto target = readTarget();
            auto status = target->openCursorUngated(sql, params, opts, out);
            if (target != primary_ && fallbackToPrimary_ &&
                (status.retryable || status.connectionBroken ||
                 status.code == common::ErrorCode::CircuitOpen)) {
                out.reset();
                status = primary_->openCursorUngated(sql, params, opts, out);
            }
            return status;
        }
        // 叶子：资源护栏 + 借连接 + 打开游标 + 重试（与 queryUngated 一致，
        // 但 fetch 中途断连不重试——游标状态已丢失，交由调用方重建）。
        std::shared_ptr<void> cursorLease;
        if (!cursorBudgetAcquire(cursorLease)) {
            return common::Status::error(common::ErrorCode::CursorLimit,
                                         "datasource '" + name_ + "': cursor limit reached");
        }
        common::Status status;
        const int attempts = std::max(1, retry_.max_attempts);
        for (int attempt = 1; attempt <= attempts; ++attempt) {
            if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
            std::unique_ptr<ConnectionPool::Handle> h;
            status = borrowSession(h, kUsePoolDefault);
            if (status.ok()) {
                std::unique_ptr<ICursor> impl;
                status = (*h)->openCursor(sql, params, opts, impl);
                if (status.ok() && impl) {
                    out = std::make_unique<Cursor>(std::move(h), std::move(impl),
                                                    Session::AuditContext{true, readOnly_},
                                                    Cursor::Binding::OwnsHandle,
                                                    std::move(cursorLease));
                    return status;
                }
                // 打开失败：impl 为 null 或报错，借出的连接随 h 析构归还，继续重试/上报。
            }
            afterAttempt(status);
            if (status.ok()) return status; // impl 为 null 但 status.ok() 不可能，仅防御
            if (!status.retryable || attempt == attempts) return status;
            std::this_thread::sleep_for(retryDelay(attempt));
        }
        return status;
    }

    bool DataSource::cursorBudgetAcquire(std::shared_ptr<void> &lease) const {
        lease.reset();
        const auto state = cursorBudget_;
        const int limit = state->limit.load();
        if (limit <= 0) return true;
        int open = state->open.load();
        while (open < limit) {
            if (state->open.compare_exchange_weak(open, open + 1)) {
                lease = std::shared_ptr<void>(state.get(), [state](void *) {
                    state->open.fetch_sub(1);
                });
                return true;
            }
        }
        return false;
    }

    common::Status DataSource::executeBatchUngated(const std::string &sql,
                                                   const common::ParamBatch &batch,
                                                   common::BatchResult &out) const {
        if (primary_) {
            std::function<common::Status()> buffered;
            if (writeBuffer_ && writeBuffer_->enabled()) {
                buffered = [primary = primary_, bufferedSql = sql, bufferedBatch = batch] {
                    common::BatchResult ignored;
                    return primary->executeBatchUngated(bufferedSql, bufferedBatch, ignored);
                };
            }
            return dispatchWrite(
                [&sql, &batch, &out](const std::shared_ptr<DataSource> &target) {
                    out.clear();
                    return target->executeBatchUngated(sql, batch, out);
                },
                buffered);
        }
        if (const auto gate = beforeAttempt(); !gate.ok()) return gate;
        std::unique_ptr<ConnectionPool::Handle> h;
        auto status = borrowSession(h, kUsePoolDefault);
        if (status.ok()) {
            Session session(std::move(h), name_);
            status = session.executeBatch(sql, batch, out);
        }
        afterAttempt(status);
        if (status.ok()) markWrite();
        return status; // 批量写默认不重试
    }

    bool DataSource::poolStats(ConnectionPool::Stats &out) const {
        out = {};
        if (!primary_) {
            const auto pool = pool_.lock();
            if (!pool) return false;
            out = pool->stats();
            return true;
        }
        std::unordered_set<const DataSource *> seen;
        bool any = false;
        auto add = [&](const std::shared_ptr<DataSource> &source) {
            if (!source || !seen.insert(source.get()).second) return;
            ConnectionPool::Stats part;
            if (!source->poolStats(part)) return;
            any = true;
            out.minConnections += part.minConnections;
            out.maxConnections += part.maxConnections;
            out.idle += part.idle;
            out.total += part.total;
            out.borrowed += part.borrowed;
            out.waiting += part.waiting;
            out.connectionsCreated += part.connectionsCreated;
            out.connectionsClosed += part.connectionsClosed;
            out.borrowTimeouts += part.borrowTimeouts;
            out.validationFailures += part.validationFailures;
            out.leakWarnings += part.leakWarnings;
            out.maxBorrowed += part.maxBorrowed;
            out.maxWaiting += part.maxWaiting;
            out.borrowRequests += part.borrowRequests;
            out.borrowSuccesses += part.borrowSuccesses;
            out.connectionCreateFailures += part.connectionCreateFailures;
            out.invalidatedConnections += part.invalidatedConnections;
            out.idleEvictions += part.idleEvictions;
            out.lifetimeEvictions += part.lifetimeEvictions;
            out.totalBorrowWait += part.totalBorrowWait;
            out.maxBorrowWait = std::max(out.maxBorrowWait, part.maxBorrowWait);
        };
        add(primary_);
        for (const auto &replica: replicas_) add(replica);
        return any;
    }

    common::Status DataSource::withSession(const SessionFn &fn) const
    {
        // 必须转发到带超时的重载，而不是直接转发到 primary_ 的同名重载：
        // 只有那条路径会回填"会话内是否发生过写"，读写分离要靠它触发写后读。
        return withSession(fn, kUsePoolDefault);
    }

    common::Status DataSource::withSession(const SessionFn &fn,
                                           const std::chrono::milliseconds borrowTimeout) const
    {
        // 会话入口只过限流，不过审计：这里还没有任何 SQL 可审（语句要等回调
        // 跑起来才存在），审计因此下沉到 Session 逐条做。限流留在入口，
        // 否则"把单条 query 全改成 withSession"就能绕开限流，开关等于没有。
        if (const auto g = gateSession(); !g.ok()) return g;
        if (primary_) {
            bool wrote = false;
            // 只读约束定义在组上，必须显式往下传：同一个叶子可能既挂在只读组下、
            // 又挂在可写组下，把 read_only 写死在叶子身上会误伤后者。
            const auto status = primary_->withSessionInternal(fn, borrowTimeout, &wrote, readOnly_);
            // 回调里有没有写只有 Session 知道。只要发生过写（哪怕最终返回失败，
            // 语句也可能已部分生效），就在 read-after-write 窗口内把后续读打到主库，
            // 否则会读到从库的旧数据。
            if (wrote) markWrite();
            return status;
        }
        return withSessionInternal(fn, borrowTimeout, nullptr, readOnly_);
    }

    common::Status DataSource::withSessionInternal(const SessionFn &fn,
                                                   const std::chrono::milliseconds borrowTimeout,
                                                   bool *wroteOut,
                                                   const bool enforceReadOnly) const
    {
        if (wroteOut) *wroteOut = false;
        // 组套组时取"或"：外层只读就一路只读到底，内层不能把它放宽。
        if (primary_)
            return primary_->withSessionInternal(fn, borrowTimeout, wroteOut,
                                                 enforceReadOnly || readOnly_);

        // 与 queryEach / executeBatch 一致，会话也要过熔断闸门。
        // 只做记录与快速失败，不自动重试——回调内容未必幂等。
        if (const auto gate = beforeAttempt(); !gate.ok()) return gate;

        std::unique_ptr<ConnectionPool::Handle> h;
        auto status = borrowSession(h, borrowTimeout);
        if (!status.ok()) {
            afterAttempt(status);
            return status;
        }
        Session s(std::move(h), name_, Session::AuditContext{true, enforceReadOnly});
        status = runGuarded(s, fn);
        afterAttempt(status);
        // 会话内写过东西就得清掉本节点缓存，否则接下来的 query() 会在整个 TTL
        // 里一直读到旧结果。组那一层还会再清一次（组名 + 各副本），不重复不算错。
        if (s.didWrite()) markWrite();
        if (wroteOut) *wroteOut = s.didWrite();
        return status;
    }

    // 4 个公开重载只做一件事：过一次会话闸门（限流），然后汇到 transactionInternal。
    //
    // 它们之间绝不能互相转发——transaction(fn) 调 transaction(options, fn, timeout)
    // 会把限流扣两次令牌，配置的 QPS 上限凭空腰斩一半。
    common::Status DataSource::transaction(const SessionFn &fn) const
    {
        if (const auto g = gateSession(); !g.ok()) return g;
        // M1 SPI（I9）：事务入口不直接发 SQL 拦截器回调——
        // 事务级 SQL 是 Session::query 等子语句，traceId 由 ctx 透传。
        // 这里只推送一次 ctx，事务回调里 Session 子语句读栈顶即可。
        return transactionInternal(common::TransactionOptions{}, fn,
                                   kUsePoolDefault, readOnly_);
    }

    common::Status DataSource::transaction(const SessionFn &fn,
                                           const std::chrono::milliseconds borrowTimeout) const
    {
        if (const auto g = gateSession(); !g.ok()) return g;
        // M1 SPI（I9）：见上。
        return transactionInternal(common::TransactionOptions{}, fn, borrowTimeout, readOnly_);
    }

    common::Status DataSource::transaction(const common::TransactionOptions &options,
                                           const SessionFn &fn) const {
        if (const auto g = gateSession(); !g.ok()) return g;
        // M1 SPI（I9）：见上。
        return transactionInternal(options, fn, kUsePoolDefault, readOnly_);
    }

    common::Status DataSource::transaction(const common::TransactionOptions &options,
                                           const SessionFn &fn,
                                           const std::chrono::milliseconds borrowTimeout) const
    {
        if (const auto g = gateSession(); !g.ok()) return g;
        return transactionInternal(options, fn, borrowTimeout, readOnly_);
    }

    common::Status DataSource::transactionInternal(const common::TransactionOptions &options,
                                                   const SessionFn &fn,
                                                   const std::chrono::milliseconds borrowTimeout,
                                                   const bool enforceReadOnly) const
    {
        if (primary_) {
            // 事务固定走主库，既不做故障转移也不入写缓冲，这是有意为之：
            //   - 转移意味着从头重放整个回调，而回调未必幂等（自增序列、
            //     外部副作用、依赖上一条语句返回值），重放可能造成重复写入；
            //   - 写缓冲是"单条写延后补发"，没有 begin/commit 可言，
            //     把事务塞进去等于把原子性悄悄降级成一堆散装写。
            // 主不可用时就该让事务直接失败，由调用方决定怎么补。
            const auto status = primary_->transactionInternal(
                options, fn, borrowTimeout, enforceReadOnly || readOnly_);
            // 事务里到底写没写，组这一层看不到（语句在用户回调里）。
            // 这里按"可能写过"保守处理：把读拉回主库、清掉缓存。
            // 两者都是往强一致那边偏，代价只是命中率，不会发出旧数据。
            if (status.ok() && !options.readOnly) markWrite();
            return status;
        }

        // 熔断闸门：事务也必须过。
        //
        // 事务恰恰是最慢、最占连接的操作。数据源已经故障时若不让它们快速失败，
        // 请求会一直排队占住连接直到超时，熔断对整个数据源就形同虚设。
        // 这里同样只记录不重试——事务回调未必幂等，自动重放可能造成重复写入。
        if (const auto gate = beforeAttempt(); !gate.ok()) return gate;

        std::unique_ptr<ConnectionPool::Handle> h;
        if (const auto st = borrowSession(h, borrowTimeout); !st.ok()) {
            afterAttempt(st);
            return st;
        }

        // options.readOnly 也并进只读判定：调用方明确开了只读事务，
        // 那么回调里出现写就是它自己的 bug，与其等驱动在 commit 时才报错，
        // 不如在下发前就挡住并指出是哪条语句。
        Session s(std::move(h), name_,
                  Session::AuditContext{true, enforceReadOnly || options.readOnly});
        if (const auto st = s.begin(options); !st.ok()) {
            afterAttempt(st);
            return st;
        }

        std::mutex deadlineMutex;
        std::condition_variable deadlineCv;
        bool finished = false;
        std::atomic<bool> timedOut{false};
        std::atomic<bool> cancelDelivered{false};
        std::thread watcher;
        // 截止时间必须由执行回调的线程预先确定。若在线程函数里调用
        // wait_for(timeout)，新线程迟迟得不到调度时计时会从它真正启动后才开始，
        // 使 20ms 之类的短超时在繁忙 runner 上被静默放宽甚至完全漏掉。
        const bool hasDeadline = options.timeout > std::chrono::milliseconds(0);
        const auto deadline = hasDeadline
            ? std::chrono::steady_clock::now() + options.timeout
            : std::chrono::steady_clock::time_point::max();
        if (hasDeadline) {
            watcher = std::thread([&] {
                std::unique_lock<std::mutex> lock(deadlineMutex);
                if (!deadlineCv.wait_until(lock, deadline, [&] { return finished; })) {
                    timedOut.store(true);
                    lock.unlock();
                    // 看门狗线程：这里绝不能让异常逃逸，否则 std::terminate
                    // 会杀掉整个进程。一个用来提升健壮性的机制，不能反过来
                    // 成为最脆的崩溃点。Session::cancel() 自身也保证不抛，
                    // 这层 catch(...) 是最后一道兜底。
                    try {
                        cancelDelivered.store(s.cancel().ok());
                    } catch (...) {
                        cancelDelivered.store(false);
                    }
                }
            });
        }

        auto operationStatus = runGuarded(s, fn);
        // 即使看门狗线程直到回调结束后才获得调度，主线程仍按同一个绝对
        // deadline 补判，保证“整体期限”不依赖操作系统的线程调度时机。
        if (hasDeadline && std::chrono::steady_clock::now() >= deadline)
            timedOut.store(true);
        {
            std::lock_guard<std::mutex> lock(deadlineMutex);
            finished = true;
        }
        deadlineCv.notify_one();
        if (watcher.joinable()) watcher.join();

        if (timedOut.load()) {
            operationStatus = common::Status::error(
                common::ErrorCode::QueryTimeout,
                "transaction timed out after " + std::to_string(options.timeout.count()) + "ms"
                + (cancelDelivered.load()
                    ? ""
                    : " (driver could not cancel the running statement; "
                      "the callback had to run to completion)"));
            operationStatus.retryable = true;
        }

        afterAttempt(operationStatus);

        if (!operationStatus.ok()) {
            // 回滚失败只记日志：业务失败原因才是调用方关心的返回值。
            if (const auto rb = s.rollback(); !rb.ok()) {
                DBMW_LOG_WARN("datasource [" + name_ + "] rollback failed: " + rb.message);
                // 回滚成功说明写全都撤销了，缓存仍然有效，不必清。
                // 但回滚失败时事务状态未知，写有可能已经落库，
                // 此时必须清缓存——宁可少命中，也不能把旧数据当成新数据发出去。
                if (s.didWrite()) markWrite();
            }
            return operationStatus;
        }

        // fn 可能已自行提交或回滚，此时事务不再处于开启状态，不重复提交。
        if (s.inTransaction()) {
            if (const auto cm = s.commit(); !cm.ok()) {
                afterAttempt(cm);
                // 提交失败同样是"结果未知"：可能服务端已提交、只是回执没回来。
                if (s.didWrite()) markWrite();
                return cm;
            }
        }
        // 提交成功且写过东西：清掉本节点缓存。
        if (s.didWrite()) markWrite();
        return common::Status::OK();
    }

    // -----------------------------------------------------------------------
    // DatabaseManager
    // -----------------------------------------------------------------------
    DatabaseManager::DatabaseManager() = default;

    DatabaseManager::~DatabaseManager() {
        shutdown(std::chrono::milliseconds(0));
    }

    common::Status DatabaseManager::init(const config::GlobalConfig &cfg,
                                         const std::chrono::milliseconds replacementGrace) {
        driver::registerBuiltinDrivers();

        if (cfg.datasources.empty()) {
            return common::Status::error(common::ErrorCode::ConfigError,
                                         "no datasource configured");
        }
        // 先在新容器里把一切建好，成功后再整体替换。
        // 这样任何一步失败都不会在成员里留下半初始化的池。
        std::unordered_map<std::string, std::shared_ptr<ConnectionPool> > newPools;
        std::unordered_map<std::string, std::shared_ptr<DataSource> > newSources;
        std::vector<std::shared_ptr<WriteBuffer> > newWriteBuffers;
        auto newHeartbeat = std::make_unique<HeartbeatManager>(
            std::chrono::milliseconds(cfg.heartbeat_interval_ms));
        const std::chrono::milliseconds borrowTimeout(cfg.pool.borrow_timeout_ms);
        const std::chrono::milliseconds idleTimeout(cfg.pool.idle_timeout_ms);
        const std::chrono::milliseconds maxLifetime(cfg.pool.max_lifetime_ms);
        const std::chrono::milliseconds leakThreshold(cfg.pool.leak_detection_threshold_ms);

        // 先扫一遍 groups 收集副本名。
        //
        // 叶子数据源在 groups 之前建好，而"我是不是读副本"决定了
        // cache_on_replica_only 下它能不能缓存——建的时候就得知道。
        // 换成事后调 setter 会把一个初始化后本该只读的对象变成可变的，
        // 而它正被多线程共享，不值得为省一次遍历去开这个口子。
        std::unordered_set<std::string> replicaNames;
        for (const auto &group: cfg.groups)
            for (const auto &replica: group.replicas)
                replicaNames.insert(replica.name);

        for (const auto &dsc: cfg.datasources) {
            if (newPools.find(dsc.name) != newPools.end()) {
                return common::Status::error(common::ErrorCode::ConfigError,
                                             "duplicate datasource name: " + dsc.name);
            }
            std::shared_ptr<ConnectionPool> pool;
            std::shared_ptr<DataSource> source;
            // init() 也走与 addDataSource 同一份私有助手：建池含网络 IO
            // （预热 min 条连接），因此**必须**在临界区外完成（M4 设计 §6.3）。
            // 失败时这里没有回滚所有前面的池——本次 init 直接整体返错即可，
            // 调用方不会再用半完成的 newPools。
            if (const auto st = buildSingleDataSource(
                dsc, cfg.pool, cfg.retry, cfg.circuit_breaker, cfg.cursor,
                makeRateLimiter(cfg.rate_limit), replicaNames,
                /*attachHeartbeat=*/false, pool, source); !st.ok()) {
                return st;
            }
            newPools[dsc.name] = std::move(pool);
            newSources[dsc.name] = std::move(source);
            // init 走"先建好 newHeartbeat 后整体替换"的语义，所以这里挂到
            // newHeartbeat；addDataSource 走运行时路径，会直接挂到运行中的
            // heartbeat_。两种行为由 attachHeartbeat 开关 + 调用方决定。
            newHeartbeat->addPool(newPools[dsc.name]);
        }

        for (const auto &group: cfg.groups) {
            if (newSources.find(group.name) != newSources.end()) {
                return common::Status::error(common::ErrorCode::ConfigError,
                                             "duplicate datasource/group name: " + group.name);
            }
            // init(GlobalConfig) 也是公开入口，不能只依赖 JSON Loader 校验；
            // 程序化构造配置同样必须显式确认高风险写语义。
            if (!group.failover.primaries.empty() &&
                !group.failover.acknowledge_external_fencing) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + group.name
                    + "' configures automatic write failover without acknowledging "
                      "external fencing");
            }
            if (group.failover.write_buffer.enabled &&
                !group.failover.write_buffer.acknowledge_data_loss_and_duplicates) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + group.name
                    + "' enables volatile write buffering without acknowledging data-loss "
                      "and duplicate-replay risk");
            }
            // 引用完整性校验（抽到私有助手，与 addGroup 共享）。
            if (const auto st = validateGroupRefs(group, newPools, replicaNames); !st.ok())
                return st;

            std::shared_ptr<DataSource> source;
            // 组构造是纯内存，建组可在锁内（与 init 路径行为等价）。
            // opts 留空 = init 路径不需要写额外配置；addGroup 路径会传 opts。
            // poolCfg 保留传参以对齐 init 与 addGroup 路径参数表（仅保留签名），
            // buildSingleDataSourceGroup 内部不读它。
            // sources 用 newSources（已含所有叶子 + 之前建好的组，按引用顺序）；
            // replicaNames 沿用本函数顶部预先扫到的副本名。
            if (const auto st = buildSingleDataSourceGroup(
                group, cfg.pool, /*opts=*/{}, newSources, replicaNames,
                newWriteBuffers, source); !st.ok())
                return st;
            newSources[group.name] = std::move(source);
            // 写缓冲的启动：必须锁外做（init 路径在临界区外统一 start；
            // addGroup 路径则直接把启动延后到锁外），见下一段统一调用。
        }

        if (newSources.find(cfg.default_datasource) == newSources.end()) {
            return common::Status::error(
                common::ErrorCode::ConfigError,
                "default_datasource '" + cfg.default_datasource
                + "' is not defined in datasources[] or groups[]");
        }

        // 全部校验都过了、只剩"整体替换"这一步，此时才落全局策略。
        //
        // 放在校验之前的话，一个配置错误会让 init 返回失败，却已经悄悄把
        // 生效中的审计策略和缓存换掉了——调用方以为回滚了，其实没有。
        SqlAuditor::configure(cfg.sql_audit);
        configurePreparedCache(cfg.prepared_cache);
        // 热加载必须清缓存：数据源名可以不变，但它指向的库/账号/schema
        // 可能已经改了。留着旧条目就是拿 A 库的数据回答 B 库的查询。
        QueryCache::configure(cfg.query_cache);

        // 旧的池与心跳移出临界区后再销毁，避免持锁做耗时 IO。
        std::unordered_map<std::string, std::shared_ptr<ConnectionPool> > oldPools;
        std::unordered_map<std::string, std::shared_ptr<DataSource> > oldSources;
        std::vector<std::shared_ptr<WriteBuffer> > oldWriteBuffers;
        std::unique_ptr<HeartbeatManager> oldHeartbeat;
        newHeartbeat->start();
        for (const auto &buffer: newWriteBuffers) buffer->start();
        {
            std::lock_guard<std::mutex> lk(mtx_);
            oldHeartbeat = std::move(heartbeat_);
            oldPools = std::move(pools_);
            oldSources = std::move(datasources_);
            oldWriteBuffers = std::move(writeBuffers_);

            pools_ = std::move(newPools);
            datasources_ = std::move(newSources);
            writeBuffers_ = std::move(newWriteBuffers);
            heartbeat_ = std::move(newHeartbeat);
            defaultName_ = cfg.default_datasource;
        }

        // M6：把每组的 shadowName_ 解析为强引用——必须在新 datasources_ 已落
        // 锁之后、观察者/对外 API 之前。校验失败直接回滚（把旧池/旧心跳装回
        // datasources_/pools_/writeBuffers_/heartbeat_），保留 init 之前的
        // 运行态，避免一个配置错误把工作进程变成"无数据源"状态。
        if (const auto rs = resolveShadows(); !rs.ok()) {
            std::lock_guard<std::mutex> lk(mtx_);
            // 暂存新（失败）态，让它析构；旧态先 move 回原位再清空暂存。
            auto stalePools = std::move(pools_);
            auto staleSources = std::move(datasources_);
            auto staleBuffers = std::move(writeBuffers_);
            auto staleHeartbeat = std::move(heartbeat_);
            pools_ = std::move(oldPools);
            datasources_ = std::move(oldSources);
            writeBuffers_ = std::move(oldWriteBuffers);
            heartbeat_ = std::move(oldHeartbeat);
            // 清空临时名（init 之前可能为空）。
            stalePools.clear();
            staleSources.clear();
            staleBuffers.clear();
            staleHeartbeat.reset();
            (void) stalePools; (void) staleSources;
            (void) staleBuffers; (void) staleHeartbeat;
            return rs;
        }

        common::Observability::configure(cfg.observability);
        // M3 池指标推送通道：把采集能力注入到 Observability，让外部观察者
        // （Prometheus exporter 等）按周期或按需拿到全量数据源快照。
        // 此处 lambda 只读 pools_，自身不需要锁；DatabaseManager::allPoolStats()
        // 内部会加 mtx_。
        common::Observability::setPoolMetricsCollector([this] { return allPoolStats(); });

        // 统计报告放在最后启动：此时新池与新数据源都已就位，采集回调拿到的
        // 必然是完整状态。start() 内部会先停掉上一版线程，因此热加载时
        // 不会出现两条线程同时写同一个文件。
        if (!statsReporter_) statsReporter_ = std::make_unique<StatsReporter>();
        statsReporter_->start(cfg.observability.stats_report,
                              [this] { return allPoolStats(); });

        if (oldHeartbeat) oldHeartbeat->stop();
        // 旧写缓冲必须在旧池关闭之前停掉，而且要早于 oldSources.clear()：
        // 缓冲里的补发任务持有旧叶子 DataSource 的强引用，线程只要还活着，
        // 就会拿着即将关闭的池反复借连接重试，把 shutdown 拖到超时才结束。
        for (const auto &buffer: oldWriteBuffers) if (buffer) buffer->stop();
        oldWriteBuffers.clear();
        oldSources.clear();
        const auto drainDeadline = std::chrono::steady_clock::now() + replacementGrace;
        for (auto &kv: oldPools) {
            const auto now = std::chrono::steady_clock::now();
            kv.second->shutdown(now < drainDeadline
                ? std::chrono::duration_cast<std::chrono::milliseconds>(drainDeadline - now)
                : std::chrono::milliseconds(0));
        }
        oldPools.clear();

        return common::Status::OK();
    }

    // -------------------------------------------------------------------
    // v0.4.0 M4：动态增删数据源与组。
    //
    // 私有助手先于公开方法，公开方法按"加数据源 → 删数据源 → 加组 → 删组"排列。
    // -------------------------------------------------------------------

    common::Status DatabaseManager::validateGroupRefs(
        const config::DataSourceGroupConfig &cfg,
        const std::unordered_map<std::string, std::shared_ptr<ConnectionPool> > &candidates,
        const std::unordered_set<std::string> &replicaNames) const {
        // primary 必须存在于 candidates（叶子池）。如果它只是另一个组，本次新组
        // 把它当 primary 会让"写路径"指向组——而 writeTargets() 又只接受叶子，
        // 这种二阶嵌套会让写语义失控。与 init 路径保持一致：必须是叶子。
        if (candidates.find(cfg.primary) == candidates.end()) {
            return common::Status::error(
                common::ErrorCode::ConfigError,
                "group '" + cfg.name + "' references unknown primary '"
                + cfg.primary + "' (must be a plain datasource, not a group)");
        }
        for (const auto &replica: cfg.replicas) {
            if (candidates.find(replica.name) == candidates.end()) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + cfg.name + "' references unknown replica '"
                    + replica.name + "'");
            }
        }
        for (const auto &candidate: cfg.failover.primaries) {
            // 写路径要求候选是叶子（与 init 同语义）；同时显式重复检查。
            if (candidates.find(candidate) == candidates.end()) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + cfg.name
                    + "' failover.primaries references unknown datasource '"
                    + candidate + "' (must be a plain datasource, not a group)");
            }
            if (replicaNames.find(candidate) != replicaNames.end()) {
                // 不报错：半同步备库被提升为主是常见拓扑，但要日志提醒运维
                // 确认提升时可写。
                DBMW_LOG_WARN("group [" + cfg.name + "] failover candidate '"
                              + candidate
                              + "' is also configured as a read replica; make sure it is"
                                " writable when promoted");
            }
        }
        return common::Status::OK();
    }

    common::Status DatabaseManager::checkLeafNotInUse_Unused(const std::string &leafName) const {
        // 占位：removeDataSource 路径已用 inline friend 访问绕过此方法，
        // 故保留空实现以兼容头文件 friend 声明；调用方已切到下方 for 循环
        // 直接读 primary_ / replicas_ 私有字段。
        (void) leafName;
        return common::Status::OK();
    }

    common::Status DatabaseManager::buildSingleDataSource(
        const config::DataSourceConfig &dsc,
        const config::PoolConfig &poolCfg,
        const config::RetryConfig &retry,
        const config::CircuitBreakerConfig &circuit,
        const config::CursorConfig &cursor,
        std::shared_ptr<RateLimiter> rateLimiter,
        const std::unordered_set<std::string> &replicaNames,
        /*attachHeartbeat — 由调用方在锁内决定挂哪条 Heartbeat*/
        bool,
        std::shared_ptr<ConnectionPool> &outPool,
        std::shared_ptr<DataSource> &outSource) {
        if (dsc.name.empty()) {
            return common::Status::error(common::ErrorCode::ConfigError,
                                         "datasource name must not be empty");
        }
        auto drv = driver::createDriver(dsc.type);
        if (!drv) {
            return common::Status::error(common::ErrorCode::UnknownDriver,
                                         "unknown datasource type: '" + dsc.type
                                         + "' (name=" + dsc.name + ")");
        }
        const std::chrono::milliseconds borrowTimeout(poolCfg.borrow_timeout_ms);
        const std::chrono::milliseconds idleTimeout(poolCfg.idle_timeout_ms);
        const std::chrono::milliseconds maxLifetime(poolCfg.max_lifetime_ms);
        const std::chrono::milliseconds leakThreshold(poolCfg.leak_detection_threshold_ms);
        // 池化按配置开关：关闭后每次 borrow 新建、归还即关闭，min/max 与预热都
        // 不再生效，而上层 DataSource / Session 的用法完全不变。
        // 预热是网络 IO，调用方必须在锁外调本方法（M4 设计 §6.3）。
        outPool = std::make_shared<ConnectionPool>(
            std::move(drv), dsc, poolCfg.min, poolCfg.max, borrowTimeout,
            idleTimeout, maxLifetime, leakThreshold,
            std::chrono::milliseconds(poolCfg.validation_interval_ms),
            /*metricsEnabled=*/true, // 运行时新增一律开池指标；reload 时按 cfg 传
            poolCfg.enabled);
        outSource = std::make_shared<DataSource>(
            outPool, dsc.name, retry, circuit, std::move(rateLimiter),
            /*readOnly=*/false,
            /*readReplica=*/replicaNames.find(dsc.name) != replicaNames.end());
        outSource->applyCursorConfig(cursor);
        DBMW_LOG_INFO("datasource registered: " + dsc.describe()
                      + (poolCfg.enabled ? "" : " (pooling disabled)"));
        return common::Status::OK();
    }

    common::Status DatabaseManager::buildSingleDataSourceGroup(
        const config::DataSourceGroupConfig &group,
        const config::PoolConfig & /*poolCfg*/, // 保留签名；组 DataSource 本身不直接用
        const GroupOptions &opts,
        const std::unordered_map<std::string, std::shared_ptr<DataSource> > &sources,
        const std::unordered_set<std::string> & /*replicaNames*/,
        std::vector<std::shared_ptr<WriteBuffer> > &outBuffers,
        std::shared_ptr<DataSource> &outSource) {
        if (group.name.empty()) {
            return common::Status::error(common::ErrorCode::ConfigError,
                                         "group name must not be empty");
        }
        if (group.read_only && group.failover.write_buffer.enabled) {
            return common::Status::error(
                common::ErrorCode::ConfigError,
                "group '" + group.name
                + "' is read_only but enables failover.write_buffer;"
                  " a read-only group never writes");
        }
        // init() 与 addGroup() 在调本方法前已分别校验过两个 ack 标志
        // （acknowledge_external_fencing / acknowledge_data_loss_and_duplicates），
        // 此处不再重复。opts 里同名字段目前为冗余保留，便于后续统一到 opts 单点。
        const auto primaryIt = sources.find(group.primary);
        if (primaryIt == sources.end()) {
            return common::Status::error(
                common::ErrorCode::ConfigError,
                "group '" + group.name + "' references unknown primary '"
                + group.primary + "'");
        }
        std::vector<std::shared_ptr<DataSource>> weightedReplicas;
        weightedReplicas.reserve(group.replicas.size());
        for (const auto &replica: group.replicas) {
            const auto replicaIt = sources.find(replica.name);
            if (replicaIt == sources.end()) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + group.name + "' references unknown replica '"
                    + replica.name + "'");
            }
            for (int i = 0; i < replica.weight; ++i)
                weightedReplicas.push_back(replicaIt->second);
        }
        // 故障转移候选：主必须置顶，去重后追加。
        std::vector<std::shared_ptr<DataSource> > failoverPrimaries;
        if (!group.failover.primaries.empty()) {
            failoverPrimaries.push_back(primaryIt->second);
            std::unordered_set<std::string> seenCandidates{group.primary};
            for (const auto &candidateName: group.failover.primaries) {
                if (!seenCandidates.insert(candidateName).second) continue;
                const auto candidateIt = sources.find(candidateName);
                if (candidateIt == sources.end()) {
                    return common::Status::error(
                        common::ErrorCode::ConfigError,
                        "group '" + group.name
                        + "' failover.primaries references unknown datasource '"
                        + candidateName + "' (must be a plain datasource, not a group)");
                }
                failoverPrimaries.push_back(candidateIt->second);
            }
        }
        std::shared_ptr<WriteBuffer> writeBuffer;
        if (group.failover.write_buffer.enabled) {
            // 已在调用方校验 ack；这里仅做日志与构造。
            DBMW_LOG_WARN("group [" + group.name
                          + "] volatile write buffer enabled: Buffered means accepted, not "
                            "committed; process failure may lose writes and replay may duplicate them");
            WriteBuffer::Config wbc;
            wbc.enabled = true;
            wbc.max_queue = group.failover.write_buffer.max_queue;
            wbc.ttl_ms = group.failover.write_buffer.ttl_ms;
            wbc.flush_interval_ms = group.failover.write_buffer.flush_interval_ms;
            writeBuffer = std::make_shared<WriteBuffer>(wbc);
            outBuffers.push_back(writeBuffer);
        }
        outSource = std::make_shared<DataSource>(
            group.name, primaryIt->second, std::move(weightedReplicas),
            std::chrono::milliseconds(group.read_after_write_ms),
            group.fallback_to_primary,
            opts.rate_limiter,
            group.read_only,
            std::move(failoverPrimaries),
            group.failover.require_healthy,
            writeBuffer);
        outSource->applyCursorConfig(opts.cursor);
        // M6：影子名延迟到 resolveShadows 解析——那时 datasources_ 已全量就位，
        // 引用完整性（影子源必须存在、非本组成员、非任何组名）才能成立。
        outSource->shadowName_ = group.shadow;
        DBMW_LOG_INFO("datasource group registered: " + group.name
                      + " primary=" + group.primary
                      + (group.read_only ? " (read-only)" : "")
                      + (group.failover.primaries.empty()
                             ? ""
                             : " failover=" + std::to_string(
                                   group.failover.primaries.size()) + " candidate(s)")
                      + (writeBuffer ? " write-buffer=on" : "")
                      + (group.shadow.empty() ? "" : " shadow=" + group.shadow));
        return common::Status::OK();
    }

    // -------------------------------------------------------------------
    // M6 影子库解析（§8.4）。
    //
    // 校验规则（任一失败返回 ConfigError）：
    //   1. 影子源必须存在于 datasources_ 映射；
    //   2. 影子源不得是任何组名（组不可直接作影子目标——路由只接受叶子）；
    //   3. 影子源不得是该组的成员（自己影自己无意义，且会让影子库的读命中
    //      到源组的副本/主，完全偏离压测目的）。
    //
    // 校验通过则把 shadow_ 填为强引用。路由层 readTarget/writeTargets 在
    // ContextScope::current().shadow 为真且 shadow_ 非空时直接返回。
    // -------------------------------------------------------------------
    common::Status DatabaseManager::resolveShadows() {
        std::lock_guard<std::mutex> lk(mtx_);
        // 收集所有组名（影子源不得是组名）。
        std::unordered_set<std::string> groupNames;
        for (const auto &kv : datasources_) {
            if (kv.second && kv.second->primary_) groupNames.insert(kv.first);
        }
        for (const auto &kv : datasources_) {
            const auto &ds = kv.second;
            if (!ds || ds->shadowName_.empty()) continue;
            const auto &name = ds->shadowName_;
            // ① 影子源必须存在。
            const auto it = datasources_.find(name);
            if (it == datasources_.end() || !it->second) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + ds->name_ + "' references unknown shadow '"
                    + name + "'");
            }
            // ② 影子源不得是任何组名。
            if (groupNames.find(name) != groupNames.end()) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + ds->name_ + "' shadow '" + name
                    + "' is a group; shadow target must be a plain datasource");
            }
            // ③ 影子源不得是该组的成员（主/副本）。
            if (it->second == ds->primary_) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + ds->name_ + "' shadow '" + name
                    + "' is the group's primary; self-shadowing is rejected");
            }
            for (const auto &replica : ds->replicas_) {
                if (replica && replica == it->second) {
                    return common::Status::error(
                        common::ErrorCode::ConfigError,
                        "group '" + ds->name_ + "' shadow '" + name
                        + "' is a replica of the group; self-shadowing is rejected");
                }
            }
            ds->shadow_ = it->second;
        }
        return common::Status::OK();
    }

    // -------------------------------------------------------------------
    // addDataSource：分两段锁，建池含网络 IO 放锁外（M4 §6.3）。
    // -------------------------------------------------------------------
    common::Status DatabaseManager::addDataSource(const config::DataSourceConfig &cfg,
                                                 const DataSourceOptions &opts) {
        if (cfg.name.empty()) {
            return common::Status::error(common::ErrorCode::ConfigError,
                                         "datasource name must not be empty");
        }
        // 段 1：锁内查重；不重名才继续，避免无谓建池。
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (pools_.find(cfg.name) != pools_.end() ||
                datasources_.find(cfg.name) != datasources_.end()) {
                return common::Status::error(common::ErrorCode::ConfigError,
                                             "datasource name already exists: " + cfg.name);
            }
        }
        // 兜底 PoolConfig：addDataSource 没有显式 pool 配置——按默认值即可；
        // 如果以后开放"加池同时改 pool 策略"，再把字段塞进 DataSourceOptions。
        config::PoolConfig runtimePool;
        runtimePool.min = 1;
        runtimePool.max = 32;
        runtimePool.borrow_timeout_ms = 30000;
        runtimePool.idle_timeout_ms = 600000;
        runtimePool.max_lifetime_ms = 1800000;
        runtimePool.leak_detection_threshold_ms = 30000;
        runtimePool.validation_interval_ms = 500;
        runtimePool.enabled = true;

        // replicaNames：运行期新增的 leaf 立刻被"未来 addGroup"引用时，
        // 它就成为读副本。但当前 addGroup 路径并不知道此 leaf 已被注册——
        // 因此这里把它"标记为读副本"的判定保守地视为 false。
        // 已有组在它加入之前已通过 validateGroupRefs 引过它，会因引用存在
        // 而视为副本（init 路径用 cfg 全集做过一次扫描）；addDataSource 之后
        // 调用的 addGroup 引用本 leaf 时，会**漏**把它标为读副本。
        // 这是缓存资格判定的"可见性窗口"，实践中短暂且不致命，故不二次扫描。
        const std::unordered_set<std::string> emptyReplicaNames;
        config::RateLimitConfig defaultRate; // 全 0 = 不限；makeRateLimiter 内部会返回 nullptr
        std::shared_ptr<ConnectionPool> pool;
        std::shared_ptr<DataSource> source;
        std::shared_ptr<RateLimiter> limiter = opts.rate_limiter;
        if (!limiter) limiter = makeRateLimiter(defaultRate);
        if (const auto st = buildSingleDataSource(
            cfg, runtimePool, opts.retry, opts.circuit_breaker, opts.cursor,
            std::move(limiter),
            emptyReplicaNames, opts.attach_heartbeat, pool, source); !st.ok())
            return st;
        // 段 2：锁内插入。若并发已被另一个 add/remove/insert 抢先，返回 ConfigError；
        // pool 出错抛出的资源由 reset 强制关闭（shutdown grace=0）。
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (pools_.find(cfg.name) != pools_.end() ||
                datasources_.find(cfg.name) != datasources_.end()) {
                // 极小并发窗口里被并发 add 抢先；销毁刚建的池（grace=0）。
                pool->shutdown(std::chrono::milliseconds(0));
                return common::Status::error(common::ErrorCode::ConfigError,
                                             "datasource name already exists: " + cfg.name);
            }
            pools_[cfg.name] = pool;
            datasources_[cfg.name] = source;
            if (opts.attach_heartbeat && heartbeat_) heartbeat_->addPool(pool);
        }
        return common::Status::OK();
    }

    // -------------------------------------------------------------------
    // removeDataSource：锁内剔，锁外 shutdown；被组引用时拒绝。
    // -------------------------------------------------------------------
    common::Status DatabaseManager::removeDataSource(const std::string &name,
                                                     const std::chrono::milliseconds grace) {
        std::shared_ptr<ConnectionPool> poolToShutdown;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (pools_.find(name) == pools_.end()) {
                return common::Status::error(common::ErrorCode::ConfigError,
                                             "datasource not found: " + name);
            }
            // 引用完整性：扫描所有"组"datasource，若 name 出现在 primary / replicas
            // / failover.primaries 列表里则拒绝。
            // DatabaseManager 是 DataSource 的 friend，能直接读私有字段。
            // 判定 candidate 是不是"组"：primary_ 非空即为组（与 DataSource 构造
            // 第二个重载一致——叶子用的是带 weak_ptr<ConnectionPool> 的版本，
            // 不会初始化 primary_）。
            for (const auto &kv: datasources_) {
                const auto &candidate = kv.second;
                if (!candidate || candidate->name() == name) continue;
                if (candidate->primary_) {
                    if (candidate->primary_->name() == name) {
                        return common::Status::error(
                            common::ErrorCode::ConfigError,
                            "datasource '" + name + "' is still referenced by group '"
                            + candidate->name() + "' as primary (remove the group first)");
                    }
                    for (const auto &replica: candidate->replicas_) {
                        if (replica && replica->name() == name) {
                            return common::Status::error(
                                common::ErrorCode::ConfigError,
                                "datasource '" + name + "' is still referenced by group '"
                                + candidate->name() + "' as replica (remove the group first)");
                        }
                    }
                    for (const auto &fp: candidate->failoverPrimaries_) {
                        if (fp && fp->name() == name) {
                            return common::Status::error(
                                common::ErrorCode::ConfigError,
                                "datasource '" + name + "' is still referenced by group '"
                                + candidate->name()
                                + "' as failover candidate (remove the group first)");
                        }
                    }
                }
            }
            poolToShutdown = std::move(pools_.at(name));
            pools_.erase(name);
            datasources_.erase(name);
            // 注意：这里不主动清 heartbeat_ 的 weak_ptr——sweepExpiredPools() 每次
            // 心跳都会回收已 expired 的 weak_ptr，无需特殊处理。
        }
        if (poolToShutdown) {
            poolToShutdown->shutdown(grace);
        }
        DBMW_LOG_INFO("datasource removed: " + name);
        return common::Status::OK();
    }

    // -------------------------------------------------------------------
    // addGroup：纯内存构造组 DataSource；writeBuffer 先 push 入 writeBuffers_
    // 再 start（先 push 后 start，让 shutdown 立刻可见，避免已 start 但
    // 找不到的窗口）。锁内构造 + 登记；start 与异常回滚在锁外。
    // -------------------------------------------------------------------
    common::Status DatabaseManager::addGroup(const config::DataSourceGroupConfig &cfg,
                                             const GroupOptions &opts) {
        if (cfg.name.empty()) {
            return common::Status::error(common::ErrorCode::ConfigError,
                                         "group name must not be empty");
        }
        std::vector<std::shared_ptr<WriteBuffer>> stagedBuffers;
        std::shared_ptr<DataSource> source;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (pools_.find(cfg.name) != pools_.end() ||
                datasources_.find(cfg.name) != datasources_.end()) {
                return common::Status::error(common::ErrorCode::ConfigError,
                                             "group name already exists: " + cfg.name);
            }
            // ack 校验：与 init() 相同的硬要求在这里逐项显式表态。
            if (!cfg.failover.primaries.empty() && !opts.acknowledge_external_fencing) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + cfg.name
                    + "' configures automatic write failover without acknowledging "
                      "external fencing");
            }
            if (cfg.failover.write_buffer.enabled && !opts.acknowledge_data_loss_and_duplicates) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + cfg.name
                    + "' enables volatile write buffering without acknowledging data-loss "
                      "and duplicate-replay risk");
            }
            if (const auto st = validateGroupRefs(cfg, pools_, {}); !st.ok())
                return st;
            // sources = datasources_（已含叶子与现存组；引用完整性已校验上述项均为叶子）。
            if (const auto st = buildSingleDataSourceGroup(
                cfg, /*poolCfg=*/{}, opts, datasources_, {},
                stagedBuffers, source); !st.ok())
                return st;
            datasources_[cfg.name] = source;
            // 先 push 到 writeBuffers_（未 start），让 shutdown 立刻可见。
            for (auto &buffer: stagedBuffers) writeBuffers_.push_back(buffer);
        }
        // 锁外 start（start 内部启动后台线程，绝不在持锁时做）。
        if (!stagedBuffers.empty()) {
            try {
                for (auto &buffer: stagedBuffers) {
                    if (buffer) buffer->start();
                }
            } catch (...) {
                // 极端情况：start 抛异常——把本次新加的组与缓冲剔出。
                std::lock_guard<std::mutex> lk(mtx_);
                datasources_.erase(cfg.name);
                for (auto &buffer: stagedBuffers) {
                    if (buffer) buffer->stop();
                    writeBuffers_.erase(std::remove(writeBuffers_.begin(),
                                                    writeBuffers_.end(), buffer),
                                       writeBuffers_.end());
                }
                return common::Status::error(common::ErrorCode::Unknown,
                                             "write buffer start failed");
            }
        }
        // M6：解析本次新组的影子引用。失败回滚——剔出新加的 datasources_ 与
        // 缓冲（缓冲线程已起，先 stop 再移除），保证 addGroup 整体失败语义。
        if (const auto rs = resolveShadows(); !rs.ok()) {
            std::lock_guard<std::mutex> lk(mtx_);
            datasources_.erase(cfg.name);
            for (auto &buffer: stagedBuffers) {
                if (buffer) buffer->stop();
                writeBuffers_.erase(std::remove(writeBuffers_.begin(),
                                                writeBuffers_.end(), buffer),
                                   writeBuffers_.end());
            }
            return rs;
        }
        return common::Status::OK();
    }

    // -------------------------------------------------------------------
    // removeGroup：锁内剔除 + 通过 friend 路径拿出 writeBuffer_；
    // 锁外 stop。组 DataSource 析构让 primary/replicas 弱引用解绑，但不主动
    // 关闭 primary pool（仍可能被其它 datasource 复用）。
    // -------------------------------------------------------------------
    common::Status DatabaseManager::removeGroup(const std::string &name,
                                                const std::chrono::milliseconds grace) {
        (void) grace;
        std::shared_ptr<WriteBuffer> bufferToStop;
        bool wasGroup = false;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            const auto it = datasources_.find(name);
            if (it == datasources_.end()) {
                return common::Status::error(common::ErrorCode::ConfigError,
                                             "group not found: " + name);
            }
            if (pools_.find(name) != pools_.end()) {
                return common::Status::error(common::ErrorCode::ConfigError,
                                             "datasource '" + name
                                             + "' is not a group (use removeDataSource)");
            }
            // friend 路径读 DataSource::writeBuffer_（组成员持同一 shared_ptr）。
            bufferToStop = it->second->writeBuffer_;
            wasGroup = true;
            datasources_.erase(it);
            // 把对应 writeBuffer 指针从成员容器中移除。
            if (bufferToStop) {
                writeBuffers_.erase(std::remove(writeBuffers_.begin(),
                                                writeBuffers_.end(), bufferToStop),
                                   writeBuffers_.end());
            }
        }
        if (wasGroup && bufferToStop) bufferToStop->stop();
        DBMW_LOG_INFO("datasource group removed: " + name);
        return common::Status::OK();
    }

    std::shared_ptr<DataSource> DatabaseManager::getDataSource(const std::string &name) {
        std::lock_guard<std::mutex> lk(mtx_);
        const auto it = datasources_.find(name);
        if (it == datasources_.end()) return nullptr;
        return it->second;
    }

    std::shared_ptr<DataSource> DatabaseManager::getDefault() {
        std::lock_guard<std::mutex> lk(mtx_);
        if (defaultName_.empty()) return nullptr;
        const auto it = datasources_.find(defaultName_);
        if (it == datasources_.end()) return nullptr;
        return it->second;
    }

    void DatabaseManager::shutdown(const std::chrono::milliseconds grace) {
        // 统计线程必须先停：它的采集回调会读 pools_，若让它活过下面的 move，
        // 回调就会摸到已经搬空的容器。stop() 在锁外调用，避免与 allPoolStats()
        // 抢同一把 mtx_ 造成死锁。
        if (statsReporter_) statsReporter_->stop();

        std::unordered_map<std::string, std::shared_ptr<ConnectionPool> > oldPools;
        std::unordered_map<std::string, std::shared_ptr<DataSource> > oldSources;
        std::vector<std::shared_ptr<WriteBuffer> > oldWriteBuffers;
        std::unique_ptr<HeartbeatManager> oldHeartbeat;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            oldHeartbeat = std::move(heartbeat_);
            oldPools = std::move(pools_);
            oldSources = std::move(datasources_);
            oldWriteBuffers = std::move(writeBuffers_);
            defaultName_.clear();
        }
        if (oldHeartbeat) oldHeartbeat->stop();
        // 写缓冲要在池关闭之前停：补发任务持有叶子 DataSource 的强引用，
        // 线程活过连接池就会拿着已关闭的池反复重试，既刷日志又让退出变慢。
        // stop() 里最后一轮 flush 用的还是活着的池，这也是唯一能补上积压的时机。
        for (const auto &buffer: oldWriteBuffers) if (buffer) buffer->stop();
        oldWriteBuffers.clear();
        oldSources.clear();
        const auto drainDeadline = std::chrono::steady_clock::now() + grace;
        for (auto &kv: oldPools) {
            const auto now = std::chrono::steady_clock::now();
            kv.second->shutdown(now < drainDeadline
                ? std::chrono::duration_cast<std::chrono::milliseconds>(drainDeadline - now)
                : std::chrono::milliseconds(0));
        }
        oldPools.clear();
    }

    size_t DatabaseManager::dataSourceCount() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return datasources_.size();
    }

    std::vector<NamedPoolStats> DatabaseManager::allPoolStats() const {
        std::vector<NamedPoolStats> result;
        std::lock_guard<std::mutex> lk(mtx_);
        result.reserve(pools_.size());
        for (const auto & [fst, snd]: pools_)
            result.push_back(NamedPoolStats{fst, snd->stats()});
        std::sort(result.begin(), result.end(), [](const auto &a, const auto &b) {
            return a.dataSource < b.dataSource;
        });
        return result;
    }
} // namespace dbmw::core
