#include "dbmw/core/database_manager.h"

#include <algorithm>

#include "dbmw/driver/driver_factory.h"
#include "dbmw/common/logger.h"
#include "dbmw/common/observer.h"
#include "dbmw/common/sql_analyze.h"
#include "dbmw/core/sql_auditor.h"
#include "dbmw/core/query_cache.h"
#include "dbmw/core/stats_reporter.h"

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
    }

    common::Status Session::query(const std::string &sql, const common::Params &params,
                                  common::ResultSet &out) const
    {
        if (const auto a = auditStatement(sql, common::OperationType::Query); !a.ok()) return a;
        std::uint64_t rows = 0;
        const auto status = observeSql(dataSource_, common::OperationType::Query, sql, params,
                                       h_->get(), rows, [&] {
            const auto result = runPreparedQuery(sql, params, out);
            rows = out.rowCount();
            return result;
        });
        if (status.connectionBroken) h_->invalidate();
        return status;
    }

    common::Status Session::execute(const std::string &sql, std::int64_t &affected) const
    {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
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
    }

    common::Status Session::execute(const std::string &sql, const common::Params &params,
                                    std::int64_t &affected) const
    {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
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
    }

    common::Status Session::queryEach(const std::string &sql, const common::Params &params,
                                      const common::RowCallback &callback,
                                      std::uint64_t &rows) const {
        if (const auto a = auditStatement(sql, common::OperationType::Stream); !a.ok()) return a;
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
    }

    common::Status Session::executeBatch(const std::string &sql,
                                         const common::ParamBatch &batch,
                                         common::BatchResult &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Batch); !a.ok()) return a;
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
    }

    common::Status Session::execute(const std::string &sql, std::int64_t &affected,
                                    common::GeneratedKeys &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
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
    }

    common::Status Session::execute(const std::string &sql, const common::Params &params,
                                    std::int64_t &affected, common::GeneratedKeys &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
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
    }

    common::Status Session::query(const std::string &sql, const common::StreamParams &params,
                                  common::ResultSet &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Query); !a.ok()) return a;
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
    }

    common::Status Session::execute(const std::string &sql, const common::StreamParams &params,
                                    std::int64_t &affected, common::GeneratedKeys &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Execute); !a.ok()) return a;
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
    }

    common::Status Session::executeBatch(const std::string &sql,
                                         const common::StreamParamBatch &batch,
                                         common::BatchResult &out) const {
        if (const auto a = auditStatement(sql, common::OperationType::Batch); !a.ok()) return a;
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
    }

    common::Status Session::prepare(const std::string &sql, const common::Params &typesSample,
                                    PreparedStatementHandle &out) const {
        out = PreparedStatementHandle{};
        // 预备语句也要过审计：借"预备"绕开黑名单等于给拦截开了后门。
        // 分类完全由 SQL 文本决定，与这里传的 OperationType 无关（见 sql_auditor）。
        if (const auto a = auditStatement(sql, common::OperationType::Query); !a.ok()) return a;
        return (*h_)->prepare(sql, typesSample, out);
    }

    common::Status Session::executePrepared(const PreparedStatementHandle &h,
                                            const common::Params &params,
                                            common::ResultSet &out) const {
        std::uint64_t rows = 0;
        // 句柄是不透明令牌，上层拿不到它对应的 SQL 文本，观测里只能记占位模板——
        // 预编译路径的观测重心在耗时与成败，SQL 文本在 prepare 那一步已经审过。
        const auto status = observeSql(dataSource_, common::OperationType::Query, "<prepared>",
                                       params, h_->get(), rows, [&] {
            const auto result = (*h_)->executePrepared(h, params, out);
            rows = out.rowCount();
            return result;
        });
        if (status.connectionBroken) h_->invalidate();
        return status;
    }

    common::Status Session::executePrepared(const PreparedStatementHandle &h,
                                            const common::Params &params,
                                            std::int64_t &affected) const {
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
        key = cacheKey(sql, params);
        return QueryCache::get(name_, key, out);
    }

    void DataSource::cacheStore(const std::string &key, const common::ResultSet &rows) const {
        if (!primary_ && QueryCache::enabled()) QueryCache::put(name_, key, rows);
    }

    common::Status DataSource::query(const std::string &sql, common::ResultSet &out) const
    {
        if (const auto g = preGate(sql, common::OperationType::Query); !g.ok()) return g;
        return queryUngated(sql, out);
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
        const bool caching = QueryCache::enabled() &&
            (!QueryCache::replicaOnly() || readReplica_);
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
                if (caching) QueryCache::put(name_, key, out);
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
        return queryUngated(sql, params, out);
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
            (!QueryCache::replicaOnly() || readReplica_);
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
                if (caching) QueryCache::put(name_, key, out);
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
        return executeUngated(sql, affected);
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
        const int attempts = retry_.retry_writes ? std::max(1, retry_.max_attempts) : 1;
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
        return executeUngated(sql, params, affected);
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
        const int attempts = retry_.retry_writes ? std::max(1, retry_.max_attempts) : 1;
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
        return executeUngated(sql, affected, out);
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
        const int attempts = retry_.retry_writes ? std::max(1, retry_.max_attempts) : 1;
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
        return executeUngated(sql, params, affected, out);
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
        const int attempts = retry_.retry_writes ? std::max(1, retry_.max_attempts) : 1;
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
        return queryUngated(sql, params, out);
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
        return executeUngated(sql, params, affected, out);
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
        return executeBatchUngated(sql, batch, out);
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
        return queryEachUngated(sql, params, callback, rows);
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
        return executeBatchUngated(sql, batch, out);
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
        return openCursorUngated(sql, params, effective, out);
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
        return transactionInternal(common::TransactionOptions{}, fn, kUsePoolDefault, readOnly_);
    }

    common::Status DataSource::transaction(const SessionFn &fn,
                                           const std::chrono::milliseconds borrowTimeout) const
    {
        if (const auto g = gateSession(); !g.ok()) return g;
        return transactionInternal(common::TransactionOptions{}, fn, borrowTimeout, readOnly_);
    }

    common::Status DataSource::transaction(const common::TransactionOptions &options,
                                           const SessionFn &fn) const {
        if (const auto g = gateSession(); !g.ok()) return g;
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
            auto drv = driver::createDriver(dsc.type);
            if (!drv) {
                return common::Status::error(common::ErrorCode::UnknownDriver,
                                             "unknown datasource type: '" + dsc.type
                                             + "' (name=" + dsc.name + ")");
            }
            // 池化按配置开关：关闭后 ConnectionPool 退化为连接工厂，
            // 每次 borrow 新建、归还即关闭，min/max 与预热都不再生效，
            // 而上层 DataSource / Session 的用法完全不变。
            auto pool = std::make_shared<ConnectionPool>(
                std::move(drv), dsc, cfg.pool.min, cfg.pool.max, borrowTimeout,
                idleTimeout, maxLifetime, leakThreshold,
                std::chrono::milliseconds(cfg.pool.validation_interval_ms),
                cfg.observability.pool_metrics.enabled,
                cfg.pool.enabled);
            newPools[dsc.name] = pool;
            newSources[dsc.name] = std::make_shared<DataSource>(
                pool, dsc.name, cfg.retry, cfg.circuit_breaker,
                makeRateLimiter(cfg.rate_limit),
                // 单数据源自身不带只读标志：只读是组级约束，由组在调用时往下传。
                /*readOnly=*/false,
                /*readReplica=*/replicaNames.find(dsc.name) != replicaNames.end());
            newSources[dsc.name]->applyCursorConfig(cfg.cursor);
            newHeartbeat->addPool(pool);
            DBMW_LOG_INFO("datasource registered: " + dsc.describe()
                          + (cfg.pool.enabled ? "" : " (pooling disabled)"));
        }

        for (const auto &group: cfg.groups) {
            if (newSources.find(group.name) != newSources.end()) {
                return common::Status::error(common::ErrorCode::ConfigError,
                                             "duplicate datasource/group name: " + group.name);
            }
            const auto primaryIt = newSources.find(group.primary);
            if (primaryIt == newSources.end() || newPools.find(group.primary) == newPools.end()) {
                return common::Status::error(
                    common::ErrorCode::ConfigError,
                    "group '" + group.name + "' references unknown primary '"
                    + group.primary + "'");
            }
            std::vector<std::shared_ptr<DataSource>> weightedReplicas;
            for (const auto &replica: group.replicas) {
                const auto replicaIt = newSources.find(replica.name);
                if (replicaIt == newSources.end() || newPools.find(replica.name) == newPools.end()) {
                    return common::Status::error(
                        common::ErrorCode::ConfigError,
                        "group '" + group.name + "' references unknown replica '"
                        + replica.name + "'");
                }
                for (int i = 0; i < replica.weight; ++i)
                    weightedReplicas.push_back(replicaIt->second);
            }

            // 故障转移候选：主必须置顶。
            //
            // writeTargets() 是按序取第一个可用的，主不排第一就会出现
            // "主明明活着、写却打到备库"的情况。列表为空表示不启用转移，
            // 写只走主（保持原语义）。
            std::vector<std::shared_ptr<DataSource> > failoverPrimaries;
            if (!group.failover.primaries.empty()) {
                failoverPrimaries.push_back(primaryIt->second);
                std::unordered_set<std::string> seenCandidates{group.primary};
                for (const auto &candidateName: group.failover.primaries) {
                    // 重复项要跳过，否则同一个已故障的节点会被连试多次，
                    // 每次都要等一遍借连接超时，转移延迟成倍放大。
                    if (!seenCandidates.insert(candidateName).second) continue;
                    const auto candidateIt = newSources.find(candidateName);
                    // 必须同时在 newPools 里——只在 newSources 里说明它是另一个组。
                    // 候选写路径要直接借连接执行，指向组会再套一层读写路由，
                    // 写可能被转移出去第二次，落点彻底失控。
                    if (candidateIt == newSources.end() ||
                        newPools.find(candidateName) == newPools.end()) {
                        return common::Status::error(
                            common::ErrorCode::ConfigError,
                            "group '" + group.name
                            + "' failover.primaries references unknown datasource '"
                            + candidateName + "' (must be a plain datasource, not a group)");
                    }
                    if (replicaNames.find(candidateName) != replicaNames.end()) {
                        // 不报错：半同步备库被提升为主是最常见的转移拓扑，
                        // 但中间件不会替你把它的 read_only 标志摘掉，必须提醒。
                        DBMW_LOG_WARN("group [" + group.name + "] failover candidate '"
                                      + candidateName
                                      + "' is also configured as a read replica; make sure it is"
                                        " writable when promoted");
                    }
                    failoverPrimaries.push_back(candidateIt->second);
                }
            }

            std::shared_ptr<WriteBuffer> writeBuffer;
            if (group.failover.write_buffer.enabled) {
                if (group.read_only) {
                    return common::Status::error(
                        common::ErrorCode::ConfigError,
                        "group '" + group.name
                        + "' is read_only but enables failover.write_buffer;"
                          " a read-only group never writes");
                }
                WriteBuffer::Config wbc;
                wbc.enabled = true;
                wbc.max_queue = group.failover.write_buffer.max_queue;
                wbc.ttl_ms = group.failover.write_buffer.ttl_ms;
                wbc.flush_interval_ms = group.failover.write_buffer.flush_interval_ms;
                writeBuffer = std::make_shared<WriteBuffer>(wbc);
                newWriteBuffers.push_back(writeBuffer);
            }

            newSources[group.name] = std::make_shared<DataSource>(
                group.name, primaryIt->second, std::move(weightedReplicas),
                std::chrono::milliseconds(group.read_after_write_ms),
                group.fallback_to_primary,
                makeRateLimiter(cfg.rate_limit),
                group.read_only,
                std::move(failoverPrimaries),
                group.failover.require_healthy,
                writeBuffer);
            newSources[group.name]->applyCursorConfig(cfg.cursor);
            DBMW_LOG_INFO("datasource group registered: " + group.name
                          + " primary=" + group.primary
                          + (group.read_only ? " (read-only)" : "")
                          + (group.failover.primaries.empty()
                                 ? ""
                                 : " failover=" + std::to_string(
                                       group.failover.primaries.size()) + " candidate(s)")
                          + (writeBuffer ? " write-buffer=on" : ""));
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

        common::Observability::configure(cfg.observability);

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
