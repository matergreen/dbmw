#ifndef DBMW_CORE_DATABASE_MANAGER_H
#define DBMW_CORE_DATABASE_MANAGER_H

#include "dbmw/config/datasource_config.h"
#include "dbmw/core/connection_pool.h"
#include "dbmw/core/heartbeat_manager.h"
#include "dbmw/core/rate_limiter.h"
#include "dbmw/core/write_buffer.h"
#include "dbmw/common/observer.h"  // common::OperationType（审计/闸门签名使用）
#include "dbmw/common/types.h"
#include "dbmw/core/cursor.h"     // ICursor / CursorOptions（游标抽象层）

#include <chrono>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>


namespace dbmw {

    // v0.2.0：异步管线引擎（实现见 src/async/async_engine.cpp）。
    // 前向声明即可——DataSource 只需要给它 friend 访问。
    namespace async::detail {
        class AsyncEngine;
    }

    namespace core {

    // 前向声明：Session / DataSource 的 openCursor 在 Cursor 完整定义（下方）之前就以
    // std::unique_ptr<Cursor> 作参数；按引用传 unique_ptr 只需不完整类型，前置声明即可，
    // 避免把整个 Cursor 类上移到 Session 之前。
    class Cursor;

    // 会话：在一条借出的独占连接上执行多条语句。
    //
    // 这是事务得以成立的前提——query()/execute() 每次都会重新借一条连接，
    // 因此跨语句的事务必须用 Session 固定住同一条连接。
    // Session 由 DataSource::withSession() / transaction() 创建，析构时归还连接。
    class Session {
    public:
        // 会话内逐条语句的审计上下文。
        //
        // 单条 query()/execute() 走 DataSource 入口时已经审计过了，不能在这里再审一遍
        // （会话内重复审计只会刷出成倍的告警日志）；而 withSession()/transaction()
        // 的语句由用户回调临时拼出，DataSource 在入口处根本看不到它们，
        // 只能由 Session 逐条把关。所以是否审计必须由创建者显式指定。
        struct AuditContext {
            bool enabled = false;
            // 该会话是否只读（来自所属 group 的 read_only 或事务的 readOnly 选项）。
            bool readOnly = false;

            // 显式构造，避免依赖“带默认成员初始化器的聚合 + {} 默认实参”的
            // 初始化形式（部分编译器在嵌套类型作为成员函数默认实参时会报
            // “无法用 {} 转换 AuditContext”）。默认构造走成员初始化器得到 false/false。
            AuditContext() = default;
            AuditContext(bool e, bool ro) : enabled(e), readOnly(ro) {}
        };

        Session(const Session &) = delete;

        Session &operator=(const Session &) = delete;

        // 会话结束时若事务仍开着，自动回滚并作废该连接。
        //
        // withSession 不接管事务，调用方 begin 之后如果既没 commit 也没 rollback
        // （比如中途 return 或抛异常），连接一旦原样还池，下一个借用者就会接手
        // 一条处于"半个事务"里的连接——它看到的数据取决于上一个调用的残局，
        // 而且事务会一直挂着占住锁。这类问题极难复现，必须在池边界挡住。
        ~Session();

        // std::atomic 不可移动，因此移动操作必须手写。
        Session(Session &&other) noexcept
            : h_(std::move(other.h_)), dataSource_(std::move(other.dataSource_)),
              audit_(other.audit_), txOpen_(other.txOpen_),
              didWrite_(other.didWrite_.load()) {
            other.txOpen_ = false;
        }

        Session &operator=(Session &&other) noexcept {
            if (this != &other) {
                h_ = std::move(other.h_);
                dataSource_ = std::move(other.dataSource_);
                audit_ = other.audit_;
                txOpen_ = other.txOpen_;
                other.txOpen_ = false;
                didWrite_.store(other.didWrite_.load());
            }
            return *this;
        }

        common::Status query(const std::string &sql, common::ResultSet &out) const;

        common::Status query(const std::string &sql, const common::Params &params,
                             common::ResultSet &out) const;

        common::Status execute(const std::string &sql, std::int64_t &affected) const;

        common::Status execute(const std::string &sql, const common::Params &params,
                               std::int64_t &affected) const;

        common::Status queryEach(const std::string &sql, const common::Params &params,
                                 const common::RowCallback &callback,
                                 std::uint64_t &rows) const;

        common::Status executeBatch(const std::string &sql,
                                    const common::ParamBatch &batch,
                                    common::BatchResult &out) const;

        // -------------------------------------------------------------------
        // 生成键（自增 ID / RETURNING）
        // -------------------------------------------------------------------

        // 执行并回吐数据库生成的键。
        //
        // MySQL 由 mysql_insert_id 合成一行一列，无需改 SQL；
        // PG / ODBC 靠 SQL 自带的 RETURNING / OUTPUT 直出——dbmw 不会自动补写，
        // 想拿生成键就自己写 RETURNING。拿不到时 out.empty() 为真（不是错误）。
        common::Status execute(const std::string &sql, std::int64_t &affected,
                               common::GeneratedKeys &out) const;

        common::Status execute(const std::string &sql, const common::Params &params,
                               std::int64_t &affected, common::GeneratedKeys &out) const;

        // -------------------------------------------------------------------
        // 大参数流式（data-at-execution）
        // -------------------------------------------------------------------

        // 带流式参数执行：StreamSource 由驱动按块拉取，超大 BLOB 不整体进内存。
        // libpq 不支持该协议，PostgreSQL 走"读入 Blob"的降级（基类默认行为）。
        common::Status query(const std::string &sql, const common::StreamParams &params,
                             common::ResultSet &out) const;

        common::Status execute(const std::string &sql, const common::StreamParams &params,
                               std::int64_t &affected, common::GeneratedKeys &out) const;

        // 批量流式：原子性与 executeBatch 一致（未在事务中时自动包一层事务）。
        common::Status executeBatch(const std::string &sql,
                                    const common::StreamParamBatch &batch,
                                    common::BatchResult &out) const;

        // -------------------------------------------------------------------
        // 预编译语句（显式句柄 API）
        // -------------------------------------------------------------------

        // 在本会话的连接上 prepare 一条语句，返回可复用句柄。
        //
        // 句柄绑在**具体连接**上，所以只能从 Session 拿——DBMW / DataSource 的
        // 无状态接口每次都重新借连接，持有不了跨调用的句柄。
        //
        // 会话结束（连接归还）后句柄仍随连接保留，下次借到同一条连接可继续复用；
        // 但**不要**把句柄存到 Session 之外长期持有：连接随时可能被池驱逐或判死。
        common::Status prepare(const std::string &sql, const common::Params &typesSample,
                               PreparedStatementHandle &out) const;

        common::Status executePrepared(const PreparedStatementHandle &h,
                                       const common::Params &params,
                                       common::ResultSet &out) const;

        common::Status executePrepared(const PreparedStatementHandle &h,
                                       const common::Params &params,
                                       std::int64_t &affected) const;

        // 在会话/事务内打开游标：连接仍归本 Session，游标为 BorrowedInSession 绑定。
        // 逐条审计（入口处还无 SQL）；不另计限流（入口已扣过）。游标随本会话
        // 的其余语句共享同一条连接与（若已开）事务快照；close() 后连接仍归 Session。
        common::Status openCursor(const std::string &sql, const common::Params &params,
                                  const CursorOptions &opts,
                                  std::unique_ptr<Cursor> &out) const;

        // 事务控制。begin() 成功后 inTransaction() 为 true，
        // commit()/rollback() 后回到 false。
        common::Status begin();

        common::Status begin(const common::TransactionOptions &options);

        common::Status commit();

        common::Status rollback();

        common::Status savepoint(const std::string &name);
        common::Status releaseSavepoint(const std::string &name);
        common::Status rollbackToSavepoint(const std::string &name);

        // 从另一线程请求取消本会话当前正在执行的语句。
        //
        // 这是唯一的跨线程入口（事务超时看门狗会在另一条线程上调它），因此内部
        // 保证不抛异常——线程里逃逸异常会 std::terminate 掉整个进程。
        [[nodiscard]] common::Status cancel() const;

        [[nodiscard]] bool inTransaction() const { return txOpen_; }

        // 本会话是否成功执行过写语句。
        // 读写分离用它做 read-after-write 判定：withSession 的回调由用户书写，
        // DataSource 无法从外部判断里面有没有写，只能问 Session。
        [[nodiscard]] bool didWrite() const { return didWrite_.load(); }

    private:
        friend class DataSource;

        // 2 参：审计上下文取默认（enabled=false, readOnly=false）。
        // 单独重载而非默认实参，避免“嵌套类型作成员函数默认实参”的边角初始化问题。
        explicit Session(std::unique_ptr<ConnectionPool::Handle> h, std::string dataSource)
            : h_(std::move(h)), dataSource_(std::move(dataSource)), audit_() {}

        // 3 参：显式传入审计上下文（withSession / transaction 路径）。
        explicit Session(std::unique_ptr<ConnectionPool::Handle> h, std::string dataSource,
                         AuditContext audit)
            : h_(std::move(h)), dataSource_(std::move(dataSource)), audit_(std::move(audit)) {}

        // 返回非 ok 表示该语句被审计拦截，调用方应直接返回、不要下发到驱动。
        [[nodiscard]] common::Status auditStatement(const std::string &sql,
                                                    common::OperationType type) const;

        // 透明预编译缓存的执行入口。
        //
        // 驱动支持（supportsPrepared()）且全局开关打开时走 prepare + executePrepared；
        // 否则——**包括预编译本身失败时**——一律退回既有的直接执行路径。
        // 幂等性来自驱动：prepare() 内部按 (SQL, 参数类型签名) 查本连接缓存，
        // 命中就直接返回既有句柄，因此这里每次调用只多一次哈希表查找。
        [[nodiscard]] common::Status runPreparedQuery(const std::string &sql,
                                                      const common::Params &params,
                                                      common::ResultSet &out) const;

        // keys 非空表示调用方要生成键：此时**不走**预编译路径。
        // executePrepared 拿不到 RETURNING 出来的结果集，为了少一次 prepare 而
        // 让调用方静默拿不到主键是本末倒置——正确性优先。
        [[nodiscard]] common::Status runPreparedExec(const std::string &sql,
                                                     const common::Params &params,
                                                     std::int64_t &affected,
                                                     common::GeneratedKeys *keys) const;

        std::unique_ptr<ConnectionPool::Handle> h_;
        std::string dataSource_;
        AuditContext audit_;
        bool txOpen_ = false;
        // 会被业务线程与看门狗线程并发读写（cancel 之后置位），必须是原子的。
        mutable std::atomic<bool> didWrite_{false};
    };

    // 游标：可持有、可多次取、可显式关闭的 RAII 包装。
    //
    // 两种绑定模式：
    //  - OwnsHandle：独立游标，持有连接 Handle 直到 close()/析构（钉连接）。
    //    用于 DataSource::openCursor——游标生命周期独立于任何事务/会话。
    //  - BorrowedInSession：事务/会话内游标，连接仍归 Session 所有，游标只借不占。
    //    close() 仅关服务端游标，不归还连接（连接随 Session 析构归还）。
    //
    // 一个连接同一时刻只能被一个游标钉住（或一条 Session 占用）；游标非线程安全。
    class Cursor {
    public:
        enum class Binding { OwnsHandle, BorrowedInSession };

        Cursor(std::unique_ptr<ConnectionPool::Handle> h,
               std::unique_ptr<ICursor> impl,
               Session::AuditContext audit,
               Binding binding)
            : handle_(std::move(h)), impl_(std::move(impl)),
              audit_(std::move(audit)), binding_(binding) {}

        Cursor(const Cursor &) = delete;
        Cursor &operator=(const Cursor &) = delete;
        // 移动可默认：impl_ 为 unique_ptr，按值移动语义成立。
        Cursor(Cursor &&) noexcept = default;
        Cursor &operator=(Cursor &&) noexcept = default;

        // 取一批（最多 n 行，n==0 由驱动按 batch_size 决定）；结果追加到 out。
        common::Status fetch(std::size_t n, common::ResultSet &out) {
            if (!impl_) return common::Status::error(common::ErrorCode::CursorClosed,
                                                    "cursor already closed or moved-from");
            return impl_->fetch(n, out);
        }

        // 取单行；无更多行时 ok=false（正常 EOF，非错误）。
        common::Status fetchRow(common::Row &out, bool &ok) {
            ok = false;
            if (!impl_) return common::Status::error(common::ErrorCode::CursorClosed,
                                                    "cursor already closed or moved-from");
            return impl_->fetchRow(out, ok);
        }

        // 显式关闭；幂等。BorrowedInSession 不归还连接（归 Session）。
        common::Status close() {
            if (!impl_) return common::Status::OK();
            const auto st = impl_->close();
            impl_.reset();
            return st;
        }

        [[nodiscard]] bool isOpen() const { return impl_ && impl_->isOpen(); }
        [[nodiscard]] bool hasNext() const { return impl_ && impl_->hasNext(); }
        [[nodiscard]] std::uint64_t rowsFetched() const {
            return impl_ ? impl_->rowsFetched() : 0;
        }

        // 析构：关游标（幂等），OwnsHandle 时 Handle 随 unique_ptr 析构归还连接。
        // noexcept：析构里 close 失败只记日志，绝不在析构中抛异常。
        // 定义放在 database_manager.cpp（已 include logger.h），避免头文件引入日志宏依赖。
        ~Cursor() noexcept;

    private:
        std::unique_ptr<ConnectionPool::Handle> handle_; // OwnsHandle 时钉连接；Borrowed 时为 null
        std::unique_ptr<ICursor> impl_;
        Session::AuditContext audit_;
        Binding binding_;
    };

    // 会话回调：返回非 ok 表示失败（事务场景会触发回滚）。
    using SessionFn = std::function<common::Status(Session &)>;

    struct NamedPoolStats {
        std::string dataSource;
        ConnectionPool::Stats stats;
    };

    // 面向应用的单数据源句柄：内部从连接池借连接执行（RAII）。
    //
    // 持有的是 weak_ptr<ConnectionPool>，池被销毁后所有操作返回 PoolClosed，
    // 不会出现悬垂访问。
    class DataSource {
    public:
        DataSource(std::weak_ptr<ConnectionPool> pool, std::string name,
                   config::RetryConfig retry = {},
                   config::CircuitBreakerConfig circuitBreaker = {},
                   std::shared_ptr<RateLimiter> rateLimiter = nullptr,
                   bool readOnly = false, bool readReplica = false)
            : pool_(std::move(pool)), name_(std::move(name)), retry_(retry),
              circuitBreaker_(circuitBreaker), rateLimiter_(std::move(rateLimiter)),
              readOnly_(readOnly), readReplica_(readReplica) {}

        // 读写路由数据源：写/事务固定 primary（可按 failoverPrimaries 转移），
        // 读按权重轮询 replicas。
        DataSource(std::string name, std::shared_ptr<DataSource> primary,
                   std::vector<std::shared_ptr<DataSource>> weightedReplicas,
                   std::chrono::milliseconds readAfterWrite,
                   bool fallbackToPrimary,
                   std::shared_ptr<RateLimiter> rateLimiter = nullptr,
                   bool readOnly = false,
                   std::vector<std::shared_ptr<DataSource>> failoverPrimaries = {},
                   bool requireHealthy = false,
                   std::shared_ptr<WriteBuffer> writeBuffer = nullptr)
            : name_(std::move(name)), primary_(std::move(primary)),
              replicas_(std::move(weightedReplicas)),
              readAfterWrite_(readAfterWrite), fallbackToPrimary_(fallbackToPrimary),
              rateLimiter_(std::move(rateLimiter)), readOnly_(readOnly),
              failoverPrimaries_(std::move(failoverPrimaries)),
              requireHealthy_(requireHealthy),
              writeBuffer_(std::move(writeBuffer)) {}

        // 单条语句：每次借一条连接，用完立即归还。
        common::Status query(const std::string &sql, common::ResultSet &out) const;

        common::Status query(const std::string &sql, const common::Params &params,
                             common::ResultSet &out) const;

        common::Status execute(const std::string &sql, std::int64_t &affected) const;

        common::Status execute(const std::string &sql, const common::Params &params,
                               std::int64_t &affected) const;

        common::Status queryEach(const std::string &sql, const common::Params &params,
                                 const common::RowCallback &callback,
                                 std::uint64_t &rows) const;

        common::Status executeBatch(const std::string &sql,
                                    const common::ParamBatch &batch,
                                    common::BatchResult &out) const;

        // 生成键：执行并回吐自增主键 / RETURNING 出来的列。
        //
        // 语义与 Session 的同名方法一致：MySQL 开箱即得，
        // PG / ODBC 需要在 SQL 里自己写 RETURNING / OUTPUT。
        common::Status execute(const std::string &sql, std::int64_t &affected,
                               common::GeneratedKeys &out) const;

        common::Status execute(const std::string &sql, const common::Params &params,
                               std::int64_t &affected, common::GeneratedKeys &out) const;

        // 大参数流式：StreamSource 由驱动按块拉取，超大 BLOB 不整体进内存。
        //
        // 注意含 StreamSource 的查询**不进结果缓存**——流式内容不是定值，
        // 无法参与 cacheKey，与游标的处理一致。
        common::Status query(const std::string &sql, const common::StreamParams &params,
                             common::ResultSet &out) const;

        common::Status execute(const std::string &sql, const common::StreamParams &params,
                               std::int64_t &affected, common::GeneratedKeys &out) const;

        common::Status executeBatch(const std::string &sql,
                                    const common::StreamParamBatch &batch,
                                    common::BatchResult &out) const;

        // 打开游标：过 preGate（审计+限流，不缓存），按读路由借连接钉住并打开游标。
        // 失败（限流/审计/连接/驱动不支持）按既有语义返回错误；成功时 out 持有游标。
        // 游标必须显式 close() 或随 unique_ptr 析构，否则连接被长期钉住（池会告警）。
        common::Status openCursor(const std::string &sql, const common::Params &params,
                                  const CursorOptions &opts,
                                  std::unique_ptr<Cursor> &out) const;

        // 在一条独占连接上执行 fn（不自动开启事务），fn 结束后归还连接。
        // 适合需要连续多条语句保持同一连接的场景（临时表、会话变量等）。
        common::Status withSession(const SessionFn &fn) const;

        common::Status withSession(const SessionFn &fn, std::chrono::milliseconds borrowTimeout) const;

        // 在一条独占连接上开启事务执行 fn：
        //   fn 返回成功且事务仍开着 -> 提交
        //   fn 返回失败或抛出异常    -> 回滚
        // fn 内部也可以自行 commit()/rollback()，此时外层不会重复提交。
        common::Status transaction(const SessionFn &fn) const;

        common::Status transaction(const SessionFn &fn, std::chrono::milliseconds borrowTimeout) const;

        common::Status transaction(const common::TransactionOptions &options,
                                   const SessionFn &fn) const;

        common::Status transaction(const common::TransactionOptions &options,
                                   const SessionFn &fn,
                                   std::chrono::milliseconds borrowTimeout) const;

        [[nodiscard]] const std::string &name() const { return name_; }

        bool poolStats(ConnectionPool::Stats &out) const;

        // 由 DatabaseManager::init 按 cursor.* 配置调用，设置本数据源的游标开关、
        // 默认批大小、滚动许可与并发配额上限（0 = 不限制）。
        // 必须在任何 openCursor 之前调用一次；热加载会再次调用以更新策略。
        void applyCursorConfig(const config::CursorConfig &cfg) {
            cursorEnabled_ = cfg.enabled;
            defaultBatchSize_ = cfg.default_batch_size > 0 ? cfg.default_batch_size : 256;
            cursorScrollable_ = cfg.allow_scrollable;
            cursorBudget_ = cfg.max_open_cursors; // 0 表示不限制
        }

    private:
        friend class DatabaseManager;
        friend class async::detail::AsyncEngine;

        common::Status beforeAttempt() const;
        void afterAttempt(const common::Status &status) const;
        [[nodiscard]] std::chrono::milliseconds retryDelay(int attempt) const;
        [[nodiscard]] std::shared_ptr<DataSource> readTarget() const;
        void markWrite() const;

        // 审计 + 限流前置闸门：返回 Ok 放行，否则 SqlBlocked / RateLimited。
        //
        // 只在"一次业务调用"的最外层执行一次。组转发给叶子时走下面的 *Ungated
        // 版本，绕开第二次闸门——重复审计会刷出成倍告警，重复扣令牌会让配置的
        // QPS 上限凭空腰斩（组扣一次、叶子再扣一次，实际只剩一半）。
        common::Status preGate(const std::string &sql, common::OperationType type) const;

        // 会话/事务入口的闸门：只限流，不审计。
        //
        // 会话里执行哪些语句要等用户回调跑起来才知道，入口处没有 SQL 可审；
        // 审计因此下沉到 Session 逐条进行。限流仍留在入口，否则"全部改用
        // transaction()"就能绕过限流，这个开关等于没有。
        common::Status gateSession() const;

        // 已过闸门的执行入口（组 -> 叶转发、写缓冲补发都用这些）。
        common::Status queryUngated(const std::string &sql, common::ResultSet &out) const;

        common::Status queryUngated(const std::string &sql, const common::Params &params,
                                    common::ResultSet &out) const;

        common::Status executeUngated(const std::string &sql, std::int64_t &affected) const;

        common::Status executeUngated(const std::string &sql, const common::Params &params,
                                      std::int64_t &affected) const;

        common::Status queryEachUngated(const std::string &sql, const common::Params &params,
                                        const common::RowCallback &callback,
                                        std::uint64_t &rows) const;

        common::Status executeBatchUngated(const std::string &sql,
                                           const common::ParamBatch &batch,
                                           common::BatchResult &out) const;

        // 生成键 / 流式的已过闸门执行入口（组 -> 叶转发用）。
        //
        // (sql, affected, keys) 与 (sql, params, affected) 参数个数相同但类型不同，
        // 不会构成重载歧义：int64_t& 无法转成 Params，反之亦然。
        common::Status executeUngated(const std::string &sql, std::int64_t &affected,
                                      common::GeneratedKeys &out) const;

        common::Status executeUngated(const std::string &sql, const common::Params &params,
                                      std::int64_t &affected,
                                      common::GeneratedKeys &out) const;

        common::Status queryUngated(const std::string &sql, const common::StreamParams &params,
                                    common::ResultSet &out) const;

        common::Status executeUngated(const std::string &sql, const common::StreamParams &params,
                                      std::int64_t &affected,
                                      common::GeneratedKeys &out) const;

        common::Status executeBatchUngated(const std::string &sql,
                                           const common::StreamParamBatch &batch,
                                           common::BatchResult &out) const;

        // openCursor 的内部实现：组路由到叶子；叶子做资源护栏+借连接+打开游标。
        common::Status openCursorUngated(const std::string &sql, const common::Params &params,
                                         const CursorOptions &opts,
                                         std::unique_ptr<Cursor> &out) const;

        // 游标资源护栏：0 = 不限制；非 0 = 剩余配额。
        bool cursorBudgetAcquire() const;
        void cursorBudgetRelease() const;

        // 组的写路径：按 failover 候选顺序尝试，全部不可用时走写缓冲。
        // attempt(target) 在选定候选上执行一次写；buffered 为写缓冲补发任务。
        common::Status dispatchWrite(
            const std::function<common::Status(const std::shared_ptr<DataSource> &)> &attempt,
            const std::function<common::Status()> &buffered) const;

        // 是否处于熔断开放状态（只读检查，不消耗半开探测令牌）。
        [[nodiscard]] bool isCircuitOpen() const;

        // 写路径候选：按 failover 顺序返回未熔断（且可选健康）的可写节点。
        // 全部不可用时返回空表，由调用方决定是报错还是入写缓冲。
        [[nodiscard]] std::vector<std::shared_ptr<DataSource>> writeTargets() const;

        common::Status borrowSession(std::unique_ptr<ConnectionPool::Handle> &out,
                                     std::chrono::milliseconds timeout) const;

        // ===== v0.2.0 异步引擎接缝（AsyncEngine 专用，见 docs/async-design-v0.2.0.md §8）=====
        //
        // 引擎自己经 borrowAsync 借连接（零线程等待），随后用它构造会话执行。
        // 审计已在调用线程经 preGate 过掉，这里与同步单语句路径一致传默认上下文。

        // 用已借到的连接构造本数据源的会话。
        std::unique_ptr<Session> makeSession(
            std::unique_ptr<ConnectionPool::Handle> h) const {
            return std::unique_ptr<Session>(new Session(std::move(h), name_));
        }

        // 本数据源（叶子）的连接池；组返回 null（引擎先经 readTarget/writeTargets
        // 解析到叶子）。内联访问器，避免引擎直接摸 pool_ 成员。
        [[nodiscard]] std::shared_ptr<ConnectionPool> pool() const { return pool_.lock(); }

        // 结果缓存三件套（叶子语义；组上 cacheEligible 恒 false）。
        // cacheLookup 命中返回 true 并填充 out 与 key（供成功后 cacheStore 复用）。
        [[nodiscard]] bool cacheEligible() const;
        bool cacheLookup(const std::string &sql, const common::Params &params,
                         common::ResultSet &out, std::string &key) const;
        void cacheStore(const std::string &key, const common::ResultSet &rows) const;

        // withSession 的真正实现。wroteOut 非空时回填"会话内是否发生过写"，
        // 供读写分离组在委托给 primary 之后做 read-after-write 标记。
        // enforceReadOnly 由组向下传递自己的 read_only 标志——只读约束定义在组上，
        // 而叶子可能同时挂在可写组下，不能把它写死在叶子身上。
        common::Status withSessionInternal(const SessionFn &fn,
                                           std::chrono::milliseconds borrowTimeout,
                                           bool *wroteOut,
                                           bool enforceReadOnly) const;

        // 4 个公开 transaction 重载最终都汇到这里，闸门只在公开入口过一次。
        common::Status transactionInternal(const common::TransactionOptions &options,
                                           const SessionFn &fn,
                                           std::chrono::milliseconds borrowTimeout,
                                           bool enforceReadOnly) const;

        std::weak_ptr<ConnectionPool> pool_;
        std::string name_;
        config::RetryConfig retry_;
        config::CircuitBreakerConfig circuitBreaker_;
        // 熔断状态用原子变量维护，避免每次请求都抢一把 mutex。三字段配合：
        //   - consecutiveFailures_：连续（连接类）失败计数；
        //   - halfOpenInFlight_：半开探测令牌，保证同时只有一个线程去试；
        //   - circuitOpenUntil_：熔断开放时刻，epoch 表示闭合。
        mutable std::atomic<int> consecutiveFailures_{0};
        mutable std::atomic<bool> halfOpenInFlight_{false};
        mutable std::atomic<std::chrono::steady_clock::time_point> circuitOpenUntil_{};
        std::shared_ptr<DataSource> primary_;
        std::vector<std::shared_ptr<DataSource>> replicas_;
        std::chrono::milliseconds readAfterWrite_{0};
        bool fallbackToPrimary_ = true;
        mutable std::atomic<std::int64_t> lastWriteNs_{0};
        // 每数据源限流器（top-level 入口限流用）。
        std::shared_ptr<RateLimiter> rateLimiter_;
        // 该数据源/组是否只读（配合全局 sql_audit.enforce_read_only 才会拦截）。
        bool readOnly_ = false;
        // 是否为读副本（驱动 cache_on_replica_only）。
        bool readReplica_ = false;
        // 主库故障转移：有序可写候选（主置顶）。为空表示不启用转移，写只走主。
        std::vector<std::shared_ptr<DataSource>> failoverPrimaries_;
        // 故障转移是否要求候选连接池健康（true 时跳过 pool 已销毁的候选）。
        bool requireHealthy_ = false;
        // 写缓冲（仅 group 持有；主与候选都不可用时接管写）。
        std::shared_ptr<WriteBuffer> writeBuffer_;
        // 游标资源护栏：剩余可开游标配额。0 表示不限制（unlimited）。
        // 由 DatabaseManager::init 按 cursor.max_open_cursors 设置。
        // 游标占用的是池连接数，必须限制，否则一个忘关的游标能拖垮整个池。
        mutable std::atomic<int> cursorBudget_{0};
        // 游标能力开关（由 applyCursorConfig 设置）。
        bool cursorEnabled_ = true;
        int defaultBatchSize_ = 256;        // 未显式指定 batch_size 时的兜底
        bool cursorScrollable_ = false;    // 是否允许滚动游标（仅 ODBC 真支持）
    };

    // 定时统计报告。完整定义只对实现可见，
    // 这样本头文件不必反向依赖统计模块（那边需要用到这里的 NamedPoolStats）。
    class StatsReporter;

    // 多数据源管理器：加载配置 -> 建池 -> 启动心跳 -> 按名分发。
    // 整个进程通常持有一个实例（见 dbmw.h 门面）。
    class DatabaseManager {
    public:
        // 默认构造延迟到 .cpp 定义：成员 unique_ptr<StatsReporter> 的析构
        // 需要 StatsReporter 完整类型，而本头仅前向声明它，故构造/析构
        // 都必须在实现文件里实例化，否则内联默认构造会触发不完整的 sizeof。
        DatabaseManager();

        ~DatabaseManager();

        DatabaseManager(const DatabaseManager &) = delete;

        DatabaseManager &operator=(const DatabaseManager &) = delete;

        // 用全局配置初始化：注册驱动、建立连接池、启动心跳。
        // 全程先在临时容器里建好再整体替换，任何一步失败都不会留下半初始化状态。
        common::Status init(const config::GlobalConfig &cfg,
                            std::chrono::milliseconds replacementGrace =
                                std::chrono::milliseconds(0));

        std::shared_ptr<DataSource> getDataSource(const std::string &name);

        std::shared_ptr<DataSource> getDefault();

        // 关闭所有连接池与心跳；grace 为等待借用中连接归还的宽限期。
        void shutdown(std::chrono::milliseconds grace = std::chrono::milliseconds(5000));

        size_t dataSourceCount() const;

        std::vector<NamedPoolStats> allPoolStats() const;

    private:
        mutable std::mutex mtx_;
        std::unordered_map<std::string, std::shared_ptr<ConnectionPool> > pools_;
        std::unordered_map<std::string, std::shared_ptr<DataSource> > datasources_;
        std::unique_ptr<HeartbeatManager> heartbeat_;
        // 各读写组的写缓冲。必须由管理器持有并在 shutdown 里先于连接池停掉：
        // 缓冲里的补发任务持有叶子 DataSource 的强引用，线程活过池的生命周期
        // 就会拿着已关闭的池反复重试。
        std::vector<std::shared_ptr<WriteBuffer>> writeBuffers_;
        std::string defaultName_;
        // 定时统计报告线程。
        //
        // 它的采集回调会读 pools_，因此必须在 shutdown 里先于池与数据源停止，
        // 否则线程会比被采集对象活得久，回调直接踩到悬垂引用。
        std::unique_ptr<StatsReporter> statsReporter_;
    };

} // namespace core
} // namespace dbmw


#endif // DBMW_CORE_DATABASE_MANAGER_H
