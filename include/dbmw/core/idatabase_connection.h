#ifndef DBMW_CORE_IDATABASE_CONNECTION_H
#define DBMW_CORE_IDATABASE_CONNECTION_H

#include "dbmw/common/types.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/core/cursor.h"
#include <cstdint>
#include <string>


namespace dbmw::core {
    // 连接级预编译语句句柄。
    //
    // 由 prepare() 产出，绑定到"当前这条物理连接"：只在借出期间（含 withSession /
    // transaction 钉住期间）有效；连接归还池后缓存随连接保留，下次借到同一条连接
    // 直接复用；连接被池驱逐 / 心跳判死 / 关闭时才随 closeAllPrepared() 释放。
    //
    // 上层把它当不透明令牌使用，不要解释内部字段。驱动内部可用 id() 做缓存表的键，
    // 或用 native() 直接持有客户端库句柄（MYSQL_STMT* / SQLHSTMT）。
    class PreparedStatementHandle {
    public:
        PreparedStatementHandle() = default;

        // 默认构造 / 移动走之后为 false。
        [[nodiscard]] bool valid() const { return id_ != 0; }
        [[nodiscard]] std::uint64_t id() const { return id_; }
        void *native() const { return native_; }

        // 仅供驱动实现调用：构造一个句柄（id 必须非 0，native 允许为 nullptr）。
        static PreparedStatementHandle make(std::uint64_t id, void *native) {
            PreparedStatementHandle h;
            h.id_ = id;
            h.native_ = native;
            return h;
        }

    private:
        std::uint64_t id_ = 0;
        void *native_ = nullptr;
    };

    // 数据库连接抽象接口：每种数据库驱动实现该接口。
    // 上层（连接池/数据源）只依赖此抽象，不关心具体数据库。
    //
    // 生产驱动应实现原生参数绑定。不支持时默认返回 NotSupported；只有明确
    // 选择兼容插值的驱动才会启用字面量替换。
    class IDatabaseConnection {
    public:
        virtual ~IDatabaseConnection() = default;

        // 建立连接（使用配置中的连接参数）。
        virtual common::Status connect(const config::DataSourceConfig &cfg) = 0;

        // 心跳/保活探测，返回是否存活（实现可发 SELECT 1 / ping）。
        virtual common::Status ping() = 0;

        // 查询，结果写入 out（SELECT 等返回结果集的语句）。
        virtual common::Status query(const std::string &sql, common::ResultSet &out) = 0;

        // 执行（INSERT/UPDATE/DELETE/DDL），affected 返回受影响行数。
        virtual common::Status execute(const std::string &sql, std::int64_t &affected) = 0;

        // 事务控制。
        virtual common::Status begin() = 0;

        // 带隔离级别/只读属性的事务。默认实现仅接受默认选项。
        virtual common::Status begin(const common::TransactionOptions &options);

        virtual common::Status commit() = 0;

        virtual common::Status rollback() = 0;

        virtual common::Status savepoint(const std::string &name);
        virtual common::Status releaseSavepoint(const std::string &name);
        virtual common::Status rollbackToSavepoint(const std::string &name);

        // 关闭连接。可重复调用。
        virtual void close() = 0;

        // 当前是否处于打开状态。
        [[nodiscard]] virtual bool isOpen() const = 0;

        // 取消当前正在执行的语句。驱动不支持时返回 NotSupported。
        // 调用方可从另一线程调用；具体驱动必须使用客户端库提供的取消原语。
        virtual common::Status cancel();

        // 逐行消费结果；callback 返回 false 时提前停止。
        // 默认实现兼容性优先（先构造 ResultSet）；内置驱动可覆盖为真正流式读取。
        virtual common::Status queryEach(const std::string &sql,
                                         const common::Params &params,
                                         const common::RowCallback &callback,
                                         std::uint64_t &rows);

        // 打开游标（如驱动支持）。默认实现返回 NotSupported（兼容旧驱动/能力禁用），
        // 与现有 query(params) 的默认行为保持一致。成功时 out 持有 ICursor，
        // 失败（含驱动不支持）时 out 为空且返回非 Ok 状态。
        //
        // 游标会钉住本连接（PG 上还连带事务）直到 close()/析构，上层必须显式关闭。
        virtual common::Status openCursor(const std::string &sql,
                                          const common::Params &params,
                                          const CursorOptions &opts,
                                          std::unique_ptr<ICursor> &out);

        // 本连接上是否已有开启的事务。
        //
        // 基类默认返回 false（无状态驱动）。有事务状态的驱动必须覆盖，
        // 否则 executeBatch 的默认实现会在调用方已开的事务里再套一层 begin，
        // 在 MySQL 上等于隐式 COMMIT 掉调用方的上半段。
        [[nodiscard]] virtual bool inTransaction() const { return false; }

        // 在同一物理连接上批量执行。
        //
        // 默认实现保证**整批原子**：未在事务中时自动包一层事务，中途失败整体
        // 回滚。驱动可覆盖为数组绑定 / COPY 等更高效的实现，但必须保持同样的
        // 原子性保证——三种内置驱动的语义必须一致。
        virtual common::Status executeBatch(const std::string &sql,
                                            const common::ParamBatch &batch,
                                            common::BatchResult &out);

        // -------------------------------------------------------------------
        // 参数化查询（可选能力）
        // -------------------------------------------------------------------

        // 带绑定参数的查询/执行，SQL 中使用 '?' 作为占位符。
        //
        // 默认返回 NotSupported。兼容驱动可显式允许字面量插值，生产驱动应覆盖
        // 为服务端绑定，避免方言、编码和二进制边界差异。
        virtual common::Status query(const std::string &sql,
                                     const common::Params &params,
                                     common::ResultSet &out);

        virtual common::Status execute(const std::string &sql,
                                       const common::Params &params,
                                       std::int64_t &affected);

        // 该驱动是否支持服务端参数绑定（true 表示上面的重载走的是真实绑定）。
        [[nodiscard]] virtual bool supportsParams() const { return false; }

        [[nodiscard]] virtual bool allowsLiteralInterpolation() const { return false; }

        // -------------------------------------------------------------------
        // 预编译语句复用（可选能力）
        // -------------------------------------------------------------------

        // 在本连接上 prepare 一条语句，返回可复用句柄。
        //
        // typesSample 只用于推导参数类型签名（占位值即可，不需要真实数据）——
        // PG / MySQL 在 prepare 阶段就需要参数类型，类型序列不同必须视为不同语句。
        // 驱动不支持服务端绑定时返回 NotSupported（基类默认）。
        virtual common::Status prepare(const std::string &sql,
                                       const common::Params &typesSample,
                                       PreparedStatementHandle &out);

        // 用已编译句柄执行（查询 / 写）。句柄必须来自**本连接**的 prepare()，
        // 跨连接使用是未定义行为（句柄绑在物理连接上）。
        virtual common::Status executePrepared(const PreparedStatementHandle &h,
                                               const common::Params &params,
                                               common::ResultSet &out);

        virtual common::Status executePrepared(const PreparedStatementHandle &h,
                                               const common::Params &params,
                                               std::int64_t &affected);

        // 释放本连接上所有预编译句柄。
        //
        // 由驱动自己的 close() 调用——连接归还池后缓存仍要保留，所以释放点不能
        // 挂在借出边界上。基类默认为空操作：没有句柄可释放的驱动无需关心。
        virtual void closeAllPrepared();

        // 该驱动是否支持预编译语句复用。
        //
        // DataSource 的透明自动缓存据此决定走不走 getOrPrepare 路径；
        // 返回 false 时退化为既有的"每次重新绑定执行"，行为与今天完全一致。
        [[nodiscard]] virtual bool supportsPrepared() const { return false; }

        // 下发本连接的预编译句柄上限（0 = 不限制）。
        //
        // 由 DataSource 在借到连接时统一下发：连接是池化的、会在多个调用方之间
        // 流转，上限只能每次借出时刷一次，不能指望驱动自己去读全局配置。
        // 基类为空实现——不支持预编译的驱动无需关心。
        virtual void setPreparedCacheLimit(int maxPerConnection) { (void) maxPerConnection; }

        // 把值渲染为本方言的 SQL 字面量。
        // 默认实现是标准 SQL 写法；各驱动应覆盖以处理本方言差异
        // （例如 MySQL 的反斜杠、PostgreSQL 的 bytea）。
        [[nodiscard]] virtual std::string escapeLiteral(const common::Value &v) const;

        // 按驱动方言生成诊断用完整 SQL。只用于日志/排障，不得用于执行。
        common::Status renderSqlForLogging(const std::string &sql,
                                           const common::Params &params,
                                           const common::SqlRenderOptions &options,
                                           std::string &out) const;

        // -------------------------------------------------------------------
        // 生成键（自增 ID / RETURNING，可选能力）
        // -------------------------------------------------------------------

        // 执行并回吐数据库生成的键。
        //
        // 统一模型是"被插入行生成的列"组成的结果集：
        //   - MySQL：mysql_insert_id 合成一行一列，无需改 SQL，开箱即得；
        //   - PG / ODBC：靠 SQL 自带的 RETURNING / OUTPUT 直出，不写就没有。
        // dbmw **不会**自动给 SQL 追加 RETURNING——那会改写语句语义并与方言耦合。
        //
        // 基类默认：委托既有的无键 execute，out 留空（老驱动零改动）。
        virtual common::Status execute(const std::string &sql, std::int64_t &affected,
                                       common::GeneratedKeys &out);

        virtual common::Status execute(const std::string &sql, const common::Params &params,
                                       std::int64_t &affected, common::GeneratedKeys &out);

        // -------------------------------------------------------------------
        // 大参数流式（data-at-execution，可选能力）
        // -------------------------------------------------------------------

        // 带流式参数的查询 / 执行：StreamSource 由驱动按块拉取，
        // 超大 BLOB / CLOB 因此不必整体物化进内存。
        //
        // 基类默认 = **流式降级**：把流整段读入 Blob 后委托给既有的 Params 路径。
        // libpq 协议不支持参数的 data-at-execution，PostgreSQL 驱动直接继承该默认；
        // 其它老驱动也随之自动获得这份能力，不必改一行代码。
        //
        // 注意：含 StreamSource 的查询**不进结果缓存**（流式内容不是定值，
        // 无法参与 cacheKey），与游标一致。
        virtual common::Status query(const std::string &sql,
                                     const common::StreamParams &params,
                                     common::ResultSet &out);

        virtual common::Status execute(const std::string &sql,
                                       const common::StreamParams &params,
                                       std::int64_t &affected,
                                       common::GeneratedKeys &out);

        // 批量流式：基类默认先把每组流转成 Params，再委托既有的 executeBatch，
        // 因此同样享有整批原子性（未在事务中时自动包一层事务，失败整体回滚）。
        virtual common::Status executeBatch(const std::string &sql,
                                            const common::StreamParamBatch &batch,
                                            common::BatchResult &out);

    protected:
        // 占位符替换回调：入参是占位符序号（从 0 开始），返回替换文本。
        using PlaceholderVisitor = std::function<std::string(std::size_t)>;

        // 扫描 sql，把位于引号/注释之外的 '?' 依次替换为 visitor(i) 的结果。
        // found 输出遇到的占位符个数。引号与注释内的 '?' 原样保留。
        static std::string replacePlaceholders(const std::string &sql,
                                               const PlaceholderVisitor &visitor,
                                               std::size_t &found);

        // 按 '?' 占位符顺序把 params 插值进 sql（基于 replacePlaceholders）。
        // 占位符数量与参数数量不一致时返回 QueryError。
        common::Status buildSql(const std::string &sql, const common::Params &params,
                                std::string &out) const;
    };
} // namespace dbmw::core


#endif // DBMW_CORE_IDATABASE_CONNECTION_H
