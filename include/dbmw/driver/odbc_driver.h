#ifndef DBMW_DRIVER_ODBC_DRIVER_H
#define DBMW_DRIVER_ODBC_DRIVER_H

#include "dbmw/core/idatabase_connection.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/driver/idriver.h"
#include "dbmw/common/types.h"

#include <string>
#include <mutex>
#include <list>
#include <unordered_map>


namespace dbmw::driver {
    // ODBC 连接（unixODBC）：统一接入 SQL Server / Oracle 等支持 ODBC 的数据库。
    class OdbcConnection : public core::IDatabaseConnection {
    public:
        ~OdbcConnection() override;

        common::Status connect(const config::DataSourceConfig &cfg) override;

        common::Status ping() override;

        common::Status query(const std::string &sql, common::ResultSet &out) override;

        common::Status execute(const std::string &sql, int64_t &affected) override;

        common::Status query(const std::string &sql, const common::Params &params,
                             common::ResultSet &out) override;

        common::Status execute(const std::string &sql, const common::Params &params,
                               int64_t &affected) override;

        // 生成键 / 自增 ID：靠 SQL 自带的 RETURNING / OUTPUT 直出（不自动补 RETURNING）。
        common::Status execute(const std::string &sql, int64_t &affected,
                               common::GeneratedKeys &out) override;
        common::Status execute(const std::string &sql, const common::Params &params,
                               int64_t &affected, common::GeneratedKeys &out) override;

        // 预编译语句复用（连接级 SQLHSTMT 缓存，prepare-once / execute-many）。
        // ODBC 用原生 '?' 占位，无需改写为 $n。
        [[nodiscard]] bool supportsPrepared() const override;
        common::Status prepare(const std::string &sql, const common::Params &typesSample,
                               core::PreparedStatementHandle &out) override;
        common::Status executePrepared(const core::PreparedStatementHandle &h,
                                       const common::Params &params,
                                       common::ResultSet &out) override;
        common::Status executePrepared(const core::PreparedStatementHandle &h,
                                       const common::Params &params,
                                       int64_t &affected) override;
        void closeAllPrepared() override;
        void setPreparedCacheLimit(int maxPerConnection) override;

        common::Status queryEach(const std::string &sql, const common::Params &params,
                                 const common::RowCallback &callback,
                                 std::uint64_t &rows) override;

        // 真游标：设置 SQL_ATTR_CURSOR_TYPE（scrollable 时用 STATIC）后执行，
        // 用 SQLFetch 按批从服务端取行。ODBC 是唯一支持滚动游标的驱动。
        common::Status openCursor(const std::string &sql, const common::Params &params,
                                 const core::CursorOptions &opts,
                                 std::unique_ptr<core::ICursor> &out) override;

        [[nodiscard]] bool supportsParams() const override {
#ifdef DBMW_ENABLE_ODBC
            return true;
#else
            return false;
#endif
        }

        common::Status begin() override;

        common::Status begin(const common::TransactionOptions &options) override;

        common::Status commit() override;

        common::Status rollback() override;
        common::Status savepoint(const std::string &name) override;
        common::Status releaseSavepoint(const std::string &name) override;
        common::Status rollbackToSavepoint(const std::string &name) override;

        void close() override;

        bool isOpen() const override { return open_; }

        // 必须反映真实事务状态：基类 executeBatch 靠它判断是否需要自己包事务。
        [[nodiscard]] bool inTransaction() const override { return txOpen_; }

        common::Status cancel() override;

    private:
        // OdbcCursor 直接借用连接句柄与结果读取逻辑，需访问私有成员，故设为友元。
        friend class OdbcCursor;

        bool open_ = false;
        bool txOpen_ = false;
        config::DataSourceConfig cfg_;

        // SQLHANDLE 在 ODBC 头文件里本质上是不透明指针。头文件保持不依赖
        // unixODBC，使未开启 DBMW_ENABLE_ODBC 的核心构建仍可离线编译。
        void *env_ = nullptr;
        void *dbc_ = nullptr;
        void *activeStmt_ = nullptr;
        std::mutex activeStmtMtx_;
        std::uint64_t defaultIsolation_ = 0;

        // 预编译语句连接级缓存：key = SQL + 参数类型签名。句柄存原生 SQLHSTMT；
        // 连接归还池后缓存保留，随连接关闭（closeAllPrepared -> SQLFreeHandle）释放。
        std::unordered_map<std::string, core::PreparedStatementHandle> preparedCache_;
        std::list<std::string> preparedLru_;
        std::uint64_t preparedSeq_ = 0;
        int preparedLimit_ = 0;
    };

    class OdbcDriver : public IDriver {
    public:
        const char *name() const override { return "odbc"; }

        std::unique_ptr<core::IDatabaseConnection> createConnection() override {
            return std::make_unique<OdbcConnection>();
        }
    };

    // 向 DriverRegistry 注册 "odbc" 驱动。
    void registerOdbcDriver();
} // namespace dbmw::driver


#endif // DBMW_DRIVER_ODBC_DRIVER_H
