#ifndef DBMW_DRIVER_MYSQL_DRIVER_H
#define DBMW_DRIVER_MYSQL_DRIVER_H

#include "dbmw/core/idatabase_connection.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/driver/idriver.h"
#include "dbmw/common/types.h"

#include <string>
#include <memory>
#include <mutex>
#include <list>
#include <unordered_map>

#ifdef DBMW_ENABLE_MYSQL
#include <mysql/mysql.h>

// MySQL 8.0 移除了 my_bool（改为 bool），5.7 及更早版本只有 my_bool。
// MYSQL_BIND::is_null 的类型随版本不同，这里统一成一个别名。
#if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 80000
using MysqlBool = bool;
#else
using MysqlBool = my_bool;
#endif

// 承载 MYSQL_BIND::is_null 这类标志位数组的容器。
//
// 必须用 unique_ptr<T[]> 而**不能**用 std::vector<T>：MySQL 8 下 MysqlBool 就是 bool，
// 而 std::vector<bool> 有位压缩特化，operator[] 返回代理对象 std::vector<bool>::reference
// 而非 bool&——对它取地址得到的是代理对象的地址（且是右值），既拿不到 bool*，
// 也无法赋给 MYSQL_BIND::is_null（bool*）。unique_ptr<bool[]>::operator[] 返回真正的引用。
using MysqlBoolArray = std::unique_ptr<MysqlBool[]>;
#endif


namespace dbmw::driver {
    // MySQL 连接（libmysqlclient）。完整实现见 mysql_driver.cpp，
    // 由 DBMW_ENABLE_MYSQL 控制是否参与真实编译（否则仅返回 DriverDisabled）。
    class MySQLConnection : public core::IDatabaseConnection {
    public:
        MySQLConnection() = default;

        ~MySQLConnection() override { MySQLConnection::close(); }

        common::Status connect(const config::DataSourceConfig &cfg) override;

        common::Status ping() override;

        common::Status query(const std::string &sql, common::ResultSet &out) override;

        common::Status execute(const std::string &sql, std::int64_t &affected) override;

        // 参数化查询：走 mysql_stmt_prepare + mysql_stmt_bind_param 的服务端预处理。
        common::Status query(const std::string &sql, const common::Params &params,
                             common::ResultSet &out) override;

        common::Status execute(const std::string &sql, const common::Params &params,
                               std::int64_t &affected) override;

        // 生成键 / 自增 ID：mysql_insert_id 合成一行一列（无需改 SQL）。
        common::Status execute(const std::string &sql, std::int64_t &affected,
                               common::GeneratedKeys &out) override;

        common::Status execute(const std::string &sql, const common::Params &params,
                               std::int64_t &affected, common::GeneratedKeys &out) override;

        // 预编译语句复用（连接级句柄缓存，prepare-once / execute-many）。
        [[nodiscard]] bool supportsPrepared() const override;
        common::Status prepare(const std::string &sql, const common::Params &typesSample,
                              core::PreparedStatementHandle &out) override;
        common::Status executePrepared(const core::PreparedStatementHandle &h,
                                      const common::Params &params,
                                      common::ResultSet &out) override;
        common::Status executePrepared(const core::PreparedStatementHandle &h,
                                      const common::Params &params,
                                      std::int64_t &affected) override;
        void closeAllPrepared() override;
        void setPreparedCacheLimit(int maxPerConnection) override;

        common::Status queryEach(const std::string &sql, const common::Params &params,
                                 const common::RowCallback &callback,
                                 std::uint64_t &rows) override;

        // 非缓冲流式游标：prepare + bind + execute（mysql_use_result 风格），
        // 不调用 mysql_stmt_store_result，按批从服务端取行。连接被钉住直到
        // 游标关闭（MySQL 在结果集消费完之前不允许在该连接上发起其它查询）。
        common::Status openCursor(const std::string &sql, const common::Params &params,
                                 const core::CursorOptions &opts,
                                 std::unique_ptr<core::ICursor> &out) override;

        // 字符串用连接感知的 mysql_real_escape_string 转义，
        // 能正确处理连接字符集与 NO_BACKSLASH_ESCAPES 模式。
        [[nodiscard]] std::string escapeLiteral(const common::Value &v) const override;

        [[nodiscard]] bool supportsParams() const override {
#ifdef DBMW_ENABLE_MYSQL
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

        [[nodiscard]] bool isOpen() const override { return open_; }

        // 必须反映真实事务状态：基类 executeBatch 靠它判断是否需要自己包事务。
        [[nodiscard]] bool inTransaction() const override { return txOpen_; }

        common::Status cancel() override;

    private:
        common::Status lastError(const char *where);

        // MyCursor 直接借用 m_ 与结果绑定缓冲，需访问私有成员，故设为友元。
        friend class MyCursor;

#ifdef DBMW_ENABLE_MYSQL
        common::Status stmtError(const char *where, MYSQL_STMT *stmt);
#endif

        bool open_ = false;
        bool txOpen_ = false;
        config::DataSourceConfig cfg_;

        // 预编译语句连接级缓存：key = SQL + 参数类型签名；句柄是不透明的，
        // 原生 MYSQL_STMT* 存于 handle.native()（void*），连接归还池后缓存仍保留，
        // 随连接关闭（closeAllPrepared）释放。preparedLimit_>0 时按 LRU 淘汰。
        std::unordered_map<std::string, core::PreparedStatementHandle> preparedCache_;
        std::list<std::string> preparedLru_;
        std::uint64_t preparedSeq_ = 0;
        int preparedLimit_ = 0;
#ifdef DBMW_ENABLE_MYSQL
        MYSQL *m_ = nullptr;
        mutable std::mutex operationMtx_;
        unsigned long activeThreadId_ = 0;
#endif
    };

    class MySQLDriver : public IDriver {
    public:
        [[nodiscard]] const char *name() const override { return "mysql"; }

        std::unique_ptr<core::IDatabaseConnection> createConnection() override {
            return std::make_unique<MySQLConnection>();
        }
    };

    // 向 DriverRegistry 注册 "mysql" 驱动。
    void registerMySQLDriver();
} // namespace dbmw::driver


#endif // DBMW_DRIVER_MYSQL_DRIVER_H
