#ifndef DBMW_DRIVER_POSTGRES_DRIVER_H
#define DBMW_DRIVER_POSTGRES_DRIVER_H

#include "dbmw/core/idatabase_connection.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/driver/idriver.h"
#include "dbmw/common/types.h"

#include <string>
#include <memory>
#include <mutex>
#include <cstdint>
#include <list>
#include <unordered_map>

#ifdef DBMW_ENABLE_POSTGRES
#include <pqxx/pqxx>
#endif


namespace dbmw::driver {
#ifdef DBMW_ENABLE_POSTGRES
    // 事务类型别名：libpqxx 8.0 删除了 pqxx::work，而 7.x 里它本就只是
    //     using work = transaction<>;
    // transaction 的默认模板参数（read_committed + read_write）在 7.x 与 8.x 上
    // 完全一致，因此写成 pqxx::transaction<> 即可同时覆盖 7.8 ~ 8.x。
    using PgTx = pqxx::transaction<>;
#endif

    // PostgreSQL 连接（libpqxx）。完整实现见 postgres_driver.cpp，
    // 由 DBMW_ENABLE_POSTGRES 控制是否参与真实编译（否则仅返回 DriverDisabled）。
    class PostgresConnection : public core::IDatabaseConnection {
    public:
        PostgresConnection() = default;

        ~PostgresConnection() override { PostgresConnection::close(); }

        common::Status connect(const config::DataSourceConfig &cfg) override;

        common::Status ping() override;

        common::Status query(const std::string &sql, common::ResultSet &out) override;

        common::Status execute(const std::string &sql, std::int64_t &affected) override;

        // 参数化查询：'?' 会被改写为 libpq 的 $1/$2/... 并走服务端绑定。
        common::Status query(const std::string &sql, const common::Params &params,
                             common::ResultSet &out) override;

        common::Status execute(const std::string &sql, const common::Params &params,
                               std::int64_t &affected) override;

        // 生成键 / 自增 ID：靠 SQL 自带的 RETURNING 直出（不自动补 RETURNING）。
        common::Status execute(const std::string &sql, std::int64_t &affected,
                               common::GeneratedKeys &out) override;
        common::Status execute(const std::string &sql, const common::Params &params,
                               std::int64_t &affected, common::GeneratedKeys &out) override;

        // 预编译语句复用（连接级命名预备语句，prepare-once / execute-many）。
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

        common::Status executeBatch(const std::string &sql,
                                    const common::ParamBatch &batch,
                                    common::BatchResult &out) override;

        // 服务端游标：DECLARE CURSOR + FETCH FORWARD n + CLOSE，必须在事务内。
        // 调用方未开事务且 auto_transaction=true 时自建事务兜底；事务由游标
        // 生命周期托管（close/析构时提交），调用方无需手动提交。
        common::Status openCursor(const std::string &sql, const common::Params &params,
                                 const core::CursorOptions &opts,
                                 std::unique_ptr<core::ICursor> &out) override;

        // bytea 在 PostgreSQL 的文本格式下是 \xHHHH，与标准 SQL 的 X'..' 不同。
        std::string escapeLiteral(const common::Value &v) const override;

        bool supportsParams() const override {
#ifdef DBMW_ENABLE_POSTGRES
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

        // 与 executeBatch / begin 的自建事务（PgTx）判定保持一致。
        [[nodiscard]] bool inTransaction() const override {
#ifdef DBMW_ENABLE_POSTGRES
            return tx_ != nullptr;
#else
            return false;
#endif
        }

        common::Status cancel() override;

    private:
        common::Status lastError(const char *where) const;

        // PgCursor 直接借用 conn_/tx_，需访问私有成员，故设为友元。
        friend class PgCursor;

        bool open_ = false;
        config::DataSourceConfig cfg_;
        // 预编译语句连接级缓存：key = SQL + 参数类型签名。PG 命名预备语句名曰
        // 存于 preparedNames_（id -> 名称），句柄本身是不透明的；连接归还池后缓存
        // 保留，随连接关闭（closeAllPrepared -> conn_->unprepare）释放。
        std::unordered_map<std::string, core::PreparedStatementHandle> preparedCache_;
        std::unordered_map<std::uint64_t, std::string> preparedNames_;
        std::list<std::string> preparedLru_;
        std::uint64_t preparedSeq_ = 0;
        int preparedLimit_ = 0;
        // 注意：不能放在 #ifdef 里——lastError() 在所有构建配置下都会被编译。
        std::string lastErr_;
        mutable std::mutex operationMtx_;
        bool operationActive_ = false;
#ifdef DBMW_ENABLE_POSTGRES
        std::unique_ptr<pqxx::connection> conn_;
        std::unique_ptr<PgTx> tx_; // 活跃事务：begin() 之后、commit()/rollback() 之前
#endif
    };

    class PostgresDriver : public IDriver {
    public:
        const char *name() const override { return "postgres"; }

        std::unique_ptr<core::IDatabaseConnection> createConnection() override {
            return std::make_unique<PostgresConnection>();
        }
    };

    // 向 DriverRegistry 注册 "postgres" 驱动。
    void registerPostgresDriver();
} // namespace dbmw::driver


#endif // DBMW_DRIVER_POSTGRES_DRIVER_H
