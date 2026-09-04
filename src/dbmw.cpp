#include "dbmw/dbmw.h"
#include "dbmw/async/dbmw_async.h"
#include "dbmw/config/config_loader.h"

#include <chrono>
#include <memory>
#include <string>
#include <utility>

namespace dbmw {
    namespace {
        core::DatabaseManager &mgr() {
            static core::DatabaseManager m;
            return m;
        }

        common::Status notFound(const std::string &name) {
            return common::Status::error(common::ErrorCode::ConfigError,
                                         "datasource not found: " + name);
        }

        common::Status noDefault() {
            return common::Status::error(common::ErrorCode::ConfigError,
                                         "no default datasource");
        }

        // 解析目标数据源：name 为空取默认，否则按名查找。
        common::Status resolve(const std::string &name, std::shared_ptr<core::DataSource> &out) {
            out = name.empty() ? mgr().getDefault() : mgr().getDataSource(name);
            if (out) return common::Status::OK();
            return name.empty() ? noDefault() : notFound(name);
        }
    }

    common::Status DBMW::init(const std::string &configPath) {
        config::GlobalConfig cfg;
        std::string err;
        if (!config::ConfigLoader::loadFromFile(configPath, cfg, err)) {
            return common::Status::error(common::ErrorCode::ConfigError, err);
        }
        const auto st = mgr().init(cfg);
        // 异步执行器在核心初始化成功后接线（v0.2.0 §9.1）。
        // 放在 dbmw.cpp 而非 DatabaseManager::init：core 不反向依赖 async
        // （与 ConnectionPool::AsyncIo 的设计一致）。线程惰性启动，
        // 未使用异步的进程零额外线程。
        if (st.ok()) async::detail::initEngine(cfg.async);
        return st;
    }

    common::Status DBMW::reload(const std::string &configPath,
                                const std::chrono::milliseconds grace) {
        config::GlobalConfig cfg;
        std::string error;
        if (!config::ConfigLoader::loadFromFile(configPath, cfg, error)) {
            return common::Status::error(common::ErrorCode::ConfigError, error);
        }
        const auto st = mgr().init(cfg, grace);
        // 热加载不重建线程池（initEngine 内部保留既有执行器）；
        // 只更新全局默认语句期限。
        if (st.ok()) async::detail::initEngine(cfg.async);
        return st;
    }

    common::Status DBMW::query(const std::string &sql, common::ResultSet &out) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(std::string(), ds); !st.ok()) return st;
        return ds->query(sql, out);
    }

    common::Status DBMW::execute(const std::string &sql, std::int64_t &affected) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(std::string(), ds); !st.ok()) return st;
        return ds->execute(sql, affected);
    }

    common::Status DBMW::query(const std::string &dataSource, const std::string &sql, common::ResultSet &out) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(dataSource, ds); !st.ok()) return st;
        return ds->query(sql, out);
    }

    common::Status DBMW::execute(const std::string &dataSource, const std::string &sql, std::int64_t &affected) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(dataSource, ds); !st.ok()) return st;
        return ds->execute(sql, affected);
    }

    common::Status DBMW::query(const std::string &sql, const common::Params &params, common::ResultSet &out) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(std::string(), ds); !st.ok()) return st;
        return ds->query(sql, params, out);
    }

    common::Status DBMW::query(const std::string &dataSource, const std::string &sql, const common::Params &params, common::ResultSet &out) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(dataSource, ds); !st.ok()) return st;
        return ds->query(sql, params, out);
    }

    common::Status DBMW::execute(const std::string &sql, const common::Params &params, std::int64_t &affected) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(std::string(), ds); !st.ok()) return st;
        return ds->execute(sql, params, affected);
    }

    common::Status DBMW::execute(const std::string &dataSource, const std::string &sql, const common::Params &params, std::int64_t &affected) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(dataSource, ds); !st.ok()) return st;
        return ds->execute(sql, params, affected);
    }

    common::Status DBMW::queryEach(const std::string &sql, const common::Params &params,
                                   const common::RowCallback &callback, std::uint64_t &rows) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(std::string(), ds); !st.ok()) return st;
        return ds->queryEach(sql, params, callback, rows);
    }

    common::Status DBMW::queryEach(const std::string &dataSource, const std::string &sql,
                                   const common::Params &params,
                                   const common::RowCallback &callback, std::uint64_t &rows) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(dataSource, ds); !st.ok()) return st;
        return ds->queryEach(sql, params, callback, rows);
    }

    common::Status DBMW::executeBatch(const std::string &sql,
                                      const common::ParamBatch &batch,
                                      common::BatchResult &out) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(std::string(), ds); !st.ok()) return st;
        return ds->executeBatch(sql, batch, out);
    }

    common::Status DBMW::executeBatch(const std::string &dataSource,
                                      const std::string &sql,
                                      const common::ParamBatch &batch,
                                      common::BatchResult &out) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(dataSource, ds); !st.ok()) return st;
        return ds->executeBatch(sql, batch, out);
    }

    common::Status DBMW::openCursor(const std::string &sql, const common::Params &params,
                                    const core::CursorOptions &opts,
                                    std::unique_ptr<core::Cursor> &out) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(std::string(), ds); !st.ok()) return st;
        return ds->openCursor(sql, params, opts, out);
    }

    common::Status DBMW::openCursor(const std::string &dataSource, const std::string &sql,
                                    const common::Params &params,
                                    const core::CursorOptions &opts,
                                    std::unique_ptr<core::Cursor> &out) {
        std::shared_ptr<core::DataSource> ds;
        if (const auto st = resolve(dataSource, ds); !st.ok()) return st;
        return ds->openCursor(sql, params, opts, out);
    }

    common::Status DBMW::transaction(const core::SessionFn &fn) {
        const auto ds = mgr().getDefault();
        if (!ds) return noDefault();
        return ds->transaction(fn);
    }

    common::Status DBMW::transaction(const std::string &dataSource, const core::SessionFn &fn) {
        const auto ds = mgr().getDataSource(dataSource);
        if (!ds) return notFound(dataSource);
        return ds->transaction(fn);
    }

    common::Status DBMW::transaction(const common::TransactionOptions &options,
                                     const core::SessionFn &fn) {
        const auto ds = mgr().getDefault();
        if (!ds) return noDefault();
        return ds->transaction(options, fn);
    }

    common::Status DBMW::transaction(const std::string &dataSource,
                                     const common::TransactionOptions &options,
                                     const core::SessionFn &fn) {
        const auto ds = mgr().getDataSource(dataSource);
        if (!ds) return notFound(dataSource);
        return ds->transaction(options, fn);
    }

    common::Status DBMW::withSession(const core::SessionFn &fn) {
        const auto ds = mgr().getDefault();
        if (!ds) return noDefault();
        return ds->withSession(fn);
    }

    common::Status DBMW::withSession(const std::string &dataSource, const core::SessionFn &fn) {
        const auto ds = mgr().getDataSource(dataSource);
        if (!ds) return notFound(dataSource);
        return ds->withSession(fn);
    }

    std::shared_ptr<core::DataSource> DBMW::dataSource(const std::string &name) {
        return name.empty() ? mgr().getDefault() : mgr().getDataSource(name);
    }

    bool DBMW::poolStats(core::ConnectionPool::Stats &out, const std::string &name) {
        const auto source = dataSource(name);
        return source && source->poolStats(out);
    }

    std::vector<core::NamedPoolStats> DBMW::allPoolStats() {
        return mgr().allPoolStats();
    }

    std::vector<common::SlowSqlStats> DBMW::slowSqlStats(
        const std::size_t limit, const std::string &dataSource) {
        return common::Observability::slowSqlStats(limit, dataSource);
    }

    std::vector<common::SlowSqlRecord> DBMW::recentSlowSql(
        const std::size_t limit, const std::string &dataSource) {
        return common::Observability::recentSlowSql(limit, dataSource);
    }

    void DBMW::clearSlowSqlStats() {
        common::Observability::clearSlowSqlStats();
    }

    void DBMW::shutdown(const std::chrono::milliseconds grace) {
        // 异步排水必须在池关闭之前（v0.2.0 §9.3，顺序是正确性问题）：
        //   1) 拒绝新异步操作（PoolClosed）
        //   2) 等在途操作归零
        //   3) 停完成调度器（已投递的回调排空）
        //   4) 停主执行器（worker 排空任务）
        // 在途操作归还连接前池必须活着，所以池的关闭仍在 mgr().shutdown 里。
        async::detail::drainAndStop(grace);
        mgr().shutdown(grace);
    }

    void DBMW::setObserver(common::OperationObserver observer) {
        common::Observability::setObserver(std::move(observer));
    }
} // namespace dbmw
