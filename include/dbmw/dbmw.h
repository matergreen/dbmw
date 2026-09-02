#ifndef DBMW_DBMW_H
#define DBMW_DBMW_H

#include "dbmw/common/types.h"
#include "dbmw/common/observer.h"
#include "dbmw/core/database_manager.h"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace dbmw
{
    // 顶层门面（Facade）：应用只需包含本头文件即可使用数据库连接中间件。
    //
    // 典型用法：
    //   dbmw::DBMW::init("config/datasources.json");
    //   dbmw::common::ResultSet rs;
    //   auto st = dbmw::DBMW::query("SELECT 1", rs);
    //   ...
    //   dbmw::DBMW::shutdown();
    //
    // 事务用法（多条语句固定在同一条连接上，失败自动回滚）：
    //   auto st = dbmw::DBMW::transaction([](dbmw::core::Session& s) {
    //       int64_t n = 0;
    //       if (auto r = s.execute("UPDATE a SET v = v - 1 WHERE id = 1", n); !r.ok()) return r;
    //       return s.execute("UPDATE b SET v = v + 1 WHERE id = 2", n);
    //   });
    class DBMW
    {
    public:
        DBMW() = delete;

        // 从 JSON 配置文件初始化（解析 + 建池 + 启动心跳）。
        static common::Status init(const std::string& configPath);

        // 原子热加载：先完整创建新数据源，再切换并排空旧连接池。
        static common::Status reload(
            const std::string &configPath,
            std::chrono::milliseconds grace = std::chrono::milliseconds(5000));

        // 使用默认数据源的便捷查询/执行。
        static common::Status query(const std::string& sql, common::ResultSet& out);

        static common::Status execute(const std::string& sql, std::int64_t& affected);

        // 指定数据源名称的查询/执行。
        static common::Status query(const std::string& dataSource, const std::string& sql, common::ResultSet& out);

        static common::Status execute(const std::string& dataSource, const std::string& sql, std::int64_t& affected);

        // 带绑定参数的查询/执行（SQL 中用 '?' 占位）。
        // 内置驱动走原生绑定；未实现绑定的自定义驱动返回 NotSupported。
        static common::Status query(const std::string& sql, const common::Params& params, common::ResultSet& out);

        static common::Status query(const std::string& dataSource, const std::string& sql, const common::Params& params, common::ResultSet& out);

        static common::Status execute(const std::string& sql, const common::Params& params, std::int64_t& affected);

        static common::Status execute(const std::string& dataSource, const std::string& sql, const common::Params& params, std::int64_t& affected);

        static common::Status queryEach(const std::string &sql,
                                        const common::Params &params,
                                        const common::RowCallback &callback,
                                        std::uint64_t &rows);

        static common::Status queryEach(const std::string &dataSource,
                                        const std::string &sql,
                                        const common::Params &params,
                                        const common::RowCallback &callback,
                                        std::uint64_t &rows);

        static common::Status executeBatch(const std::string &sql,
                                           const common::ParamBatch &batch,
                                           common::BatchResult &out);

        static common::Status executeBatch(const std::string &dataSource,
                                           const std::string &sql,
                                           const common::ParamBatch &batch,
                                           common::BatchResult &out);

        // 打开游标：按读路由借连接并钉住，返回可多次 fetch 的游标。
        // 游标必须显式 close() 或随 unique_ptr 析构，否则连接被长期钉住（池会告警）。
        // 不缓存、不审计为“整结果集”语义；游标期间断连不自动重试（状态已丢失）。
        static common::Status openCursor(const std::string &sql, const common::Params &params,
                                        const core::CursorOptions &opts,
                                        std::unique_ptr<core::Cursor> &out);

        // 在指定数据源上打开游标。
        static common::Status openCursor(const std::string &dataSource, const std::string &sql,
                                        const common::Params &params,
                                        const core::CursorOptions &opts,
                                        std::unique_ptr<core::Cursor> &out);

        // 在默认数据源上开启事务执行 fn：成功提交，失败或抛异常则回滚。
        static common::Status transaction(const core::SessionFn& fn);

        // 在指定数据源上开启事务执行 fn。
        static common::Status transaction(const std::string& dataSource, const core::SessionFn& fn);

        static common::Status transaction(const common::TransactionOptions &options,
                                          const core::SessionFn &fn);

        static common::Status transaction(const std::string &dataSource,
                                          const common::TransactionOptions &options,
                                          const core::SessionFn &fn);

        // 在一条独占连接上执行 fn（不自动开启事务）。
        static common::Status withSession(const std::string& dataSource, const core::SessionFn& fn);

        static common::Status withSession(const core::SessionFn& fn);

        // 获取数据源句柄（name 为空返回默认数据源）；返回 nullptr 表示不存在。
        static std::shared_ptr<core::DataSource> dataSource(const std::string& name = "");

        static bool poolStats(core::ConnectionPool::Stats &out,
                              const std::string &name = "");

        // 返回每个物理数据源的连接池快照；数据源组不会掩盖成员池的明细。
        static std::vector<core::NamedPoolStats> allPoolStats();

        // 慢 SQL 按平均耗时倒序返回；recentSlowSql 按最近发生时间倒序返回。
        static std::vector<common::SlowSqlStats> slowSqlStats(
            std::size_t limit = 100, const std::string &dataSource = "");
        static std::vector<common::SlowSqlRecord> recentSlowSql(
            std::size_t limit = 100, const std::string &dataSource = "");
        static void clearSlowSqlStats();

        // 释放所有连接池与心跳线程。
        // grace 为等待借用中连接归还的宽限期，超时未归还的连接会被强制关闭。
        static void shutdown(std::chrono::milliseconds grace = std::chrono::milliseconds(5000));

        // 注册进程级操作观察器（事件不包含 SQL 与参数）。
        static void setObserver(common::OperationObserver observer);
    };
} // namespace dbmw

#endif // DBMW_DBMW_H
