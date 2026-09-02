#include "dbmw/dbmw.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// =============================================================================
// dbmw 功能点全覆盖示例
//
// 本程序按"一次业务调用"的视角依次演示 dbmw 暴露的全部公开能力：
//   1. 初始化 / 热加载 / 关闭
//   2. 单条查询 query（默认 / 指定数据源 / 指定 group）
//   3. 参数化查询（原生绑定，'?' 占位，值不拼进 SQL 文本）
//   4. 单条执行 execute（拿受影响行数）
//   5. 流式消费 queryEach（逐行回调，避免一次性物化大结果集）
//   6. 批量执行 executeBatch（ParamBatch / BatchResult）
//   7. 游标 openCursor（钉连接、分批 fetch、显式 close）
//   8. 事务 transaction（默认 / 指定数据源 / 带 TransactionOptions）
//   9. 会话 withSession（同连接多语句 + 手动 begin/savepoint/rollback/commit）
//  10. 数据源句柄 dataSource() 与连接池统计 poolStats / allPoolStats
//  11. 慢 SQL 统计 slowSqlStats / recentSlowSql / clearSlowSqlStats
//  12. 操作观察器 setObserver（事件不含 SQL 与参数，默认不泄漏业务数据）
//  13. 值辅助函数 valueToString / timestampToString 等
//
// 配置驱动（无需在 C++ 里写代码，改 config/datasources.json 即可）的能力：
//   心跳保活 heartbeat / 连接池 pool / 重试 retry / 熔断 circuit_breaker /
//   限流 rate_limit / SQL 审计拦截 sql_audit / 查询缓存 query_cache /
//   主库故障转移 + 写缓冲 failover.write_buffer / 读写分离 group（读副本权重轮询、
//   read_after_write 读后写、read_only 只读保护）。详见示例配置与类型定义
//   include/dbmw/config/datasource_config.h。
//
// 注意：默认（对应驱动未在编译期启用 DBMW_ENABLE_*）时，query/execute/事务/游标
// 会返回 DriverDisabled / NotSupported，这是预期行为——开启驱动并安装客户端库、
// 启动数据库后即可真正跑通。本示例对每个调用都按状态打印 [OK]/[NOTE]，便于你
// 在"驱动已启用"与"驱动未启用"两种环境下都能直接编译运行。
// =============================================================================

namespace {
    // 统一打印一次操作的结果（成功标 [OK]，其余标 [NOTE] 并附错误码）。
    void report(const char *tag, const dbmw::common::Status &st) {
        std::cout << (st.ok() ? "[OK]   " : "[NOTE] ")
                << tag << ": " << st.message
                << " (code=" << dbmw::common::errorCodeToString(st.code) << ")" << std::endl;
    }

    // OperationType -> 可读名（头文件未导出 operationName，示例内自维护一份）。
    const char *opName(dbmw::common::OperationType t) {
        using T = dbmw::common::OperationType;
        switch (t) {
            case T::Query: return "query";
            case T::Execute: return "execute";
            case T::Begin: return "begin";
            case T::Commit: return "commit";
            case T::Rollback: return "rollback";
            case T::Cancel: return "cancel";
            case T::Stream: return "stream";
            case T::Batch: return "batch";
            case T::Savepoint: return "savepoint";
            case T::Select: return "select(cursor)";
            default: return "unknown";
        }
    }
} // namespace

int main(int argc, char **argv) {
    // 配置路径：优先命令行参数，否则用工程根目录的示例配置。
    std::string configPath = (argc > 1) ? argv[1] : "config/datasources.json.example";

    // -------------------------------------------------------------------------
    // 1) 初始化：解析配置 + 建连接池 + 启心跳。任意一步失败都不会留下半初始化状态。
    // -------------------------------------------------------------------------
    auto st = dbmw::DBMW::init(configPath);
    if (!st.ok()) {
        std::cerr << "[FAIL] init: " << st.message << std::endl;
        return 1;
    }
    std::cout << "[OK]   init from " << configPath << std::endl;

    // 12) 注册进程级操作观察器（在真正发请求前注册，才能捕获到事件）。
    //     回调里只打印失败/慢请求，避免刷屏；异常会被中间件吞掉。
    dbmw::DBMW::setObserver([](const dbmw::common::OperationEvent &ev) {
        if (!ev.status.ok() || ev.slow) {
            std::cout << "  [OBS] " << ev.dataSource << " " << opName(ev.type)
                    << " dur=" << ev.duration.count() << "us"
                    << " ok=" << ev.status.ok()
                    << (ev.slow ? " SLOW" : "") << std::endl;
        }
    });

    // -------------------------------------------------------------------------
    // 2) 单条查询：默认数据源 / 指定数据源 / 指定 group（读写分离组，读路由到副本）
    // -------------------------------------------------------------------------
    dbmw::common::ResultSet rs;
    report("query(default)", dbmw::DBMW::query("SELECT 1", rs));

    // 指定名为 "pg" 的数据源。
    dbmw::common::ResultSet rsPg;
    report("query(pg)", dbmw::DBMW::query("pg", "SELECT 1", rsPg));

    // 指定名为 "app" 的 group：读按权重轮询副本，体现读写分离；
    // 该组配置了 read_after_write_ms，写后短时间内读会落到主库保证读到刚写的数据。
    dbmw::common::ResultSet rsGroup;
    report("query(group=app)", dbmw::DBMW::query("app", "SELECT 1", rsGroup));

    // -------------------------------------------------------------------------
    // 3) 参数化查询（SQL 中用 '?' 占位，值不会拼进 SQL 文本；内置驱动走原生绑定）。
    // -------------------------------------------------------------------------
    dbmw::common::Params params;
    params.emplace_back(std::string("O'Brien")); // 单引号会被正确转义/绑定
    params.emplace_back(static_cast<std::int64_t>(42));
    dbmw::common::ResultSet prs;
    report("query(params)", dbmw::DBMW::query("SELECT id, name FROM users WHERE name = ? AND age > ?", params, prs));

    // -------------------------------------------------------------------------
    // 4) 单条执行：拿受影响行数。
    // -------------------------------------------------------------------------
    std::int64_t affected = 0;
    report("execute", dbmw::DBMW::execute("UPDATE accounts SET balance = balance - 100 WHERE id = ?",
               dbmw::common::Params{static_cast<std::int64_t>(1)}, affected));
    std::cout << "        affected=" << affected << std::endl;

    // -------------------------------------------------------------------------
    // 5) 流式消费 queryEach：逐行回调，适合大结果集（不一次性物化到内存）。
    //    回调返回 false 可提前停止遍历（类似 LIMIT / 断点续传）。
    // -------------------------------------------------------------------------
    std::uint64_t streamed = 0;
    report("queryEach",
           dbmw::DBMW::queryEach(
               "SELECT id, name FROM users WHERE age > ?",
               dbmw::common::Params{static_cast<std::int64_t>(0)},
               [&streamed](const dbmw::common::Row &row) {
                   ++streamed;
                   std::cout << "        row#" << streamed << " id=" << dbmw::common::valueToString(row.at("id")) << std::endl;
                   return true; // 返回 false 即停止
               },
               streamed));

    // -------------------------------------------------------------------------
    // 6) 批量执行 executeBatch：同一模板 + 多组参数，一次下发。
    //    BatchResult.affected 逐条记录受影响行数；totalAffected() 汇总。
    // -------------------------------------------------------------------------
    dbmw::common::ParamBatch batch;
    batch.push_back(dbmw::common::Params{std::string("alice"), static_cast<std::int64_t>(20)});
    batch.push_back(dbmw::common::Params{std::string("bob"), static_cast<std::int64_t>(25)});
    dbmw::common::BatchResult bres;
    report("executeBatch",
           dbmw::DBMW::executeBatch(
               "INSERT INTO users(name, age) VALUES (?, ?)", batch, bres));
    std::cout << "        totalAffected=" << bres.totalAffected() << std::endl;

    // -------------------------------------------------------------------------
    // 7) 游标 openCursor：按读路由借连接并钉住，多次 fetch 分批取，用完显式 close。
    //    不缓存、不审计为"整结果集"语义；游标必须 close() 或随 unique_ptr 析构，
    //    否则连接被长期钉住（池会告警 / 受 max_open_cursors 限制）。
    // -------------------------------------------------------------------------
    {
        std::unique_ptr<dbmw::core::Cursor> cur;
        dbmw::core::CursorOptions opts;
        opts.batch_size = 100; // 每次预取行数
        opts.auto_transaction = true; // PG 必须 true（游标活在事务里）
        auto co = dbmw::DBMW::openCursor(
            "SELECT id, name FROM users WHERE age > ?",
            dbmw::common::Params{static_cast<std::int64_t>(0)}, opts, cur);
        report("openCursor", co);
        if (co.ok() && cur) {
            std::uint64_t n = 0;
            dbmw::common::Row row;
            bool ok = false;
            // fetchRow 在无更多行时 ok=false（正常 EOF，状态为 CursorClosed 非错误）。
            while (true) {
                const auto fs = cur->fetchRow(row, ok);
                if (!ok) break; // 正常到末尾
                ++n;
                std::cout << "        cursor row#" << n << " name="
                        << dbmw::common::valueToString(row.at("name")) << std::endl;
            }
            std::cout << "        cursor rowsFetched=" << cur->rowsFetched()
                    << " isOpen=" << cur->isOpen() << std::endl;
            report("cursor.close", cur->close());
        }
    }

    // -------------------------------------------------------------------------
    // 8) 事务 transaction：多条语句固定在同一条连接，fn 返回失败或抛异常则回滚。
    // -------------------------------------------------------------------------
    report("transaction",
           dbmw::DBMW::transaction([](const dbmw::core::Session &s) {
               std::int64_t n = 0;
               if (auto r = s.execute(
                   "UPDATE accounts SET balance = balance - 100 WHERE id = ?",
                   dbmw::common::Params{static_cast<std::int64_t>(1)}, n); !r.ok())
                   return r; // 失败 -> 自动 rollback
               return s.execute(
                   "UPDATE accounts SET balance = balance + 100 WHERE id = ?",
                   dbmw::common::Params{static_cast<std::int64_t>(2)}, n);
           }));

    // 带选项的版本：隔离级别 / 只读 / 整个事务回调的期限（到期尽力取消并回滚）。
    dbmw::common::TransactionOptions txOpts;
    txOpts.isolation = dbmw::common::IsolationLevel::Serializable;
    txOpts.readOnly = false;
    txOpts.timeout = std::chrono::milliseconds(5000);
    report("transaction(opts)",
           dbmw::DBMW::transaction(txOpts, [](const dbmw::core::Session &s) {
               std::int64_t n = 0;
               return s.execute("UPDATE accounts SET balance = balance + 1 WHERE id = ?",
                                dbmw::common::Params{static_cast<std::int64_t>(1)}, n);
           }));

    // 在指定 group 上开事务：写路由到主库（按 failover.primaries 顺序故障转移）。
    report("transaction(group=app)",
           dbmw::DBMW::transaction("app", [](const dbmw::core::Session &s) {
               std::int64_t n = 0;
               return s.execute("INSERT INTO t(k, v) VALUES (?, ?)",
                                dbmw::common::Params{std::string("k"), static_cast<std::int64_t>(1)}, n);
           }));

    // -------------------------------------------------------------------------
    // 9) 会话 withSession：在一条独占连接上连续执行多条语句（临时表、会话变量等），
    //    并手动演示 begin / savepoint / rollbackToSavepoint / commit 等事务原语。
    // -------------------------------------------------------------------------
    report("withSession",
           dbmw::DBMW::withSession("pg", [](dbmw::core::Session &s) {
               if (auto r = s.begin(); !r.ok()) return r;
               std::int64_t n = 0;
               if (auto r = s.execute("INSERT INTO t(k,v) VALUES (?,?)",
                                      dbmw::common::Params{std::string("a"), static_cast<std::int64_t>(1)}, n);
                   !r.ok())
                   return r;
               if (auto r = s.savepoint("sp1"); !r.ok()) return r; // 打保存点
               if (auto r = s.execute("INSERT INTO t(k,v) VALUES (?,?)",
                                      dbmw::common::Params{std::string("b"), static_cast<std::int64_t>(2)}, n);
                   !r.ok())
                   return r;
               if (auto r = s.rollbackToSavepoint("sp1"); !r.ok()) return r; // 回退到 sp1
               std::cout << "        inTransaction=" << s.inTransaction()
                       << " didWrite=" << s.didWrite() << std::endl;
               return s.commit(); // 仅提交到 sp1 之前的部分
           }));

    // -------------------------------------------------------------------------
    // 10) 数据源句柄与连接池统计：可拿到每个物理池的借出/空闲/等待等指标。
    // -------------------------------------------------------------------------
    if (auto ds = dbmw::DBMW::dataSource("main")) {
        std::cout << "[OK]   dataSource(name=" << ds->name() << ")" << std::endl;
        dbmw::core::ConnectionPool::Stats s;
        if (ds->poolStats(s)) {
            std::cout << "        pool: total=" << s.total << " idle=" << s.idle
                    << " borrowed=" << s.borrowed << " waiting=" << s.waiting
                    << " util=" << s.utilization() << std::endl;
        }
    } else {
        std::cout << "[NOTE] dataSource(\"main\") not found" << std::endl;
    }

    dbmw::core::ConnectionPool::Stats def;
    if (dbmw::DBMW::poolStats(def)) {
        std::cout << "[OK]   default poolStats: total=" << def.total
                << " borrowed=" << def.borrowed << std::endl;
    }
    for (const auto &np: dbmw::DBMW::allPoolStats()) {
        std::cout << "        allPoolStats: " << np.dataSource
                << " total=" << np.stats.total
                << " borrowed=" << np.stats.borrowed << std::endl;
    }

    // -------------------------------------------------------------------------
    // 11) 慢 SQL 统计：按平均耗时倒序（slowSqlStats）与按最近发生倒序（recentSlowSql）。
    //     注意：默认配置 slow_sql.enabled=true，这里只是展示如何读取与清理。
    // -------------------------------------------------------------------------
    for (const auto &s: dbmw::DBMW::slowSqlStats(/*limit=*/5)) {
        std::cout << "        slowSql[" << s.dataSource << "] "
                << s.sqlTemplate.substr(0, 60) << " count=" << s.count
                << " max=" << s.maxDuration.count() << "us" << std::endl;
    }
    for (const auto &r: dbmw::DBMW::recentSlowSql(/*limit=*/5)) {
        std::cout << "        recentSlow[" << r.dataSource << "] dur="
                << r.duration.count() << "us code="
                << dbmw::common::errorCodeToString(r.errorCode) << std::endl;
    }
    dbmw::DBMW::clearSlowSqlStats(); // 清空慢 SQL 聚合（示例收尾用）

    // -------------------------------------------------------------------------
    // 13) 值辅助函数：不依赖驱动地把 Value / Timestamp 转字符串或解析时间戳。
    // -------------------------------------------------------------------------
    dbmw::common::Value v = std::string("hello");
    std::cout << "[OK]   valueToString(\"hello\") = "
            << dbmw::common::valueToString(v) << std::endl;
    dbmw::common::Timestamp ts;
    if (dbmw::common::tryParseTimestamp("2026-09-02 16:30:00", ts)) {
        std::cout << "[OK]   timestampToString = "
                << dbmw::common::timestampToString(ts) << std::endl;
    }

    // 防注入辅助：标识符加引号翻倍；字面量转义（供不支持服务端绑定的驱动插值时兜底）。
    std::cout << "[OK]   quoteIdentifier(\"order\") = "
            << dbmw::common::quoteIdentifier("order") << std::endl;
    std::cout << "[OK]   escapeLiteralGeneric(\"a'b\") = "
            << dbmw::common::escapeLiteralGeneric(std::string("a'b")) << std::endl;

    // -------------------------------------------------------------------------
    // 1b) 热加载 reload：先完整创建新数据源，再切换并排空旧连接池（grace 宽限）。
    //     用于改了配置不重启进程的场景；其余功能调用方式与 init 后完全一致。
    // -------------------------------------------------------------------------
    report("reload", dbmw::DBMW::reload(configPath));

    // -------------------------------------------------------------------------
    // 1c) 关闭：释放所有连接池与心跳线程。宽限期内等待借用中连接归还，
    //     超时未归还的连接会被强制关闭。务必在进程退出前调用。
    // -------------------------------------------------------------------------
    dbmw::DBMW::shutdown();
    std::cout << "[OK]   shutdown" << std::endl;
    return 0;
}
