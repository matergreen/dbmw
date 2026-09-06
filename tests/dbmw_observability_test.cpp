// M2 追踪上下文层单测：覆盖 emitSql 自动从 ContextScope 注入 traceId/spanId
// 到 OperationEvent 与 SlowSqlRecord 的语义。
//
// 关键不变量：
//  - traceId 来自调用方栈顶（业务在请求入口从 HTTP header 解析后塞入）；
//  - spanId 优先沿用调用方栈顶的，调用方未填时在有 trace 的前提下按语句自动生成 16 hex；
//  - 没有调用方上下文时两字段都保持空串，不发"幽灵 trace/span"；
//  - 日志与慢 SQL 共享同一来源的 traceId/spanId，便于跨出口对齐。
//
// 本测试只覆盖 emitSql 直调路径（不经过任何数据库或执行器），保证 trace 注入
// 在最浅一层的语义先稳定——executeSql → attemptFn → emitSql 的端到端留给集成测试。
#include "dbmw/common/context.h"
#include "dbmw/common/observer.h"
#include "dbmw/config/datasource_config.h"

#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

using namespace dbmw::common;
using dbmw::config::ObservabilityConfig;

namespace dbmw::common {
    namespace {
        bool isLowerHex16(const std::string &s) {
            if (s.size() != 16) return false;
            for (char c : s) {
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
            }
            return true;
        }
    } // namespace
} // namespace dbmw::common

static int g_failed = 0;
static int g_passed = 0;

static void check(bool cond, const std::string &name) {
    if (cond) { ++g_passed; std::cout << "  [PASS] " << name << "\n"; }
    else { ++g_failed; std::cout << "  [FAIL] " << name << "\n"; }
}

// capture 锁：emitSql → observer 回调与 SlowSqlRecord 入队共用一把。
// 测试里全部顺序写读，不主动并发 emit，但回调路径与 emitSql 落慢 SQL 是
// 两条独立路径，用锁防止任何未来扩展时把既有断言撞坏。
static std::mutex g_capMtx;
static std::vector<OperationEvent> g_captured;

static void capturingObserver(const OperationEvent &e) {
    std::lock_guard<std::mutex> lk(g_capMtx);
    g_captured.push_back(e);
}

// 把“清空 g_captured”集中到一处，避免每个 case 重复九遍。锁的最小临界区
// 不能跨越 emitSql（emitSql 会反向回调 observer、可能拿 g_capMtx）。
#define CLEAR_CAPTURED() do { std::lock_guard<std::mutex> _lk(g_capMtx); g_captured.clear(); } while (0)

int main() {
    std::cout << "== M2 追踪上下文：emitSql 无 ctx 时不发幽灵字段 ==\n";
    {
        CLEAR_CAPTURED();
        Observability::clearSlowSqlStats();
        Observability::setObserver(&capturingObserver);
        dbmw::config::ObservabilityConfig cfg; // 默认：slow_sql/sql_log 都关
        Observability::configure(cfg);

        OperationEvent e;
        e.dataSource = "ds-noctx";
        e.type = OperationType::Query;
        e.status = Status::OK();
        e.duration = std::chrono::microseconds(500);
        Observability::emitSql(e, "SELECT 1");

        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "无 ctx 时观察者仍触发一次");
        check(g_captured[0].traceId.empty(),
              "无 ctx 时 event.traceId 不被自动生成（不发幽灵 trace）");
        check(g_captured[0].spanId.empty(),
              "无 ctx 时 event.spanId 不被自动生成（不发幽灵 span）");
    }

    std::cout << "== M2 追踪上下文：emitSql 注入 trace + span ==\n";
    {
        CLEAR_CAPTURED();
        Observability::clearSlowSqlStats();

        SqlContext ctx;
        ctx.traceId = std::string(32, 'a'); // 32 hex
        ctx.spanId = std::string(16, 'b');
        ContextScope scope(ctx);

        OperationEvent e;
        e.dataSource = "ds-trace";
        e.type = OperationType::Execute;
        e.status = Status::OK();
        e.duration = std::chrono::microseconds(1200);
        Observability::emitSql(e, "UPDATE t SET v=1");

        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "emitSql 在 ctx 下正常进入观察者");
        check(g_captured[0].traceId == ctx.traceId,
              "event.traceId 与栈顶 traceId 一致");
        check(g_captured[0].spanId == ctx.spanId,
              "调用方已填 spanId 时沿用，不自动改写");
    }

    std::cout << "== M2 追踪上下文：trace 单独、span 自动生成 ==\n";
    {
        CLEAR_CAPTURED();
        Observability::clearSlowSqlStats();

        SqlContext ctx;
        ctx.traceId = std::string(32, 'c');
        // spanId 故意不填：emitSql 应在有 trace 的前提下，按语句生成 16 hex 子跨度。
        ContextScope scope(ctx);

        OperationEvent e;
        e.dataSource = "ds-autospan";
        e.status = Status::OK();
        e.duration = std::chrono::microseconds(800);
        Observability::emitSql(e, "SELECT 1 FROM a");

        std::string firstSpan;
        {
            std::lock_guard<std::mutex> lk(g_capMtx);
            check(g_captured.size() == 1, "trace-only 路径仍触发观察者");
            check(g_captured[0].traceId == ctx.traceId,
                  "trace-only 路径 event.traceId 等于栈顶");
            check(isLowerHex16(g_captured[0].spanId),
                  "trace-only 路径自动生成 16 hex spanId");
            firstSpan = g_captured[0].spanId;
        } // ← 必须先释放锁，才能让第二轮 emitSql 的 observer 回调进来。

        // 同一语句第二次应得到不同的 spanId（按语句独立编号）。
        Observability::emitSql(e, "SELECT 1 FROM a");
        std::lock_guard<std::mutex> lk2(g_capMtx);
        check(g_captured.size() == 2,
              "第二次 emitSql 也进观察者");
        check(g_captured[1].spanId != firstSpan && isLowerHex16(g_captured[1].spanId),
              "两次 emitSql 的 spanId 互不相同且都是 16 hex");
    }

    std::cout << "== M2 追踪上下文：嵌套 ContextScope 取栈顶 ==\n";
    {
        CLEAR_CAPTURED();
        Observability::clearSlowSqlStats();

        SqlContext outer;
        outer.traceId = std::string(32, 'o');
        outer.spanId = std::string(16, 'O');
        ContextScope sOuter(outer);

        SqlContext inner;
        inner.traceId = std::string(32, 'i');
        inner.spanId = std::string(16, 'I');
        ContextScope sInner(inner);

        OperationEvent e;
        e.dataSource = "ds-nested";
        e.status = Status::OK();
        e.duration = std::chrono::microseconds(700);
        Observability::emitSql(e, "SELECT * FROM t");

        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "嵌套路径 emitSql 仍进观察者");
        check(g_captured[0].traceId == inner.traceId,
              "嵌套 emitSql 取栈顶（inner）的 traceId");
        check(g_captured[0].spanId == inner.spanId,
              "嵌套 emitSql 取栈顶（inner）的 spanId");
    }

    std::cout << "== M2 追踪上下文：SlowSqlRecord 继承 trace 字段 ==\n";
    {
        CLEAR_CAPTURED();
        Observability::clearSlowSqlStats();

        // 慢 SQL：threshold 设为 1ms，发出去的 duration 都触发 slow。
        dbmw::config::ObservabilityConfig cfg;
        cfg.slow_sql.enabled = true;
        cfg.slow_sql.threshold_ms = 1;
        cfg.slow_sql.recent_capacity = 10;
        Observability::configure(cfg);

        SqlContext ctx;
        ctx.traceId = std::string(32, 's');
        ctx.spanId = std::string(16, 'S');
        ContextScope scope(ctx);

        OperationEvent e;
        e.dataSource = "ds-slow";
        e.type = OperationType::Query;
        e.status = Status::OK();
        e.duration = std::chrono::milliseconds(20);
        Observability::emitSql(e, "SELECT COUNT(*) FROM bigtable");

        const auto recent = Observability::recentSlowSql(10);
        check(recent.size() == 1, "慢 SQL 窗口收到一条");
        if (!recent.empty()) {
            check(recent[0].traceId == ctx.traceId,
                  "SlowSqlRecord.traceId 与 event.traceId 同源");
            check(recent[0].spanId == ctx.spanId,
                  "SlowSqlRecord.spanId 与 event.spanId 同源");
            check(recent[0].dataSource == "ds-slow",
                  "SlowSqlRecord.dataSource 沿用事件值（回归保护）");
        }
    }

    std::cout << "== M2 追踪上下文：Observability::emit（非 SQL 路径）保留空 trace ==\n";
    {
        CLEAR_CAPTURED();
        // Observability::emit（非 emitSql）不接触 ctx：保持 trace 字段空，避免
        // 给那些没有 SQL 语义的操作（如 Begin/Rollback）注入来源不明的 trace。
        Observability::setObserver(&capturingObserver);
        OperationEvent plain;
        plain.dataSource = "ds-emit";
        plain.type = OperationType::Begin;
        plain.status = Status::OK();
        plain.duration = std::chrono::microseconds(100);
        Observability::emit(plain);

        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "emit 单独路径触发观察者");
        check(g_captured[0].traceId.empty() && g_captured[0].spanId.empty(),
              "emit（非 emitSql）不留 trace 字段，归属清晰");
    }

    std::cout << "\n----------------------------------------\n";
    std::cout << "通过 " << g_passed << " 项，失败 " << g_failed << " 项\n";

    // 收尾：清状态，避免污染其他测试或后续运行
    Observability::setObserver({});
    Observability::configure({});
    Observability::clearSlowSqlStats();
    return g_failed == 0 ? 0 : 1;
}
