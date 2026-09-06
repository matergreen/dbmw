// M6/M7 观测层延展单测：OperationEvent.shadow / .transformed 在 emitSql 路径
// 上被正确填充，供监控/告警按"影子流量 vs 生产流量"、"已脱敏 vs 原值"两维度
// 分别打标签。
//
// 关键不变量：
//  - shadow：来自当前栈顶 SqlContext（同步路径由 runWithInterceptors push
//    onRoute 决策后的 view.ctx；异步路径 worker 已 push entryCtx），不传 ctx
//    时保持 false；
//  - transformed：来自 observeSql 透传的 ResultSet 指针；fn 完成后 result 已
//    被 SPI afterExecution 改写，因此 observer 看到的是真实值；非 query 类
//    没有 result 时保持 false。
//
// 设计上不打真实数据库——直接构造 OperationEvent 走 emitSql 直调路径，
// 保证影子/脱敏标记在 emitSql 这一层先稳定；observability_test 已有 trace
// 注入覆盖，本测试只补 M6/M7 两条新字段的注入。
#include "dbmw/common/context.h"
#include "dbmw/common/observer.h"
#include "dbmw/common/types.h"
#include "dbmw/config/datasource_config.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

using namespace dbmw;
using common::ContextScope;
using common::ErrorCode;
using common::OperationEvent;
using common::OperationType;
using common::Observability;
using common::ResultSet;
using common::Row;
using common::SqlContext;
using common::Status;

static int g_failed = 0;
static int g_passed = 0;

static void check(bool cond, const std::string &name) {
    if (cond) { ++g_passed; std::cout << "  [PASS] " << name << "\n"; }
    else { ++g_failed; std::cout << "  [FAIL] " << name << "\n"; }
}

// ---------------------------------------------------------------------------
// capture：与 dbmw_observability_test.cpp 不同，本测试**频繁换 observer**，
// 且每次都清空，避免不同 case 互相污染。
// ---------------------------------------------------------------------------
static std::mutex g_capMtx;
static std::vector<OperationEvent> g_captured;

static void capObserver(const OperationEvent &e) {
    std::lock_guard<std::mutex> lk(g_capMtx);
    g_captured.push_back(e);
}

static void clearCapture() {
    std::lock_guard<std::mutex> lk(g_capMtx);
    g_captured.clear();
}

// ---------------------------------------------------------------------------
// case 1：emitSql 不传 ctx 时，event.shadow / .transformed 都保持默认 false。
// ---------------------------------------------------------------------------
static void test_defaults_without_ctx_or_result() {
    std::cout << "== M9.1 默认值：空 ctx + 空 result 时 shadow/transformed=false ==\n";
    clearCapture();
    Observability::setObserver(&capObserver);
    dbmw::config::ObservabilityConfig cfg;
    Observability::configure(cfg);

    OperationEvent e;
    e.dataSource = "ds";
    e.type = OperationType::Query;
    e.status = Status::OK();
    e.duration = std::chrono::microseconds(100);
    Observability::emitSql(e, "SELECT 1");
    {
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "observer 已触发一次");
        if (!g_captured.empty()) {
            check(!g_captured[0].shadow, "默认 ctx：event.shadow=false");
            check(!g_captured[0].transformed, "默认 result=nullptr：event.transformed=false");
        }
    }
}

// ---------------------------------------------------------------------------
// case 2：栈顶 SqlContext.shadow=true → emitSql 写入 event.shadow=true。
// ---------------------------------------------------------------------------
static void test_shadow_propagates_from_ctx() {
    std::cout << "== M9.2 shadow 由栈顶 ctx 注入 ==\n";
    clearCapture();
    SqlContext ctx;
    ctx.shadow = true;
    ContextScope scope(ctx);
    OperationEvent e;
    e.dataSource = "ds";
    e.type = OperationType::Query;
    e.status = Status::OK();
    e.duration = std::chrono::microseconds(100);
    Observability::emitSql(e, "SELECT * FROM probe_health");
    {
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "shadow scope 内：observer 已触发");
        if (!g_captured.empty()) {
            check(g_captured[0].shadow,
                  "shadow=true scope：event.shadow=true（注入成功）");
        }
    }
}

// ---------------------------------------------------------------------------
// case 3：shadow 帧退出后立即恢复 false。
// ---------------------------------------------------------------------------
static void test_shadow_resets_after_scope() {
    std::cout << "== M9.3 shadow 帧退出后恢复 false ==\n";
    clearCapture();
    {
        SqlContext ctx;
        ctx.shadow = true;
        ContextScope scope(ctx);
        OperationEvent e;
        e.dataSource = "ds";
        e.type = OperationType::Query;
        e.status = Status::OK();
        e.duration = std::chrono::microseconds(50);
        Observability::emitSql(e, "SELECT 1");
    }
    OperationEvent e2;
    e2.dataSource = "ds";
    e2.type = OperationType::Query;
    e2.status = Status::OK();
    e2.duration = std::chrono::microseconds(50);
    Observability::emitSql(e2, "SELECT 2");
    {
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 2, "两条 emit 都触发 observer");
        if (g_captured.size() >= 2) {
            check(g_captured[0].shadow, "shadow 帧内：shadow=true");
            check(!g_captured[1].shadow, "shadow 帧外：shadow=false（栈帧隔离）");
        }
    }
}

// ---------------------------------------------------------------------------
// case 4：result.translated = true → emitSql 写入 event.transformed = true。
// ---------------------------------------------------------------------------
static void test_transformed_from_result() {
    std::cout << "== M9.4 transformed 由 result.transformed 注入 ==\n";
    clearCapture();
    ResultSet out;
    out.setFields({"secret"});
    Row row;
    row.set("secret", std::string("***"));
    out.addRow(std::move(row));
    // SPI 改写后会置位 transformed=true
    out.transformed = true;

    OperationEvent e;
    e.dataSource = "ds";
    e.type = OperationType::Query;
    e.status = Status::OK();
    e.duration = std::chrono::microseconds(80);
    Observability::emitSql(e, "SELECT secret FROM t", {}, &out);
    {
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "observer 已触发");
        if (!g_captured.empty()) {
            check(g_captured[0].transformed,
                  "result.transformed=true → event.transformed=true");
        }
    }
}

// ---------------------------------------------------------------------------
// case 5：result.translated = false（非脱敏）→ event.transformed = false。
// ---------------------------------------------------------------------------
static void test_not_transformed() {
    std::cout << "== M9.5 非脱敏：result.transformed=false → event.transformed=false ==\n";
    clearCapture();
    ResultSet out;
    out.setFields({"id"});
    Row row;
    row.set("id", std::int64_t(42));
    out.addRow(std::move(row));
    // out.transformed 保持默认 false

    OperationEvent e;
    e.dataSource = "ds";
    e.type = OperationType::Query;
    e.status = Status::OK();
    e.duration = std::chrono::microseconds(80);
    Observability::emitSql(e, "SELECT id FROM t", {}, &out);
    {
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "observer 已触发");
        if (!g_captured.empty()) {
            check(!g_captured[0].transformed,
                  "result.transformed=false → event.transformed=false");
        }
    }
}

// ---------------------------------------------------------------------------
// case 6：result = nullptr（写路径 / execute / batch / stream）→ event.transformed 默认 false。
// ---------------------------------------------------------------------------
static void test_write_path_keeps_transformed_false() {
    std::cout << "== M9.6 写路径无 result：transformed 默认 false ==\n";
    clearCapture();
    // 不传 result，相当于 execute/batch 的观察路径
    OperationEvent e;
    e.dataSource = "ds";
    e.type = OperationType::Execute;
    e.status = Status::OK();
    e.duration = std::chrono::microseconds(40);
    Observability::emitSql(e, "UPDATE x SET y=1");
    {
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "observer 已触发");
        if (!g_captured.empty()) {
            check(!g_captured[0].transformed, "execute 路径：event.transformed=false（写没 result）");
            check(!g_captured[0].shadow, "execute 路径：event.shadow=false（默认）");
        }
    }
}

// ---------------------------------------------------------------------------
// case 7：组合：shadow=true + transformed=true 同时生效。
// ---------------------------------------------------------------------------
static void test_shadow_and_transformed_together() {
    std::cout << "== M9.7 shadow + transformed 同一事件内并行 ==\n";
    clearCapture();
    SqlContext ctx;
    ctx.shadow = true;
    ContextScope scope(ctx);
    ResultSet out;
    out.transformed = true;
    OperationEvent e;
    e.dataSource = "shadow-ds";
    e.type = OperationType::Query;
    e.status = Status::OK();
    e.duration = std::chrono::microseconds(120);
    Observability::emitSql(e, "SELECT secret FROM probe", {}, &out);
    {
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "observer 已触发");
        if (!g_captured.empty()) {
            check(g_captured[0].shadow, "shadow=true 生效");
            check(g_captured[0].transformed, "transformed=true 生效");
        }
    }
}

// ---------------------------------------------------------------------------
// case 8：错误事件：status 非 OK 时也要把 shadow/transformed 写上，告警才分得出归属。
// ---------------------------------------------------------------------------
static void test_error_event_carries_marks() {
    std::cout << "== M9.8 错误事件同样带 shadow/transformed 标记（告警归因需要） ==\n";
    clearCapture();
    SqlContext ctx;
    ctx.shadow = true;
    ctx.tenantId = std::string("t-acme");
    ContextScope scope(ctx);
    ResultSet out;
    out.transformed = true;
    OperationEvent e;
    e.dataSource = "shadow-ds";
    e.type = OperationType::Query;
    e.status = Status::error(ErrorCode::NotConnected, "broken");
    e.duration = std::chrono::microseconds(5'000);
    Observability::emitSql(e, "SELECT secret FROM probe", {}, &out);
    {
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "失败事件也触发 observer");
        if (!g_captured.empty()) {
            check(g_captured[0].shadow,
                  "失败事件：event.shadow=true（影子流量发生的失败要单独计数）");
            check(g_captured[0].transformed,
                  "失败事件：event.transformed=true（脱敏路径上的失败也要计入合规）");
            check(!g_captured[0].status.ok(),
                  "失败事件：event.status.ok()=false");
        }
    }
}

int main() {
    test_defaults_without_ctx_or_result();    // M9.1
    test_shadow_propagates_from_ctx();        // M9.2
    test_shadow_resets_after_scope();         // M9.3
    test_transformed_from_result();           // M9.4
    test_not_transformed();                   // M9.5
    test_write_path_keeps_transformed_false();// M9.6
    test_shadow_and_transformed_together();   // M9.7
    test_error_event_carries_marks();         // M9.8
    Observability::setObserver(nullptr);
    Observability::configure({});
    std::cout << "\n===== total PASS=" << g_passed << " FAIL=" << g_failed << " =====\n";
    return g_failed == 0 ? 0 : 1;
}
