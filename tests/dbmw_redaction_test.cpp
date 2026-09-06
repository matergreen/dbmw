// dbmw v0.4.0 M7 单测：结果脱敏（I10 落地）§9。
//
// 覆盖（docs/roadmap-design-v0.4.0.md §9.4 风险表 + §9.3 落地步骤）：
//   1. QueryCache 只保存驱动原始结果，SPI afterExecution 在返回前改写副本。
//   2. 第二次同 SQL：命中原始缓存，仍再次触发脱敏拦截器。
//   3. 未脱敏的读照常进缓存（I10 不误伤）。
//   4. 缓存命中路径仍要调 afterExecution（§9.4 风险行：缓存命中漏脱敏）。
//   5. 异步路径下脱敏 + 缓存交互同源：transformed 进缓存被守卫拦截，
//      缓存命中也会触发 afterExecution 再次脱敏。
//
// 测试用 mock 拦截器：每次 afterExecution 时把数据列改写为 "***" 并
// 设 transformed=true。MockConnection 维持 queryCount 计数验证调用次数。
#include "dbmw/dbmw.h"
#include "dbmw/async/dbmw_async.h"
#include "dbmw/common/context.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/core/database_manager.h"
#include "dbmw/core/idatabase_connection.h"
#include "dbmw/core/interceptor.h"
#include "dbmw/core/query_cache.h"
#include "dbmw/driver/driver_registry.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <string>

using namespace dbmw;
using common::Status;
using common::ErrorCode;

static int g_failed = 0;
static int g_passed = 0;

static void check(bool cond, const std::string &name) {
    if (cond) { ++g_passed; std::cout << "  [PASS] " << name << "\n"; }
    else { ++g_failed; std::cout << "  [FAIL] " << name << "\n"; }
}

// ---------------------------------------------------------------------------
// Mock 驱动：每次 query 返回一行固定数据 "secret=N"，便于验证脱敏。
// queryCount 用于数 driver 调用次数：第二次若命中缓存则不再调 driver。
// ---------------------------------------------------------------------------
static std::atomic<int> gMockQueryCount{0};

class MockRedactConnection : public core::IDatabaseConnection {
public:
    common::Status connect(const config::DataSourceConfig &) override {
        open_ = true; return Status::OK();
    }
    common::Status ping() override {
        return open_ ? Status::OK()
                     : Status::error(common::ErrorCode::NotConnected, "closed");
    }
    common::Status query(const std::string &, common::ResultSet &out) override {
        ++gMockQueryCount;
        out.setFields({"secret"});
        common::Row r;
        r.set("secret", std::string("SECRET-12345"));
        out.addRow(std::move(r));
        return Status::OK();
    }
    common::Status execute(const std::string &, std::int64_t &affected) override {
        affected = 1; return Status::OK();
    }
    common::Status begin() override { return Status::OK(); }
    common::Status commit() override { return Status::OK(); }
    common::Status rollback() override { return Status::OK(); }
    void close() override { open_ = false; }
    bool isOpen() const override { return open_; }
private:
    bool open_ = false;
};

class MockRedactDriver : public driver::IDriver {
public:
    const char *name() const override { return "mockr"; }
    std::unique_ptr<core::IDatabaseConnection> createConnection() override {
        return std::make_unique<MockRedactConnection>();
    }
};

// ---------------------------------------------------------------------------
// 测试用拦截器：afterExecution 把所有 cell 改写成 "***" 并置 transformed=true。
// 三个 toggle 控制行为：enableTransform / changeCellValue。
// ---------------------------------------------------------------------------
struct RedactionInterceptor : public core::ISqlInterceptor {
    bool enableTransform = true;       // 总开关（影响是否置 transformed）
    std::string maskedValue = "***";   // 改写后的值
    std::atomic<int> afterCount{0};    // 调用计数（验证 §9.4 缓存命中是否漏调）
    std::atomic<int> rowCount{0};

    void onRoute(const std::string &, const std::string &,
                 common::OperationType, common::SqlContext &) override {}

    common::Status beforeExecution(const core::ExecutionView &) override {
        return Status::OK();
    }

    void afterExecution(const core::ExecutionView &view) override {
        ++afterCount;
        if (!enableTransform) return;
        if (!view.result) return;       // 非查询（写/批/游标）不动
        // I10 标记（关键）：声明该结果已被业务改写。
        // 实际改写 cell 的逻辑由 MaskingInterceptor 的实现者承担
        // （dbmw 只提供"标记 + 守卫"机制，不替业务做合规决策）。
        view.result->transformed = true;
        (void)view.result->rows();     // 留作"业务可遍历"的接口演示
    }

    void onRow(const core::ExecutionView &, common::Row &row) override {
        ++rowCount;
        if (enableTransform && row.has("secret")) row.set("secret", maskedValue);
    }

    void onCompletion(const core::ExecutionView &) override {}
};

// ---------------------------------------------------------------------------
// M7.1 同步路径：缓存原始结果，命中后按请求重新脱敏（I10 核心）。
// ---------------------------------------------------------------------------
static void test_sync_redaction_not_cached() {
    std::cout << "== M7.1 同步路径：脱敏读不进缓存（I10 核心）==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig ds;
    ds.name = "ds"; ds.type = "mockr"; ds.host = "localhost";
    check(mgr.addDataSource(ds).ok(), "addDataSource ok");

    // 开启查询缓存。
    config::QueryCacheConfig qc_on;
    qc_on.enabled = true;
    core::QueryCache::configure(qc_on);

    // 注册脱敏拦截器。
    auto redact = std::make_shared<RedactionInterceptor>();
    redact->enableTransform = true;
    core::InterceptorRegistry::clear();
    core::InterceptorRegistry::setEnabled(true);
    core::InterceptorRegistry::add(redact);

    auto g = mgr.getDataSource("ds");
    gMockQueryCount = 0;
    redact->afterCount = 0;

    common::ResultSet rs1;
    check(g->query("SELECT secret", rs1).ok(), "首次读：成功");
    check(rs1.transformed, "首次读：rs.transformed=true（拦截器置位）");
    check(rs1.rowCount() == 1, "首次读：1 行");

    common::ResultSet rs2;
    check(g->query("SELECT secret", rs2).ok(), "第二次读：成功");
    check(rs2.transformed, "第二次读：rs.transformed=true（每次都重脱敏）");
    check(rs2.rowCount() == 1, "第二次读：1 行");

    check(gMockQueryCount.load() == 1,
          "I10：driver 只调用 1 次，缓存保存的是原始结果");
    check(redact->afterCount.load() == 2,
          "每个公开 query 恰好执行一次 afterExecution");

    std::uint64_t streamedRows = 0;
    std::string streamedSecret;
    check(g->queryEach("SELECT secret", {}, [&](const common::Row &row) {
              if (const auto *value = std::get_if<std::string>(&row.at("secret")))
                  streamedSecret = *value;
              return true;
          }, streamedRows).ok() && streamedRows == 1,
          "queryEach 正常逐行交付");
    check(streamedSecret == "***" && redact->rowCount.load() == 1,
          "queryEach 在业务回调前恰好执行一次 onRow 脱敏");

    core::QueryCache::configure({});    // 关闭缓存隔离
    core::InterceptorRegistry::clear();
    core::InterceptorRegistry::setEnabled(false);
    mgr.shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// M7.2 未脱敏的读照常进缓存（I10 不误伤）。
// 把拦截器 enableTransform 关掉（afterExecution 直接 return）→ 改写
// 未发生 → transformed 保持 false → 结果可缓存 → 第二次读命中缓存。
// ---------------------------------------------------------------------------
static void test_non_redacted_caches_normally() {
    std::cout << "== M7.2 未脱敏的读照常进缓存（I10 不误伤）==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig ds;
    ds.name = "ds"; ds.type = "mockr"; ds.host = "localhost";
    mgr.addDataSource(ds);

    config::QueryCacheConfig qc_on;
    qc_on.enabled = true;
    core::QueryCache::configure(qc_on);

    auto noop = std::make_shared<RedactionInterceptor>();
    noop->enableTransform = false;      // 关键：不动 transformed
    core::InterceptorRegistry::clear();
    core::InterceptorRegistry::setEnabled(true);
    core::InterceptorRegistry::add(noop);

    auto g = mgr.getDataSource("ds");
    gMockQueryCount = 0;
    noop->afterCount = 0;

    common::ResultSet rs1;
    g->query("SELECT secret", rs1);
    check(!rs1.transformed, "首次读：rs.transformed=false（未脱敏）");
    check(rs1.rowCount() == 1, "首次读：1 行");

    common::ResultSet rs2;
    g->query("SELECT secret", rs2);
    check(!rs2.transformed, "第二次读：rs.transformed=false");
    check(gMockQueryCount.load() == 1,
          "非脱敏读：driver 只调 1 次（缓存命中）");
    check(noop->afterCount.load() == 2,
          "每个公开 query 恰好执行一次 afterExecution");

    core::QueryCache::configure({});
    core::InterceptorRegistry::clear();
    core::InterceptorRegistry::setEnabled(false);
    mgr.shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// M7.3 transformed 标记在 RS 跨函数传递时仍生效。
// 设计一个"分层"拦截器：把 transformed 标记暴露到外部静态位，
// 由测试主体在两次 query 之间检查 transformed 翻转。
// 验证：transformed=true 时缓存不入库（即使在拦截器里翻转）。
// ---------------------------------------------------------------------------
static std::atomic<int> gTransformedReads{0};   // 拦截器报告的"被改写"次数

struct RedactionFlagInterceptor : public core::ISqlInterceptor {
    void onRoute(const std::string &, const std::string &,
                 common::OperationType, common::SqlContext &) override {}
    common::Status beforeExecution(const core::ExecutionView &) override {
        return Status::OK();
    }
    void afterExecution(const core::ExecutionView &view) override {
        if (!view.result) return;
        view.result->transformed = true;
        ++gTransformedReads;
    }
    void onCompletion(const core::ExecutionView &) override {}
};

static void test_transformed_flag_blocks_cache() {
    std::cout << "== M7.3 transformed=true 不污染原始缓存==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig ds;
    ds.name = "ds"; ds.type = "mockr"; ds.host = "localhost";
    mgr.addDataSource(ds);

    config::QueryCacheConfig qc_on;
    qc_on.enabled = true;
    core::QueryCache::configure(qc_on);

    auto re = std::make_shared<RedactionFlagInterceptor>();
    core::InterceptorRegistry::clear();
    core::InterceptorRegistry::setEnabled(true);
    core::InterceptorRegistry::add(re);

    auto g = mgr.getDataSource("ds");
    gMockQueryCount = 0;
    gTransformedReads = 0;

    common::ResultSet rs1;
    g->query("SELECT x", rs1);
    check(rs1.transformed, "首次：rs.transformed=true");
    check(gMockQueryCount.load() == 1, "首次：driver 1 次");
    // 首次：afterExecution 改写标记 1 次（仅 DataSource 层；Session 子语句未触发因它未到 driver）
    check(gTransformedReads.load() >= 1, "首次：afterExecution 改写标记 ≥1 次");

    // 第二次从原始缓存复制结果，再执行当前请求的改写。
    common::ResultSet rs2;
    g->query("SELECT x", rs2);
    check(rs2.transformed, "二次：rs.transformed=true（再次脱敏）");
    check(gMockQueryCount.load() == 1,
          "二次：命中未改写的原始缓存");
    check(gTransformedReads.load() == 2,
          "二次：afterExecution 改写标记累计 2 次");

    core::QueryCache::configure({});
    core::InterceptorRegistry::clear();
    core::InterceptorRegistry::setEnabled(false);
    mgr.shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// M7.4 异步路径：脱敏 + 缓存交互同源。
// 异步 query 两次：第一次走 driver + afterExecution 改写 → 不进缓存。
// 第二次：缓存未命中 → driver 再次调 → 再次 afterExecution。
// ---------------------------------------------------------------------------
static void test_async_redaction_not_cached() {
    std::cout << "== M7.4 异步路径：脱敏读不进缓存（I10 + 异步桥接）==\n";
    const auto path = (std::filesystem::temp_directory_path() /
                       "dbmw_redaction_async.json").string();
    std::ofstream(path) << R"({
  "default_datasource": "ds",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 4, "borrow_timeout_ms": 2000 },
  "retry": { "max_attempts": 1, "retry_writes": true },
  "circuit_breaker": { "failure_threshold": 0 },
  "rate_limit": { "enabled": false },
  "sql_audit": { "enabled": false },
  "query_cache": { "enabled": true },
  "async": { "enabled": true, "threads": 1, "queue_size": 64 },
  "interceptors": { "enabled": true },
  "datasources": [
    { "name": "ds", "type": "mockr", "host": "localhost" }
  ]
})";
    check(DBMW::init(path).ok(), "DBMW::init(async redaction cfg) ok");

    auto redact = std::make_shared<RedactionInterceptor>();
    redact->enableTransform = true;
    core::InterceptorRegistry::clear();
    core::InterceptorRegistry::add(redact);

    gMockQueryCount = 0;
    redact->afterCount = 0;

    // 第一次 async query
    {
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::query("ds", "SELECT secret",
                     [&](async::QueryResult &&r) { pr.set_value(std::move(r)); });
        const auto out = fut.get();
        check(out.status.ok(), "异步首次：成功");
        check(out.rows.rowCount() == 1, "异步首次：1 行");
        // transform 标记生效
        check(out.rows.transformed, "异步首次：rows.transformed=true");
    }

    // 第二次 async query
    {
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::query("ds", "SELECT secret",
                     [&](async::QueryResult &&r) { pr.set_value(std::move(r)); });
        const auto out = fut.get();
        check(out.status.ok(), "异步第二次：成功");
        check(out.rows.transformed, "异步第二次：rows.transformed=true");
    }

    check(gMockQueryCount.load() == 2,
          "异步 I10：driver 调用 2 次（缓存被 I10 守住）");
    check(redact->afterCount.load() >= 2,
          "异步 afterExecution 至少 2 次（缓存命中也跑）");

    DBMW::shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// M7.5 异步路径：缓存命中后调 afterExecution（§9.4 风险行）。
//   - 拦截器先做一次未脱敏的读 → 缓存被填（不带 transformed）。
//   - 再注册脱敏拦截器 → 第二次读：缓存命中 + 调 afterExecution 改写 + 不重写回缓存。
// 这一用例验证：缓存命中路径必须经过 afterExecution，否则脱敏被绕过。
// ---------------------------------------------------------------------------
static void test_async_cache_hit_triggers_after() {
    std::cout << "== M7.5 异步缓存命中仍触发 afterExecution（§9.4 风险行）==\n";
    const auto path = (std::filesystem::temp_directory_path() /
                       "dbmw_redaction_async_hit.json").string();
    std::ofstream(path) << R"({
  "default_datasource": "ds",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 4, "borrow_timeout_ms": 2000 },
  "retry": { "max_attempts": 1, "retry_writes": true },
  "circuit_breaker": { "failure_threshold": 0 },
  "rate_limit": { "enabled": false },
  "sql_audit": { "enabled": false },
  "query_cache": { "enabled": true },
  "async": { "enabled": true, "threads": 1, "queue_size": 64 },
  "interceptors": { "enabled": false },
  "datasources": [
    { "name": "ds", "type": "mockr", "host": "localhost" }
  ]
})";
    check(DBMW::init(path).ok(), "DBMW::init(async cache hit cfg) ok");

    // 第一次：interceptors.enabled=false（在 JSON 里）→ 不调 afterExecution，
    // 结果是原始 SECRET-12345，缓存正常入库。
    gMockQueryCount = 0;
    {
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::query("ds", "SELECT secret",
                     [&](async::QueryResult &&r) { pr.set_value(std::move(r)); });
        const auto out = fut.get();
        check(out.status.ok(), "首次异步：成功（缓存被填，无脱敏）");
        check(!out.rows.transformed, "首次异步：rows.transformed=false");
        check(out.rows.rowCount() == 1, "首次异步：1 行");
    }
    check(gMockQueryCount.load() == 1, "首次异步：driver 1 次（缓存填了）");

    // 第二次：动态把拦截器打开 + 注册脱敏拦截器。
    auto redact2 = std::make_shared<RedactionInterceptor>();
    redact2->enableTransform = true;
    redact2->afterCount = 0;
    core::InterceptorRegistry::setEnabled(true);
    core::InterceptorRegistry::add(redact2);

    {
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::query("ds", "SELECT secret",
                     [&](async::QueryResult &&r) { pr.set_value(std::move(r)); });
        const auto out = fut.get();
        check(out.status.ok(), "二次异步（缓存命中 + 脱敏）：成功");
        // 缓存命中：driver 不再调
        check(gMockQueryCount.load() == 1,
              "缓存命中：driver 仍只调 1 次（验证 §9.4 修复：缓存命中走 afterExecution）");
        // 但 afterExecution 必须跑一次：把缓存里原始数据改写成 ***
        check(redact2->afterCount.load() == 1,
              "afterExecution 调 1 次（缓存命中路径补调，§9.4 修复）");
        check(out.rows.transformed,
              "缓存命中 + afterExecution：rows.transformed=true");
        check(out.rows.rowCount() == 1, "缓存命中 + afterExecution：1 行");
    }

    DBMW::shutdown(std::chrono::milliseconds(0));
}

int main() {
    driver::DriverRegistry::instance().registerDriver(
        "mockr", [] { return std::make_unique<MockRedactDriver>(); });

    test_sync_redaction_not_cached();
    test_non_redacted_caches_normally();
    test_transformed_flag_blocks_cache();
    test_async_redaction_not_cached();
    test_async_cache_hit_triggers_after();

    std::cout << "\n========== M7 结果脱敏 总计: " << g_passed << " 通过 / "
              << g_failed << " 失败 ==========\n";
    return g_failed == 0 ? 0 : 1;
}
