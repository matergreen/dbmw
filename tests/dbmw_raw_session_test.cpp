// dbmw_raw_session_test.cpp
//
// M8（v0.4.0 §10）会话级读后写一致性单测。
//
// 设计要点：
//  1. SqlContext::wroteInThisRequest 由 DataSource::markWrite 在写成功路径
//     集中置位；readTarget 检测到该标记后返回 primary_，早于 read_after_write_ms
//     时间戳与副本轮询（§10.2 优先级最高）。
//  2. 栈帧定位：业务 ContextScope 是[s]单帧时栈顶即业务帧；同步 runWithInterceptors
//     有内部 scope（[s, internal_copy]），栈顶 = internal_copy，业务帧 = [s]。
//     pinRequestWrite 选 [sz-2]，无 internal_copy（sz==1）则 [sz-1]。
//  3. 异步路径：submit 时拷贝 entryCtx 快照；worker 写成功后置 entryCtx.wIRT，
//     该 op 后续 attemptFn 透到 entryCtx；新 submit 重新拷，彼此隔离。
//  4. config_loader 副本 + 零窗口 → stderr WARN（不阻断 load）。
//  5. M5（幂等）/ M6（影子）/ M8（wIRT）三者正交，分别独立路由。
//
// 隔离原则：每个场景一个函数，每个函数自建 core::DatabaseManager 实例并 shutdown，
// 零共享 counter。

#include "dbmw/async/dbmw_async.h"
#include "dbmw/common/context.h"
#include "dbmw/config/config_loader.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/core/database_manager.h"
#include "dbmw/core/idatabase_connection.h"
#include "dbmw/core/interceptor.h"     // CircuitBreakerConfig
#include "dbmw/dbmw.h"
#include "dbmw/driver/driver_registry.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <variant>

using namespace dbmw;
using common::Row;
using common::Status;

static int g_failed = 0;
static int g_passed = 0;
static std::string g_scenario;

static void check(bool cond, const std::string &name) {
    if (cond) { ++g_passed; std::cout << "  [PASS] " << name << "\n"; }
    else      { ++g_failed; std::cout << "  [FAIL] " << g_scenario << ": " << name << "\n"; }
}

// variant<nullptr_t,bool,int64_t,double,string,Timestamp,Blob> 里 string 是 index 5
// 显式持有验证：避免 v.index() 漂移。
static std::string rowString(const common::ResultSet &rs, const std::string &field) {
    if (rs.empty()) return {};
    const auto &row = rs.rows().front();
    if (!row.has(field)) return {};
    const auto &v = row.at(field);
    if (!std::holds_alternative<std::string>(v)) return {};
    return std::get<std::string>(v);
}

// ===========================================================================
// 通用 mock：tag-based IDriver。同一驱动可注册多个 name，每个 name 返回不同
// source 字段。完全不需要 static counter —— 状态由 connection 实例自己持有。
// ===========================================================================
namespace mockraw {

class Connection : public core::IDatabaseConnection {
public:
    explicit Connection(std::string tag) : tag_(std::move(tag)) {}
    common::Status connect(const config::DataSourceConfig &) override { open_ = true; return Status::OK(); }
    common::Status ping() override { return Status::OK(); }
    common::Status query(const std::string &, common::ResultSet &out) override {
        if (!open_) {
            auto st = Status::error(common::ErrorCode::NotConnected, "not open");
            st.retryable = true;
            st.connectionBroken = true;
            return st;
        }
        // 每次 query 都把 source 填上行，验证路由结果。
        // pool 缓存连接，单次 query 应当正好 1 行。
        out.setFields({"source"});
        Row r;
        r.set("source", tag_);
        out.addRow(std::move(r));
        return Status::OK();
    }
    common::Status execute(const std::string &, std::int64_t &a) override {
        if (!open_) {
            auto st = Status::error(common::ErrorCode::NotConnected, "not open");
            st.retryable = true;
            st.connectionBroken = true;
            return st;
        }
        a = 1;
        return Status::OK();
    }
    common::Status begin() override { return Status::OK(); }
    common::Status commit() override { return Status::OK(); }
    common::Status rollback() override { return Status::OK(); }
    void close() override { open_ = false; }
    bool isOpen() const override { return open_; }
private:
    bool open_ = false;
    std::string tag_;
};

class Driver : public driver::IDriver {
public:
    explicit Driver(std::string tag) : tag_(std::move(tag)) {}
    const char *name() const override { return "mockraw"; }
    std::unique_ptr<core::IDatabaseConnection> createConnection() override {
        return std::make_unique<Connection>(tag_);
    }
private:
    std::string tag_;
};

inline void install(std::string dsname, std::string tag) {
    driver::DriverRegistry::instance().registerDriver(
        dsname, [tag] { return std::make_unique<Driver>(tag); });
}

} // namespace mockraw

// DriverRegistry 没有 unregisterDriver；同名二次注册即覆盖。
// 各 case 互不相干（用各自 dsCfg），不需要显式卸载。
static void uninstallAllMock() {}

// 构造一个 dsCfg
static config::DataSourceConfig dsCfg(const std::string &name) {
    config::DataSourceConfig c;
    c.name = name;
    c.type = name;          // driver name 与 ds name 同名
    c.host = "localhost";
    c.connection_timeout_ms = 100;
    return c;
}

// 关闭熔断、配重试消除噪声
static core::DataSourceOptions rawOpts() {
    core::DataSourceOptions o;
    o.retry.retry_writes = false;
    o.circuit_breaker.failure_threshold = 0;
    return o;
}

// ===========================================================================
// M8.1 同步：ContextScope 内：写→读同栈 → 读走主（wIRT=primary）
// ===========================================================================
static void M8_1_sync_write_then_read_in_scope() {
    g_scenario = "M8.1";
    std::cout << "== M8.1 sync write-then-read in same scope: wIRT=primary ==\n";

    mockraw::install("primary", "primary");
    mockraw::install("r0", "replica-0");

    core::DatabaseManager mgr;
    mgr.addDataSource(dsCfg("primary"), rawOpts());
    mgr.addDataSource(dsCfg("r0"), rawOpts());

    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "primary";
    config::ReplicaConfig rc; rc.name = "r0"; rc.weight = 1;
    grp.replicas = {rc};
    grp.read_after_write_ms = 60000; // 大窗口：单独靠时间戳也会命中 primary
    check(mgr.addGroup(grp).ok(), "addGroup ok");

    auto g = mgr.getDataSource("grp");

    common::ResultSet rs;
    {
        common::ContextScope scope({});
        std::int64_t a = 0;
        check(g->execute("UPDATE x", a).ok(), "write ok");
        check(a == 1, "write: affected==1");
        check(g->query("SELECT source FROM t", rs).ok(), "read after write ok");
    }
    const auto src = rowString(rs, "source");
    check(src == "primary",
          "M8.1 写后同栈读：source==primary (wIRT pin 优先级 >= 时间戳窗口)");

    mgr.shutdown(std::chrono::milliseconds(0));
    uninstallAllMock();
}

// ===========================================================================
// M8.2 同步：栈空写后无栈读 → 既不 pin 也不走时间戳窗口（read_after_write_ms=0）
// ===========================================================================
static void M8_2_sync_write_no_scope_RAW_zero() {
    g_scenario = "M8.2";
    std::cout << "== M8.2 sync no-scope with RAW=0: pinRequestWrite 早返，读走副本 ==\n";

    mockraw::install("primary", "primary");
    mockraw::install("r0", "replica-0");

    core::DatabaseManager mgr;
    mgr.addDataSource(dsCfg("primary"), rawOpts());
    mgr.addDataSource(dsCfg("r0"), rawOpts());
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "primary";
    config::ReplicaConfig rc; rc.name = "r0"; rc.weight = 1;
    grp.replicas = {rc};
    grp.read_after_write_ms = 0; // 关窗口（与 §10.2 改动 A 配合，仅打 WARN）
    check(mgr.addGroup(grp).ok(), "addGroup ok");

    auto g = mgr.getDataSource("grp");

    std::int64_t a = 0;
    check(g->execute("UPDATE x", a).ok(), "no-scope write ok");
    common::ResultSet rs;
    check(g->query("SELECT source FROM t", rs).ok(), "no-scope read ok");
    const auto src = rowString(rs, "source");
    check(src == "replica-0",
          "M8.2 RAW=0 + 无 ContextScope：source==replica-0（pinWrite 早返）");

    mgr.shutdown(std::chrono::milliseconds(0));
    uninstallAllMock();
}

// ===========================================================================
// M8.3 同步：frame A 写，frame B 读 → 隔离（各自 ContextScope 帧）
// 设计：read_after_write_ms=0 才能隔离 DataSource 时间戳窗口这一干扰项，
//      否则 readTarget 的第 2 级判定（时间戳窗口）会替 M8 wIRT 抓回 primary。
// ===========================================================================
static void M8_3_sync_scopes_isolated() {
    g_scenario = "M8.3";
    std::cout << "== M8.3 sync: A 帧写 / B 帧读 → B 不受 A wIRT 影响（RAW=0 隔离时间戳）==\n";

    mockraw::install("primary", "primary");
    mockraw::install("r0", "replica-0");

    core::DatabaseManager mgr;
    mgr.addDataSource(dsCfg("primary"), rawOpts());
    mgr.addDataSource(dsCfg("r0"), rawOpts());
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "primary";
    config::ReplicaConfig rc; rc.name = "r0"; rc.weight = 1;
    grp.replicas = {rc};
    grp.read_after_write_ms = 0; // §10.2 改动 A：关时间戳窗口，剩下的只有 wIRT 这一条粘性源
    check(mgr.addGroup(grp).ok(), "addGroup ok");

    auto g = mgr.getDataSource("grp");

    // frame A：写（wIRT pinned 到 A 帧）
    {
        common::ContextScope a({});
        std::int64_t aAff = 0;
        check(g->execute("UPDATE x", aAff).ok(), "A: write ok");
    }
    // A 已析构：wIRT 跟着销毁
    // frame B（无 A 共享栈）：读 → 应走副本（wIRT=false，RAW=0）
    common::ResultSet rs;
    {
        common::ContextScope b({});
        check(g->query("SELECT source FROM t", rs).ok(), "B: read ok");
    }
    const auto src = rowString(rs, "source");
    check(src == "replica-0",
          "M8.3 RAW=0 + frame-B read：source==replica-0（A 帧已析构，wIRT 不串）");

    mgr.shutdown(std::chrono::milliseconds(0));
    uninstallAllMock();
}

// ===========================================================================
// M8.4 异步：worker 写成功后置 entryCtx.wIRT；同 attemptFn 内后续读走主
// 设计：read_after_write_ms=0 隔离 DataSource 时间戳窗口这条干扰线。
//      本用例验证**异步 worker 的 entryCtx 隔离**：上一次 op 的 entryCtx.wIRT
//      不污染下次 submit 创建的新 entryCtx。新 submit → 拷一份新栈顶（wIRT=false）→
//      不应继承上次 op 内部写留下的 wIRT=true。
// ===========================================================================
static void M8_4_async_wirt_in_entryctx() {
    g_scenario = "M8.4";
    std::cout << "== M8.4 async: 新 submit 的 entryCtx 不被前次 op 污染（RAW=0）==\n";

    mockraw::install("primary", "primary");
    mockraw::install("r0", "replica-0");

    const auto path = (std::filesystem::temp_directory_path() /
                       "dbmw_m8_async.json").string();
    std::ofstream(path) << R"({
  "default_datasource": "grp",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 4, "borrow_timeout_ms": 2000 },
  "retry": { "max_attempts": 1, "initial_backoff_ms": 0, "max_backoff_ms": 0, "retry_writes": false },
  "circuit_breaker": { "failure_threshold": 0 },
  "rate_limit": { "enabled": false },
  "sql_audit": { "enabled": false },
  "query_cache": { "enabled": false },
  "async": { "enabled": true, "threads": 1, "queue_size": 64 },
  "datasources": [
    { "name": "primary", "type": "primary", "host": "localhost", "connection_timeout_ms": 100 },
    { "name": "r0", "type": "r0", "host": "localhost", "connection_timeout_ms": 100 }
  ],
  "groups": [
    { "name": "grp", "primary": "primary", "replicas": [{ "name": "r0", "weight": 1 }], "read_after_write_ms": 0 }
  ]
})";
    check(DBMW::init(path).ok(), "DBMW::init ok");

    // 第一次写：scope 内提交 → submit 时栈顶 wIRT=false（业务还未写）
    //   worker 跑成功后置 entryCtx.wIRT=true（但仅本 op 内部有效）。
    {
        std::promise<async::ExecResult> pw;
        auto fw = pw.get_future();
        common::ContextScope scope({}); // wIRT=false（裸 scope）
        async::execute("grp", "UPDATE x",
                       [&pw](async::ExecResult &&r) mutable { pw.set_value(std::move(r)); });
        auto rw = fw.get();
        check(rw.status.ok(), "async write: status ok");
        check(rw.affected == 1, "async write: affected==1");
    }

    // 第二次读：**新 submit，新 entryCtx 拷贝时栈顶 wIRT=false**
    //   若 worker 间能跨 op 泄漏 wIRT，会走到 primary；正常应走副本。
    common::ResultSet rs;
    {
        std::promise<async::QueryResult> pr;
        auto fr = pr.get_future();
        common::ContextScope scope({});
        async::query("grp", "SELECT source FROM t",
                     [&pr](async::QueryResult &&r) mutable { pr.set_value(std::move(r)); });
        auto rr = fr.get();
        check(rr.status.ok(), "async read after unrelated scope: status ok");
        rs = std::move(rr.rows);
    }
    const auto src = rowString(rs, "source");
    check(src == "replica-0",
          "M8.4 RAW=0 + 新 submit：source==replica-0（前次 op 的 entryCtx.wIRT 不串）");

    DBMW::shutdown(std::chrono::milliseconds(0));
    uninstallAllMock();
}

// ===========================================================================
// M8.5 config_loader：副本 + 零窗口 → stderr WARN
// ===========================================================================
static void M8_5_config_loader_warns_on_replica_zero_window() {
    g_scenario = "M8.5";
    std::cout << "== M8.5 config_loader 副本 + 零窗口：fprintf(stderr, ...) 必须出现 ==\n";

    // 重定向 stderr 到临时文件，捕获后再读。
    const auto errPath = (std::filesystem::temp_directory_path() /
                          "dbmw_m8_stderr.txt").string();
    std::ofstream devnull("/dev/null");
    // 用 freopen 把 stderr 重定向到 errPath
    std::freopen(errPath.c_str(), "w", stderr);
    // 同时把 stdout 静默（不希望 noise）
    std::freopen("/dev/null", "w", stdout);

    const auto path = (std::filesystem::temp_directory_path() /
                       "dbmw_m8_cfg_warn.json").string();
    std::ofstream(path) << R"({
  "default_datasource": "p",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 2, "borrow_timeout_ms": 1000 },
  "retry": { "max_attempts": 1, "initial_backoff_ms": 0, "max_backoff_ms": 0, "retry_writes": false },
  "circuit_breaker": { "failure_threshold": 0 },
  "rate_limit": { "enabled": false },
  "sql_audit": { "enabled": false },
  "query_cache": { "enabled": false },
  "async": { "enabled": false },
  "datasources": [
    { "name": "p", "type": "primary", "host": "localhost" },
    { "name": "r0", "type": "r0", "host": "localhost" }
  ],
  "groups": [
    { "name": "g", "primary": "p", "replicas": [{ "name": "r0", "weight": 1 }], "read_after_write_ms": 0 }
  ]
})";
    config::GlobalConfig cfg;
    std::string err;
    const bool ok = config::ConfigLoader::loadFromFile(path, cfg, err);
    // 先把 stderr/stdout 还原——这样 check() 还能正常打到终端
    std::fflush(stderr);
    std::freopen("/dev/tty", "w", stderr);
    std::freopen("/dev/tty", "w", stdout);

    check(ok, "ConfigLoader: load ok");
    check(err.empty(), "ConfigLoader: no error");

    std::ifstream errFile(errPath);
    std::string captured((std::istreambuf_iterator<char>(errFile)),
                          std::istreambuf_iterator<char>());
    const bool sawWarn = captured.find("read_after_write_ms=0") != std::string::npos
                      && captured.find("replica") != std::string::npos
                      && captured.find("stale data") != std::string::npos;
    check(sawWarn,
          "M8.5 stderr 输出含 'read_after_write_ms=0 / replica / stale data' 提示");

    std::remove(errPath.c_str());
    std::remove(path.c_str());
    uninstallAllMock();
}

// ===========================================================================
// M8.6 正交：M5 idempotent + M8 wIRT 互不干扰
//  NonIdempotent 写后置位 wIRT，照样 primary
// ===========================================================================
static void M8_6_idempotency_orthogonal() {
    g_scenario = "M8.6";
    std::cout << "== M8.6 NonIdempotent 写后置位 wIRT：与幂等正交 ==\n";

    mockraw::install("primary", "primary");
    mockraw::install("r0", "replica-0");

    core::DatabaseManager mgr;
    core::DataSourceOptions o = rawOpts();
    o.retry.retry_writes = true;        // 故意：让 NonIdempotent=1 差异凸显
    o.retry.max_attempts = 4;
    mgr.addDataSource(dsCfg("primary"), o);
    mgr.addDataSource(dsCfg("r0"), o);
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "primary";
    config::ReplicaConfig rc; rc.name = "r0"; rc.weight = 1;
    grp.replicas = {rc};
    grp.read_after_write_ms = 60000;
    check(mgr.addGroup(grp).ok(), "addGroup ok");

    auto g = mgr.getDataSource("grp");

    common::ResultSet rs;
    {
        common::SqlContext nonIdempotentContext;
        nonIdempotentContext.idempotency = common::Idempotency::NonIdempotent;
        common::ContextScope scope(nonIdempotentContext);
        std::int64_t a = 0;
        check(g->execute("UPDATE x", a).ok(), "NonIdempotent write ok");
        check(a == 1, "NonIdempotent write: affected==1 (no retry w/o failure)");
        check(g->query("SELECT source FROM t", rs).ok(), "NonIdempotent: read after write");
    }
    const auto src = rowString(rs, "source");
    check(src == "primary",
          "M8.6 NonIdempotent 写后读：source==primary（幂等只改 attempts，不改 wIRT）");

    mgr.shutdown(std::chrono::milliseconds(0));
    uninstallAllMock();
}

int main() {
    std::cout << "===== dbmw_raw_session_test (M8) =====\n";

    // M8.1 — 必须最先跑，验证最简路径
    M8_1_sync_write_then_read_in_scope();

    // M8.2 — pinRequestWrite 栈空早返
    M8_2_sync_write_no_scope_RAW_zero();

    // M8.3 — 帧隔离
    M8_3_sync_scopes_isolated();

    // M8.4 — 异步 entryCtx 隔离
    M8_4_async_wirt_in_entryctx();

    // M8.5 — config_loader WARN
    M8_5_config_loader_warns_on_replica_zero_window();

    // M8.6 — 正交
    M8_6_idempotency_orthogonal();

    std::cout << "===== total PASS=" << g_passed << " FAIL=" << g_failed << " =====\n";
    return g_failed == 0 ? 0 : 1;
}
