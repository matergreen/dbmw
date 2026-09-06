// dbmw v0.4.0 M6 单测：影子库路由（§8）。
//
// 覆盖（docs/roadmap-design-v0.4.0.md §8）：
//   1. 同步读：onRoute 置 shadow → query 落到影子叶而非主叶。
//   2. 同步写：onRoute 置 shadow → execute 落到影子叶；不进写缓冲（I12）。
//   3. 影子读不进查询缓存（避免污染真实租户）。
//   4. 未触发 shadow → 读走主/副本，写走主（路由无变化）。
//   5. 校验失败：影子源不存在 → init 返回 ConfigError。
//   6. 校验失败：影子源是本组主 → 拒绝（自影自己）。
//   7. 校验失败：影子源是本组副本 → 拒绝。
//   8. 校验失败：影子源是另一个组名 → 拒绝（组不可直接作影子目标）。
//   9. 异步路径：onRoute 置 shadow → 异步路由到影子叶；写缓冲不入队。
//
// 无真实数据库依赖。两个 mock 驱动（mockp 生产、mocks 影子）分别统计
// execute/query 计数 + 写缓冲入队计数；通过"影子触发后只有 mocks 的计数
// 增加"判定路由是否真正生效。
#include "dbmw/dbmw.h"
#include "dbmw/async/dbmw_async.h"
#include "dbmw/common/context.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/core/connection_pool.h"
#include "dbmw/core/database_manager.h"
#include "dbmw/core/idatabase_connection.h"
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
// Mock 驱动：两套独立类，分别代表"生产叶"与"影子叶"。
// 各自独立的 execute/query 计数 + 写缓冲入队计数用于判定路由命中。
// ---------------------------------------------------------------------------
class MockPrimaryConnection : public core::IDatabaseConnection {
public:
    static std::atomic<int> execCount;
    static std::atomic<int> queryCount;

    common::Status connect(const config::DataSourceConfig &) override {
        open_ = true; return Status::OK();
    }
    common::Status ping() override {
        return open_ ? Status::OK()
                     : Status::error(common::ErrorCode::NotConnected, "closed");
    }
    common::Status query(const std::string &, common::ResultSet &out) override {
        ++queryCount;
        common::Row r;
        r.set("source", std::string("primary"));
        out.addRow(std::move(r));
        return Status::OK();
    }
    common::Status execute(const std::string &, std::int64_t &affected) override {
        ++execCount;
        affected = 1;
        return Status::OK();
    }
    common::Status begin() override { return Status::OK(); }
    common::Status commit() override { return Status::OK(); }
    common::Status rollback() override { return Status::OK(); }
    void close() override { open_ = false; }
    bool isOpen() const override { return open_; }

private:
    bool open_ = false;
};
std::atomic<int> MockPrimaryConnection::execCount{0};
std::atomic<int> MockPrimaryConnection::queryCount{0};

class MockPrimaryDriver : public driver::IDriver {
public:
    const char *name() const override { return "mockp"; }
    std::unique_ptr<core::IDatabaseConnection> createConnection() override {
        return std::make_unique<MockPrimaryConnection>();
    }
};

class MockShadowConnection : public core::IDatabaseConnection {
public:
    static std::atomic<int> execCount;
    static std::atomic<int> queryCount;

    common::Status connect(const config::DataSourceConfig &) override {
        open_ = true; return Status::OK();
    }
    common::Status ping() override {
        return open_ ? Status::OK()
                     : Status::error(common::ErrorCode::NotConnected, "closed");
    }
    common::Status query(const std::string &, common::ResultSet &out) override {
        ++queryCount;
        common::Row r;
        r.set("source", std::string("shadow"));
        out.addRow(std::move(r));
        return Status::OK();
    }
    common::Status execute(const std::string &, std::int64_t &affected) override {
        ++execCount;
        affected = 1;
        return Status::OK();
    }
    common::Status begin() override { return Status::OK(); }
    common::Status commit() override { return Status::OK(); }
    common::Status rollback() override { return Status::OK(); }
    void close() override { open_ = false; }
    bool isOpen() const override { return open_; }

private:
    bool open_ = false;
};
std::atomic<int> MockShadowConnection::execCount{0};
std::atomic<int> MockShadowConnection::queryCount{0};

class MockShadowDriver : public driver::IDriver {
public:
    const char *name() const override { return "mocks"; }
    std::unique_ptr<core::IDatabaseConnection> createConnection() override {
        return std::make_unique<MockShadowConnection>();
    }
};

static void resetCounters() {
    MockPrimaryConnection::execCount = 0;
    MockPrimaryConnection::queryCount = 0;
    MockShadowConnection::execCount = 0;
    MockShadowConnection::queryCount = 0;
}

// ---------------------------------------------------------------------------
// M6.1 同步读：onRoute 置 shadow → query 落到影子叶。
// ---------------------------------------------------------------------------
static void test_sync_shadow_read() {
    std::cout << "== M6.1 同步读：onRoute 置 shadow → query 落到影子叶 ==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig pp;
    pp.name = "prod"; pp.type = "mockp"; pp.host = "localhost";
    check(mgr.addDataSource(pp).ok(), "addDataSource(prod) ok");
    config::DataSourceConfig sh;
    sh.name = "shadow_ds"; sh.type = "mocks"; sh.host = "localhost";
    check(mgr.addDataSource(sh).ok(), "addDataSource(shadow_ds) ok");
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "prod";
    grp.shadow = "shadow_ds";
    check(mgr.addGroup(grp).ok(), "addGroup(grp, shadow=shadow_ds) ok");

    auto g = mgr.getDataSource("grp");
    resetCounters();

    // 不推 shadow → 走默认主路径。
    common::ResultSet rs;
    check(g->query("SELECT 1", rs).ok(), "无 shadow：query 成功");
    check(MockPrimaryConnection::queryCount.load() == 1, "无 shadow：主叶 query 1 次");
    check(MockShadowConnection::queryCount.load() == 0, "无 shadow：影子叶 query 0 次");

    // 推 shadow → 路由到影子叶。
    resetCounters();
    common::ResultSet rs2;
    {
        common::SqlContext shadowContext;
        shadowContext.shadow = true;
        common::ContextScope scope(shadowContext);
        check(g->query("SELECT 1", rs2).ok(), "shadow=true：query 成功");
    }
    check(MockShadowConnection::queryCount.load() == 1, "shadow=true：影子叶 query 1 次");
    check(MockPrimaryConnection::queryCount.load() == 0, "shadow=true：主叶 query 0 次");

    mgr.shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// M6.2 同步写：onRoute 置 shadow → execute 落到影子叶。
// ---------------------------------------------------------------------------
static void test_sync_shadow_write() {
    std::cout << "== M6.2 同步写：onRoute 置 shadow → execute 落到影子叶 ==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig pp;
    pp.name = "prod"; pp.type = "mockp"; pp.host = "localhost";
    mgr.addDataSource(pp);
    config::DataSourceConfig sh;
    sh.name = "shadow_ds"; sh.type = "mocks"; sh.host = "localhost";
    mgr.addDataSource(sh);
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "prod";
    grp.shadow = "shadow_ds";
    mgr.addGroup(grp);

    auto g = mgr.getDataSource("grp");
    resetCounters();
    std::int64_t aff = 0;
    {
        common::SqlContext shadowContext;
        shadowContext.shadow = true;
        common::ContextScope scope(shadowContext);
        check(g->execute("INSERT INTO t VALUES (1)", aff).ok(),
              "shadow=true：execute 成功");
    }
    check(MockShadowConnection::execCount.load() == 1, "shadow=true：影子叶 execute 1 次");
    check(MockPrimaryConnection::execCount.load() == 0, "shadow=true：主叶 execute 0 次");

    mgr.shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// 失败注入 mock（文件作用域，便于注册驱动；本文件内 helper 通用计数器）。
// ---------------------------------------------------------------------------
static std::atomic<int> gShadowFailRemaining{0};

class FailingShadowConnection : public MockShadowConnection {
    public:
        common::Status execute(const std::string &, std::int64_t &affected) override {
            int prev = gShadowFailRemaining.fetch_sub(1, std::memory_order_acq_rel);
            if (prev > 0) {
                auto st = Status::error(common::ErrorCode::NotConnected, "shadow broken");
                st.retryable = true;
                st.connectionBroken = true;
                return st;
            }
            return MockShadowConnection::execute("", affected);
        }
        common::Status query(const std::string &, common::ResultSet &out) override {
            int prev = gShadowFailRemaining.fetch_sub(1, std::memory_order_acq_rel);
            if (prev > 0) {
                auto st = Status::error(common::ErrorCode::NotConnected, "shadow broken");
                st.retryable = true;
                st.connectionBroken = true;
                return st;
            }
            return MockShadowConnection::query("", out);
        }
    };

class FailingShadowDriver : public driver::IDriver {
    public:
        const char *name() const override { return "mocks_fail"; }
        std::unique_ptr<core::IDatabaseConnection> createConnection() override {
            return std::make_unique<FailingShadowConnection>();
        }
};

// ---------------------------------------------------------------------------
// M6.3 影子写不进写缓冲（I12）：主与影子都失败时，不应入队到主库的缓冲。
// 影子故障应让压测停掉，而不是悄悄补发到生产库。
// 这里我们用一个会失败连接的影子 mock；为简化，只验证影子失败时没有触发
// 写缓冲入队（生产主路径不会执行）。
// ---------------------------------------------------------------------------
static void test_shadow_no_write_buffer() {
    std::cout << "== M6.3 影子写不进写缓冲（I12）：故障应直返，不入队 ==\n";
    // 影子叶在失败注入下下应直接返回错误，不应把"压测写"补发到生产主。
    // 由于同步 dispatchWrite 在影子短路里根本不构造 buffered lambda，
    // 这一用例只能通过"影子失败 → 生产主未收到 execute"间接验证：
    //   - 注入 FailingShadowConnection 在前 1 次返回可重试连接失败
    //   - 影子短路直接 attempt 影子，影子失败 → 返回
    //   - 主叶 MockPrimaryConnection::execCount 应保持 0
    core::DatabaseManager mgr;
    config::DataSourceConfig pp;
    pp.name = "prod"; pp.type = "mockp"; pp.host = "localhost";
    mgr.addDataSource(pp);
    config::DataSourceConfig sh;
    sh.name = "shadow_ds"; sh.type = "mocks_fail"; sh.host = "localhost";
    mgr.addDataSource(sh);
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "prod";
    grp.shadow = "shadow_ds";
    // 启用写缓冲——如果 I12 守卫漏掉，影子失败会触发入队。
    grp.failover.write_buffer.enabled = true;
    grp.failover.write_buffer.acknowledge_data_loss_and_duplicates = true;
    grp.failover.write_buffer.max_queue = 10;
    grp.failover.write_buffer.ttl_ms = 5000;
    grp.failover.write_buffer.flush_interval_ms = 1000;
    core::GroupOptions gopts;
    gopts.acknowledge_data_loss_and_duplicates = true;
    mgr.addGroup(grp, gopts);

    auto g = mgr.getDataSource("grp");
    resetCounters();
    gShadowFailRemaining.store(1, std::memory_order_release);
    std::int64_t aff = 0;
    common::Status st;
    {
        common::SqlContext shadowContext;
        shadowContext.shadow = true;
        common::ContextScope scope(shadowContext);
        st = g->execute("INSERT INTO t VALUES (1)", aff);
    }
    check(!st.ok(), "影子写失败：返回失败");
    check(MockPrimaryConnection::execCount.load() == 0,
          "影子写失败：主叶未收到任何 execute（I12 阻止入队）");
    check(MockShadowConnection::execCount.load() == 0,
          "影子写失败：影子叶也未在失败注入后执行（短路直接返回）");

    mgr.shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// M6.4 影子读不进查询缓存。
// 配 query_cache=true，触发同一查询两次：第一次未走影子（命中缓存），
// 第二次走影子（cacheLookup 内部短路 → cacheStore 不会写）。
// ---------------------------------------------------------------------------
static void test_shadow_no_cache() {
    std::cout << "== M6.4 影子读不进查询缓存 ==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig pp;
    pp.name = "prod"; pp.type = "mockp"; pp.host = "localhost";
    mgr.addDataSource(pp);
    config::DataSourceConfig sh;
    sh.name = "shadow_ds"; sh.type = "mocks"; sh.host = "localhost";
    mgr.addDataSource(sh);
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "prod";
    grp.shadow = "shadow_ds";
    mgr.addGroup(grp);
    // 启用查询缓存（仅叶子命中；组不缓存——既有不变量）。
    config::QueryCacheConfig qc_on;
    qc_on.enabled = true;
    core::QueryCache::configure(qc_on);

    auto g = mgr.getDataSource("grp");
    resetCounters();
    common::ResultSet rs1, rs2;
    {
        // 第一次：影子触发——cacheLookup 内部短路，不应命中任何缓存（短路返回 false）。
        // 影子叶被实际调用一次。
        common::SqlContext shadowContext;
        shadowContext.shadow = true;
        common::ContextScope scope(shadowContext);
        check(g->query("SELECT 'cached'", rs1).ok(), "首次（影子）：成功");
    }
    {
        // 第二次：非影子，应该按主叶读——并且不该命中影子刚才的缓存（即 I10）。
        // 由于 cache 严格按目标名（prod / shadow_ds）分键，影子写进
        // "shadow_ds:..." 的项不会被主叶（prod）的 lookup 命中——
        // 所以主叶再次被调用一次。这是预期的隔离行为。
        common::ContextScope scope({});  // shadow 默认 false
        check(g->query("SELECT 'cached'", rs2).ok(), "再次（非影子）：成功");
    }
    // I10 验证：影子写不进"主叶缓存"。所以两次之后：主叶 1 次、影子叶 1 次。
    // 若影子直接污染主缓存，主叶第二次就会被 cacheLookup 短路（0 次）。
    check(MockPrimaryConnection::queryCount.load() == 1,
          "I10：主叶 query 1 次（未命中影子写过的缓存）");
    check(MockShadowConnection::queryCount.load() == 1,
          "影子读：影子叶 query 1 次");

    // 查询缓存只有 configure（带 enabled/replica_only/max_entries/ttl/max_key）。
    // 关闭它以隔离：configure({}) 中 enabled 默认 false。
    config::QueryCacheConfig qc_off{};
    core::QueryCache::configure(qc_off);
    mgr.shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// M6.5/6/7/8 校验失败用例——通过 DatabaseManager::addDataSource + addGroup 后
// 由 resolveShadows 校验；任一不合法返回 ConfigError。
// ---------------------------------------------------------------------------
static void test_validation_unknown_shadow() {
    std::cout << "== M6.5 校验：影子源不存在 ==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig pp;
    pp.name = "prod"; pp.type = "mockp"; pp.host = "localhost";
    mgr.addDataSource(pp);
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "prod";
    grp.shadow = "ghost";  // 不在 datasources 里
    const auto st = mgr.addGroup(grp);
    check(!st.ok(), "addGroup(grp, shadow=ghost) 返回错误");
    check(st.code == ErrorCode::ConfigError, "错误码 = ConfigError");
    check(st.message.find("ghost") != std::string::npos, "错误消息含影子名");
    mgr.shutdown(std::chrono::milliseconds(0));
}

static void test_validation_self_primary() {
    std::cout << "== M6.6 校验：影子源 = 本组主（自影自己） ==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig pp;
    pp.name = "prod"; pp.type = "mockp"; pp.host = "localhost";
    mgr.addDataSource(pp);
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "prod";
    grp.shadow = "prod";  // = 主
    const auto st = mgr.addGroup(grp);
    check(!st.ok(), "addGroup(grp, shadow=主名) 返回错误");
    check(st.code == ErrorCode::ConfigError, "错误码 = ConfigError");
    check(st.message.find("primary") != std::string::npos,
          "错误消息提及 primary（自影自己被拒绝）");
    mgr.shutdown(std::chrono::milliseconds(0));
}

static void test_validation_self_replica() {
    std::cout << "== M6.7 校验：影子源 = 本组副本 ==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig pp;
    pp.name = "prod"; pp.type = "mockp"; pp.host = "localhost";
    mgr.addDataSource(pp);
    config::DataSourceConfig rp;
    rp.name = "r1"; rp.type = "mockp"; rp.host = "localhost";
    mgr.addDataSource(rp);
    config::DataSourceGroupConfig grp;
    grp.name = "grp";
    grp.primary = "prod";
    config::ReplicaConfig rc;
    rc.name = "r1"; rc.weight = 1;
    grp.replicas.push_back(rc);
    grp.shadow = "r1";  // = 副本
    const auto st = mgr.addGroup(grp);
    check(!st.ok(), "addGroup(grp, shadow=副本) 返回错误");
    check(st.code == ErrorCode::ConfigError, "错误码 = ConfigError");
    check(st.message.find("replica") != std::string::npos,
          "错误消息提及 replica");
    mgr.shutdown(std::chrono::milliseconds(0));
}

static void test_validation_shadow_is_group() {
    std::cout << "== M6.8 校验：影子源 = 另一个组名 ==\n";
    core::DatabaseManager mgr;
    config::DataSourceConfig pp;
    pp.name = "prod"; pp.type = "mockp"; pp.host = "localhost";
    mgr.addDataSource(pp);
    config::DataSourceConfig sh;
    sh.name = "shadow_ds"; sh.type = "mocks"; sh.host = "localhost";
    mgr.addDataSource(sh);
    config::DataSourceGroupConfig grp_a;
    grp_a.name = "grp"; grp_a.primary = "prod";
    mgr.addGroup(grp_a);
    // 第二个组把"grp"当作为子——应被拒绝（组不可直接作影子目标）。
    config::DataSourceGroupConfig grp_b;
    grp_b.name = "grp_b"; grp_b.primary = "shadow_ds";
    grp_b.shadow = "grp";
    const auto st = mgr.addGroup(grp_b);
    check(!st.ok(), "addGroup(grp_b, shadow=grp) 返回错误");
    check(st.code == ErrorCode::ConfigError, "错误码 = ConfigError");
    check(st.message.find("group") != std::string::npos,
          "错误消息提及 group（组不可作影子）");
    mgr.shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// M6.9 异步路径：onRoute 置 shadow → async::execute 落到影子叶，写缓冲不入队。
// ---------------------------------------------------------------------------
static void test_async_shadow_write() {
    std::cout << "== M6.9 异步路径：onRoute 置 shadow → 异步 execute 落到影子叶 ==\n";
    const auto path = (std::filesystem::temp_directory_path() /
                       "dbmw_shadow_async.json").string();
    std::ofstream(path) << R"({
  "default_datasource": "grp",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 4, "borrow_timeout_ms": 2000 },
  "retry": { "max_attempts": 1, "retry_writes": true },
  "circuit_breaker": { "failure_threshold": 0 },
  "rate_limit": { "enabled": false },
  "sql_audit": { "enabled": false },
  "query_cache": { "enabled": false },
  "async": { "enabled": true, "threads": 1, "queue_size": 64 },
  "datasources": [
    { "name": "prod", "type": "mockp", "host": "localhost" },
    { "name": "shadow_ds", "type": "mocks", "host": "localhost" }
  ],
  "groups": [
    { "name": "grp", "primary": "prod", "shadow": "shadow_ds" }
  ]
})";
    check(DBMW::init(path).ok(), "DBMW::init(async cfg) ok");
    resetCounters();

    std::promise<async::ExecResult> pr;
    auto fut = pr.get_future();
    common::SqlContext shadowContext;
    shadowContext.shadow = true;
    common::ContextScope scope(shadowContext);
    async::execute("grp", "INSERT INTO t VALUES (1)",
                   [&](async::ExecResult &&r) { pr.set_value(std::move(r)); });
    const auto out = fut.get();
    check(out.status.ok(), "异步 shadow=true：execute 成功");
    check(MockShadowConnection::execCount.load() == 1,
          "异步 shadow=true：影子叶 execCount = 1");
    check(MockPrimaryConnection::execCount.load() == 0,
          "异步 shadow=true：主叶 execCount = 0（路由到影子）");

    DBMW::shutdown(std::chrono::milliseconds(0));
}

// ---------------------------------------------------------------------------
// M6.10 异步读：onRoute 置 shadow → async::query 落到影子叶；不进缓存。
// ---------------------------------------------------------------------------
static void test_async_shadow_query() {
    std::cout << "== M6.10 异步路径：onRoute 置 shadow → async::query 落到影子叶 ==\n";
    const auto path = (std::filesystem::temp_directory_path() /
                       "dbmw_shadow_async_q.json").string();
    std::ofstream(path) << R"({
  "default_datasource": "grp",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 4, "borrow_timeout_ms": 2000 },
  "retry": { "max_attempts": 1, "retry_writes": true },
  "circuit_breaker": { "failure_threshold": 0 },
  "rate_limit": { "enabled": false },
  "sql_audit": { "enabled": false },
  "query_cache": { "enabled": false },
  "async": { "enabled": true, "threads": 1, "queue_size": 64 },
  "datasources": [
    { "name": "prod", "type": "mockp", "host": "localhost" },
    { "name": "shadow_ds", "type": "mocks", "host": "localhost" }
  ],
  "groups": [
    { "name": "grp", "primary": "prod", "shadow": "shadow_ds" }
  ]
})";
    check(DBMW::init(path).ok(), "DBMW::init(async query cfg) ok");
    resetCounters();

    std::promise<async::QueryResult> pr;
    auto fut = pr.get_future();
    common::SqlContext shadowContext;
    shadowContext.shadow = true;
    common::ContextScope scope(shadowContext);
    async::query("grp", "SELECT 1",
                 [&](async::QueryResult &&r) { pr.set_value(std::move(r)); });
    const auto out = fut.get();
    check(out.status.ok(), "异步 shadow=true：query 成功");
    check(MockShadowConnection::queryCount.load() == 1,
          "异步 shadow=true：影子叶 queryCount = 1");
    check(MockPrimaryConnection::queryCount.load() == 0,
          "异步 shadow=true：主叶 queryCount = 0");

    DBMW::shutdown(std::chrono::milliseconds(0));
}

int main() {
    driver::DriverRegistry::instance().registerDriver(
        "mockp", [] { return std::make_unique<MockPrimaryDriver>(); });
    driver::DriverRegistry::instance().registerDriver(
        "mocks", [] { return std::make_unique<MockShadowDriver>(); });
    driver::DriverRegistry::instance().registerDriver(
        "mocks_fail", [] { return std::make_unique<FailingShadowDriver>(); });

    test_sync_shadow_read();
    test_sync_shadow_write();
    test_shadow_no_write_buffer();
    test_shadow_no_cache();
    test_validation_unknown_shadow();
    test_validation_self_primary();
    test_validation_self_replica();
    test_validation_shadow_is_group();
    test_async_shadow_write();
    test_async_shadow_query();

    std::cout << "\n========== M6 影子库路由 总计: " << g_passed << " 通过 / "
              << g_failed << " 失败 ==========\n";
    return g_failed == 0 ? 0 : 1;
}
