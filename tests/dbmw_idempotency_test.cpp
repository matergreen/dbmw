// dbmw v0.4.0 M5 单测：幂等声明（Idempotency）叠加到重试决策。
//
// 覆盖（docs/roadmap-design-v0.4.0.md §7）：
//   1. Unspecified + retry_writes=true  → 走既有逻辑（失败重试到 max_attempts）。
//   2. Unspecified + retry_writes=true + 失败数超过 max → 耗尽后返回失败。
//   3. NonIdempotent + retry_writes=true → 覆盖配置，绝不重试（仅 1 次）。
//   4. Idempotent + retry_writes=false   → 覆盖配置，允许重试（到 max_attempts）。
//   5. Idempotent + retry_writes=false + 失败超 max → 耗尽后失败。
//   6. NonIdempotent + 可重试错误         → 仍仅 1 次（声明压制重试语义一致）。
//   7. 读路径不受影响：NonIdempotent 声明下 query 仍按 max_attempts 重试。
//   8. 异步路径：Idempotent + retry_writes=false → 重试到 max_attempts。
//   9. 异步路径：NonIdempotent + retry_writes=false → 仅 1 次。
//
// 无真实数据库依赖，全部走 mock 驱动；通过"注入 N 次可重试失败 + 数 execute
// 调用次数"验证重试决策，直接命中 resolveWriteAttempts / maxAttempts 的真实分支。
#include "dbmw/dbmw.h"
#include "dbmw/async/dbmw_async.h"
#include "dbmw/core/connection_pool.h"
#include "dbmw/core/database_manager.h"
#include "dbmw/core/idatabase_connection.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/driver/driver_registry.h"
#include "dbmw/common/context.h"

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
#include <vector>

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
// Mock 驱动：支持"注入 N 次可重试连接失败"，并分别统计 execute / query 调用次数。
// 这是验证重试决策的最小可控手段——直接数驱动层被调几次即可反推 attempts。
// ---------------------------------------------------------------------------
class MockConnection : public core::IDatabaseConnection {
public:
    static std::atomic<int> alive;
    static std::atomic<int> execCount;
    static std::atomic<int> queryCount;
    static std::atomic<int> execFailRemaining;   // 前 N 次 execute 返回可重试错误
    static std::atomic<int> queryFailRemaining;  // 前 N 次 query 返回可重试错误
    static std::atomic<bool> execFailNonRetryable;// true=注入的错误不可重试

    common::Status connect(const config::DataSourceConfig &) override {
        open_ = true;
        ++alive;
        return Status::OK();
    }
    common::Status ping() override {
        return open_ ? Status::OK()
                     : Status::error(common::ErrorCode::NotConnected, "closed");
    }
    common::Status query(const std::string &sql, common::ResultSet &out) override {
        ++queryCount;
        if (queryFailRemaining > 0) {
            --queryFailRemaining;
            return makeRetryable(ErrorCode::NotConnected, "mock query broke");
        }
        common::Row r;
        r.set("echo", std::string(sql));
        out.addRow(std::move(r));
        return Status::OK();
    }
    common::Status execute(const std::string &, std::int64_t &affected) override {
        ++execCount;
        if (execFailRemaining > 0) {
            --execFailRemaining;
            if (execFailNonRetryable.load()) {
                affected = 0;
                return Status::error(ErrorCode::Unknown, "mock non-retryable fail");
            }
            return makeRetryable(ErrorCode::NotConnected, "mock broke");
        }
        affected = 1;
        return Status::OK();
    }
    common::Status begin() override {
        return open_ ? Status::OK() : Status::error(ErrorCode::NotConnected, "closed");
    }
    common::Status commit() override {
        return open_ ? Status::OK() : Status::error(ErrorCode::NotConnected, "closed");
    }
    common::Status rollback() override {
        return open_ ? Status::OK() : Status::error(ErrorCode::NotConnected, "closed");
    }
    void close() override { open_ = false; --alive; }
    bool isOpen() const override { return open_; }

private:
    static Status makeRetryable(ErrorCode c, const char *msg) {
        auto st = Status::error(c, msg);
        st.retryable = true;
        st.connectionBroken = true;
        return st;
    }
    bool open_ = false;
};

std::atomic<int> MockConnection::alive{0};
std::atomic<int> MockConnection::execCount{0};
std::atomic<int> MockConnection::queryCount{0};
std::atomic<int> MockConnection::execFailRemaining{0};
std::atomic<int> MockConnection::queryFailRemaining{0};
std::atomic<bool> MockConnection::execFailNonRetryable{false};

class MockDriver : public driver::IDriver {
public:
    const char *name() const override { return "mock"; }
    std::unique_ptr<core::IDatabaseConnection> createConnection() override {
        return std::make_unique<MockConnection>();
    }
};

// 每个场景前重置：活连接 / 计数 / 失败注入。
static void resetMock(int execFails = 0, int queryFails = 0, bool nonRetry = false) {
    MockConnection::alive = 0;
    MockConnection::execCount = 0;
    MockConnection::queryCount = 0;
    MockConnection::execFailRemaining = execFails;
    MockConnection::queryFailRemaining = queryFails;
    MockConnection::execFailNonRetryable = nonRetry;
}

static config::DataSourceConfig mockLeafCfg(const std::string &name) {
    config::DataSourceConfig c;
    c.name = name;
    c.type = "mock";
    c.host = "localhost";
    c.connection_timeout_ms = 100;
    return c;
}

// retry_writes / max_attempts 可配；熔断一律关闭，隔离重试测试。
static core::DataSourceOptions retryOpts(bool retryWrites, int maxAttempts,
                                         int backoffMs = 0) {
    core::DataSourceOptions o;
    o.retry.retry_writes = retryWrites;
    o.retry.max_attempts = maxAttempts;
    o.retry.initial_backoff_ms = backoffMs;
    o.retry.max_backoff_ms = backoffMs;
    o.circuit_breaker.failure_threshold = 0;
    return o;
}

// ---------------------------------------------------------------------------
int main() {
    driver::DriverRegistry::instance().registerDriver(
        "mock", [] { return std::make_unique<MockDriver>(); });

    // ------------------------------------------------------------------
    std::cout << "== M5.1 Unspecified + retry_writes=true：走既有逻辑（失败重试到 max）==\n";
    {
        core::DatabaseManager mgr;
        mgr.addDataSource(mockLeafCfg("ds"), retryOpts(/*retryWrites=*/true, /*max=*/3));
        auto ds = mgr.getDataSource("ds");
        resetMock(/*execFails=*/2);
        // 不推 ContextScope → idempotency = Unspecified（default）。
        std::int64_t aff = 0;
        const auto st = ds->execute("INSERT INTO t VALUES (1)", aff);
        check(st.ok(), "Unspecified + retry_writes=true：最终成功");
        check(MockConnection::execCount.load() == 3,
              "执行 3 次（2 次失败 + 1 次成功）");
        mgr.shutdown(std::chrono::milliseconds(0));
    }

    // ------------------------------------------------------------------
    std::cout << "== M5.2 Unspecified + retry_writes=true + 失败超 max：耗尽返回失败 ==\n";
    {
        core::DatabaseManager mgr;
        mgr.addDataSource(mockLeafCfg("ds"), retryOpts(/*retryWrites=*/true, /*max=*/3));
        auto ds = mgr.getDataSource("ds");
        resetMock(/*execFails=*/5);
        std::int64_t aff = 0;
        const auto st = ds->execute("INSERT INTO t VALUES (1)", aff);
        check(!st.ok(), "Unspecified + retry_writes=true：失败耗尽");
        check(MockConnection::execCount.load() == 3,
              "执行恰好 3 次（受 max_attempts 上限）");
        mgr.shutdown(std::chrono::milliseconds(0));
    }

    // ------------------------------------------------------------------
    std::cout << "== M5.3 NonIdempotent + retry_writes=true：覆盖配置，绝不重试 ==\n";
    {
        core::DatabaseManager mgr;
        mgr.addDataSource(mockLeafCfg("ds"), retryOpts(/*retryWrites=*/true, /*max=*/3));
        auto ds = mgr.getDataSource("ds");
        resetMock(/*execFails=*/5);
        common::ContextScope scope({.idempotency = common::Idempotency::NonIdempotent});
        std::int64_t aff = 0;
        const auto st = ds->execute("INSERT INTO t VALUES (1)", aff);
        check(!st.ok(), "NonIdempotent：失败（未重试）");
        check(MockConnection::execCount.load() == 1,
              "仅执行 1 次（声明覆盖 retry_writes=true）");
        mgr.shutdown(std::chrono::milliseconds(0));
    }

    // ------------------------------------------------------------------
    std::cout << "== M5.4 Idempotent + retry_writes=false：覆盖配置，允许重试写 ==\n";
    {
        core::DatabaseManager mgr;
        mgr.addDataSource(mockLeafCfg("ds"), retryOpts(/*retryWrites=*/false, /*max=*/3));
        auto ds = mgr.getDataSource("ds");
        resetMock(/*execFails=*/2);
        common::ContextScope scope({.idempotency = common::Idempotency::Idempotent});
        std::int64_t aff = 0;
        const auto st = ds->execute("INSERT INTO t VALUES (1)", aff);
        check(st.ok(), "Idempotent + retry_writes=false：重试后成功");
        check(MockConnection::execCount.load() == 3,
              "执行 3 次（声明允许重试，覆盖 retry_writes=false）");
        mgr.shutdown(std::chrono::milliseconds(0));
    }

    // ------------------------------------------------------------------
    std::cout << "== M5.5 Idempotent + retry_writes=false + 失败超 max：耗尽失败 ==\n";
    {
        core::DatabaseManager mgr;
        mgr.addDataSource(mockLeafCfg("ds"), retryOpts(/*retryWrites=*/false, /*max=*/3));
        auto ds = mgr.getDataSource("ds");
        resetMock(/*execFails=*/5);
        common::ContextScope scope({.idempotency = common::Idempotency::Idempotent});
        std::int64_t aff = 0;
        const auto st = ds->execute("INSERT INTO t VALUES (1)", aff);
        check(!st.ok(), "Idempotent：失败耗尽");
        check(MockConnection::execCount.load() == 3,
              "执行恰好 3 次（受 max_attempts 上限）");
        mgr.shutdown(std::chrono::milliseconds(0));
    }

    // ------------------------------------------------------------------
    std::cout << "== M5.6 NonIdempotent + 可重试错误：仍仅 1 次 ==\n";
    {
        core::DatabaseManager mgr;
        mgr.addDataSource(mockLeafCfg("ds"), retryOpts(/*retryWrites=*/true, /*max=*/3));
        auto ds = mgr.getDataSource("ds");
        resetMock(/*execFails=*/5);
        common::ContextScope scope({.idempotency = common::Idempotency::NonIdempotent});
        std::int64_t aff = 0;
        const auto st = ds->execute("INSERT INTO t VALUES (1)", aff);
        check(!st.ok(), "NonIdempotent + 可重试错误：失败（不重试）");
        check(MockConnection::execCount.load() == 1,
              "仅执行 1 次（非可重试错误本就不重试，声明语义一致）");
        mgr.shutdown(std::chrono::milliseconds(0));
    }

    // ------------------------------------------------------------------
    std::cout << "== M5.7 读路径不受影响：NonIdempotent 下 query 仍按 max 重试 ==\n";
    {
        core::DatabaseManager mgr;
        mgr.addDataSource(mockLeafCfg("ds"), retryOpts(/*retryWrites=*/false, /*max=*/3));
        auto ds = mgr.getDataSource("ds");
        resetMock(/*execFails=*/0, /*queryFails=*/2);
        common::ResultSet rs;
        const auto st = ds->query("SELECT 1", rs);
        check(st.ok(), "NonIdempotent：query 重试后成功（读不受声明影响）");
        check(MockConnection::queryCount.load() == 3,
              "query 执行 3 次（读路径忽略 idempotency，照常重试）");
        mgr.shutdown(std::chrono::milliseconds(0));
    }

    // ------------------------------------------------------------------
    // 异步路径：经 DBMW::init 启用内置执行器，声明经 ContextScope 快照进入
    // async_engine 的 maxAttempts（与同步 resolveWriteAttempts 同源优先级表）。
    std::cout << "== M5.8/9 异步路径：声明叠加到 maxAttempts ==\n";
    {
        const auto path = (std::filesystem::temp_directory_path() /
                           "dbmw_idem_async.json").string();
        std::ofstream(path) << R"({
  "default_datasource": "main",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 4, "borrow_timeout_ms": 2000 },
  "retry": { "max_attempts": 3, "initial_backoff_ms": 1, "max_backoff_ms": 5, "retry_writes": false },
  "circuit_breaker": { "failure_threshold": 0 },
  "rate_limit": { "enabled": false },
  "sql_audit": { "enabled": false },
  "query_cache": { "enabled": false },
  "async": { "enabled": true, "threads": 1, "queue_size": 64 },
  "datasources": [ { "name": "main", "type": "mock", "host": "localhost" } ],
  "groups": []
})";
        check(DBMW::init(path).ok(), "DBMW::init(async cfg) ok");

        // M5.8：Idempotent + retry_writes=false → 应重试到 max_attempts。
        resetMock(/*execFails=*/2);
        {
            std::promise<async::ExecResult> pr;
            auto fut = pr.get_future();
            common::ContextScope scope({.idempotency = common::Idempotency::Idempotent});
            async::execute("main", "INSERT INTO t VALUES (1)",
                           [&](async::ExecResult &&r) { pr.set_value(std::move(r)); });
            const auto out = fut.get();
            check(out.status.ok(), "异步 Idempotent：重试后成功");
        }
        check(MockConnection::execCount.load() == 3,
              "异步 Idempotent 执行 3 次（覆盖 retry_writes=false）");

        // M5.9：NonIdempotent + retry_writes=false → 仅 1 次。
        resetMock(/*execFails=*/5);
        {
            std::promise<async::ExecResult> pr;
            auto fut = pr.get_future();
            common::ContextScope scope({.idempotency = common::Idempotency::NonIdempotent});
            async::execute("main", "INSERT INTO t VALUES (1)",
                           [&](async::ExecResult &&r) { pr.set_value(std::move(r)); });
            const auto out = fut.get();
            check(!out.status.ok(), "异步 NonIdempotent：不重试（失败）");
        }
        check(MockConnection::execCount.load() == 1,
              "异步 NonIdempotent 仅执行 1 次");

        DBMW::shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "\n========== M5 幂等声明 总计: " << g_passed << " 通过 / "
              << g_failed << " 失败 ==========\n";
    return g_failed == 0 ? 0 : 1;
}
