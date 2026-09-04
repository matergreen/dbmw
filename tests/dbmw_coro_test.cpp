// dbmw v0.2.0 协程层验证（设计 §14：T15，仅在 DBMW_ENABLE_ASYNC_CORO=ON 时构建）。
//
// 复用 dbmw_async_test 的 mock 驱动手法，验证协程层相对回调层的增量语义：
//   - Task 惰性启动 / 安全放弃（R5）
//   - run() 受控 fire-and-forget（跑完自毁，不悬垂）
//   - 协程恢复线程 = 完成调度器线程（I1 的协程版）
//   - 超时 / 治理 / 事务 / 生命周期与回调形态完全同源（零额外语义）
//   - 异常沿 continuation 链传播；detached 协程异常 = terminate（文档化，
//     本测试只验证前半段）
//
// 运行顺序有依赖：C6（生命周期）必须最后——它会 shutdown 掉全局引擎。
#include "dbmw/dbmw.h"
#include "dbmw/async/task.h"
#include "dbmw/core/idatabase_connection.h"
#include "dbmw/driver/driver_registry.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace dbmw;
using common::Status;

static int g_failed = 0;
static int g_passed = 0;

static void check(bool cond, const std::string &name) {
    if (cond) { ++g_passed; std::cout << "  [PASS] " << name << "\n"; }
    else { ++g_failed; std::cout << "  [FAIL] " << name << "\n"; }
}

// ---------------------------------------------------------------------------
// Mock 驱动（与 dbmw_async_test 的 AsyncMockConnection 同一套路）
// ---------------------------------------------------------------------------
class CoroMockConnection : public core::IDatabaseConnection {
public:
    static std::atomic<int> connectCalls;
    static std::atomic<int> queryCalls;
    static std::atomic<int> executeCalls;
    static std::atomic<int> cancelCalls;
    static std::atomic<int> queryDelayMs;
    static std::atomic<bool> queryEntered;
    static std::vector<std::string> log;
    static std::mutex logMtx;

    static void resetLog() {
        std::lock_guard<std::mutex> lk(logMtx);
        log.clear();
    }

    static bool logHas(const std::string &needle) {
        std::lock_guard<std::mutex> lk(logMtx);
        for (const auto &x: log) if (x.find(needle) != std::string::npos) return true;
        return false;
    }

    Status connect(const config::DataSourceConfig &cfg) override {
        (void) cfg;
        ++connectCalls;
        open_ = true;
        return Status::OK();
    }

    Status ping() override {
        return open_ ? Status::OK()
                     : Status::error(common::ErrorCode::PingFailed, "closed");
    }

    Status query(const std::string &sql, common::ResultSet &out) override {
        if (!open_) return Status::error(common::ErrorCode::NotConnected, "closed");
        ++queryCalls;
        queryEntered.store(true, std::memory_order_release);
        if (const int d = queryDelayMs.load(); d > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(d));
        {
            std::lock_guard<std::mutex> lk(logMtx);
            log.push_back("query:" + sql);
        }
        common::Row r;
        r.set("echo", std::string(sql));
        out.addRow(std::move(r));
        return Status::OK();
    }

    Status execute(const std::string &sql, std::int64_t &affected) override {
        if (!open_) return Status::error(common::ErrorCode::NotConnected, "closed");
        ++executeCalls;
        {
            std::lock_guard<std::mutex> lk(logMtx);
            log.push_back("execute:" + sql);
        }
        affected = 1;
        return Status::OK();
    }

    Status begin() override {
        if (tx_) return Status::error(common::ErrorCode::TxError, "already in tx");
        tx_ = true;
        std::lock_guard<std::mutex> lk(logMtx);
        log.push_back("begin");
        return Status::OK();
    }

    Status begin(const common::TransactionOptions &options) override {
        return begin();
    }

    Status commit() override {
        if (!tx_) return Status::error(common::ErrorCode::TxError, "no tx");
        tx_ = false;
        std::lock_guard<std::mutex> lk(logMtx);
        log.push_back("commit");
        return Status::OK();
    }

    Status rollback() override {
        if (!tx_) return Status::error(common::ErrorCode::TxError, "no tx");
        tx_ = false;
        std::lock_guard<std::mutex> lk(logMtx);
        log.push_back("rollback");
        return Status::OK();
    }

    Status cancel() override {
        ++cancelCalls;
        return Status::OK();
    }

    void close() override { open_ = false; }

    bool isOpen() const override { return open_; }
    bool inTransaction() const override { return tx_; }
    bool allowsLiteralInterpolation() const override { return true; }

private:
    bool open_ = false;
    bool tx_ = false;
};

std::atomic<int> CoroMockConnection::connectCalls{0};
std::atomic<int> CoroMockConnection::queryCalls{0};
std::atomic<int> CoroMockConnection::executeCalls{0};
std::atomic<int> CoroMockConnection::cancelCalls{0};
std::atomic<int> CoroMockConnection::queryDelayMs{0};
std::atomic<bool> CoroMockConnection::queryEntered{false};
std::vector<std::string> CoroMockConnection::log;
std::mutex CoroMockConnection::logMtx;

class CoroMockDriver : public driver::IDriver {
public:
    const char *name() const override { return "cmock"; }

    std::unique_ptr<core::IDatabaseConnection> createConnection() override {
        return std::make_unique<CoroMockConnection>();
    }
};

// ---------------------------------------------------------------------------
// 配置与工具
// ---------------------------------------------------------------------------
struct CfgFlags {
    bool auditBlock = false;
};

static std::string buildConfig(const CfgFlags &f) {
    std::string audit = f.auditBlock
        ? R"("sql_audit": { "enabled": true, "action": "block", "block_no_where_dml": true, "enforce_read_only": true },)"
        : R"("sql_audit": { "enabled": false },)";
    return R"({
  "default_datasource": "main",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 4, "borrow_timeout_ms": 2000 },
  "retry": { "max_attempts": 3, "initial_backoff_ms": 20, "max_backoff_ms": 20, "retry_writes": false },
  )" + audit + R"(
  "query_cache": { "enabled": false },
  "circuit_breaker": { "failure_threshold": 0 },
  "rate_limit": { "enabled": false },
  "async": { "enabled": true, "threads": 1, "queue_size": 64 },
  "groups": [],
  "datasources": [
    { "name": "main", "type": "cmock", "host": "localhost" }
  ]
}
)";
}

static std::string g_configPath;

static void applyConfig(const CfgFlags &f) {
    std::ofstream(g_configPath) << buildConfig(f);
}

static bool waitUntil(const std::function<bool()> &pred, int timeoutMs = 5000) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return pred();
}

template <class R>
static bool awaitFuture(std::future<R> &f, R &out, int timeoutMs = 5000) {
    if (f.wait_for(std::chrono::milliseconds(timeoutMs)) == std::future_status::ready) {
        out = f.get();
        return true;
    }
    return false;
}

// ---------------------------------------------------------------------------
// 被测协程（注意：必须是具名函数，不能用带捕获的 lambda——闭包临时对象
// 在 full-expression 结束即销毁，异步恢复后捕获将悬垂，是经典 UB）
// ---------------------------------------------------------------------------

// C1/C3：单协程串行多个操作，记录恢复线程与完成顺序。
static async::Task<void> pipelineBody(std::promise<std::vector<std::string>> pr) {
    std::vector<std::string> trace;
    // 帧连续性探针：局部变量必须跨 co_await 挂起/恢复完整存活。
    int frameMarker = 42;

    auto q = co_await async::queryAsync("SELECT c1");
    trace.push_back(std::string("q:") + (q.status.ok() ? "ok" : "fail") +
                    ":" + std::get<std::string>(q.rows.rows()[0].at("echo")));
    trace.push_back(frameMarker == 42 ? "frame-intact" : "frame-corrupted");

    auto e = co_await async::executeAsync("UPDATE t SET v = 1");
    trace.push_back(std::string("e:") + (e.status.ok() ? "ok" : "fail") +
                    ":affected=" + std::to_string(e.affected));

    common::ParamBatch batch;
    batch.emplace_back();
    batch.back().push_back(common::Value(std::int64_t(1)));
    batch.emplace_back();
    batch.back().push_back(common::Value(std::int64_t(2)));
    auto b = co_await async::executeBatchAsync("UPDATE t SET v = ?", batch);
    trace.push_back(std::string("b:") + (b.status.ok() ? "ok" : "fail") +
                    ":total=" + std::to_string(b.batch.totalAffected()));

    pr.set_value(std::move(trace));
}

// C1：恢复线程断言——co_await 之后记录当前线程。
static async::Task<void> tidBody(std::promise<std::thread::id> pr) {
    co_await async::queryAsync("SELECT tid");
    pr.set_value(std::this_thread::get_id());
}

// C5：审计拦截路径。
static async::Task<void> blockedBody(std::promise<async::QueryResult> pr) {
    auto r = co_await async::queryAsync("DELETE FROM t");
    pr.set_value(std::move(r));
}

// C8：显式数据源重载。
static async::Task<void> dsBody(std::promise<async::QueryResult> pr) {
    auto r = co_await async::queryAsync("main", "SELECT ds", {});
    pr.set_value(std::move(r));
}

// C4：语句超时。
static async::Task<void> timeoutBody(std::promise<async::QueryResult> pr) {
    async::Options opts;
    opts.timeout = std::chrono::milliseconds(80);
    auto r = co_await async::queryAsync("SELECT slow", {}, opts);
    pr.set_value(std::move(r));
}

// C6：事务（提交 / 回滚两条路径）。
static async::Task<void> txBody(common::TransactionOptions txOpts, bool failFn,
                                std::promise<async::OpResult> pr) {
    auto r = co_await async::transactionAsync(txOpts, [failFn](core::Session &s) -> Status {
        std::int64_t affected = 0;
        if (const auto st = s.execute("UPDATE t SET v = 1", affected); !st.ok())
            return st;
        if (failFn) return Status::error(common::ErrorCode::QueryError, "biz fail");
        return Status::OK();
    });
    pr.set_value(std::move(r));
}

// C7：异常传播——内层抛、外层接。
static async::Task<void> thrower() {
    auto r = co_await async::queryAsync("SELECT before-throw");
    if (!r.status.ok()) throw std::runtime_error("op failed");
    throw std::runtime_error("boom");
}

static async::Task<void> catcher(std::promise<std::string> pr) {
    try {
        co_await thrower();
        pr.set_value("no-exception");
    } catch (const std::runtime_error &e) {
        pr.set_value(std::string("caught:") + e.what());
    }
}

// C6 生命周期：在途慢语句 + shutdown 排水。
static async::Task<void> drainBody(std::promise<async::QueryResult> pr) {
    auto r = co_await async::queryAsync("SELECT drain");
    pr.set_value(std::move(r));
}

// C6 生命周期：shutdown 之后的新协程任务。
static async::Task<void> afterShutdownBody(std::promise<async::QueryResult> pr) {
    auto r = co_await async::queryAsync("SELECT after");
    pr.set_value(std::move(r));
}

// ---------------------------------------------------------------------------
int main() {
    g_configPath = (std::filesystem::temp_directory_path() /
                    "dbmw_coro_test.json").string();

    driver::DriverRegistry::instance().registerDriver(
        "cmock", [] { return std::make_unique<CoroMockDriver>(); });

    applyConfig(CfgFlags{});
    if (!DBMW::init(g_configPath).ok()) {
        std::cout << "init failed\n";
        return 1;
    }

    // =====================================================================
    std::cout << "== C1. 基础链路：run + co_await，恢复线程 = 完成调度器线程 ==\n";
    {
        std::promise<std::vector<std::string>> pr;
        auto fut = pr.get_future();
        const auto callerTid = std::this_thread::get_id();
        CoroMockConnection::queryCalls = 0;
        CoroMockConnection::executeCalls = 0;

        async::run(pipelineBody(std::move(pr)));

        std::vector<std::string> trace;
        check(awaitFuture(fut, trace), "协程任务完成并交付结果");
        check(trace.size() == 4, "协程体内 3 个操作 + 1 条帧连续性断言全部执行");
        if (trace.size() == 4) {
            check(trace[0] == "q:ok:SELECT c1", "queryAsync 结果完整送达（值语义）");
            check(trace[1] == "frame-intact",
                  "局部变量跨 co_await 挂起/恢复完整存活（帧连续性）");
            check(trace[2] == "e:ok:affected=1", "executeAsync 成功且 affected 正确");
            check(trace[3] == "b:ok:total=2", "executeBatchAsync 两批全部生效");
        }
        check(CoroMockConnection::queryCalls == 1 && CoroMockConnection::executeCalls == 3,
              "驱动调用次数 = 1 query + 1 execute + 2 batch（execute 复用）");

        // run() 返回后协程帧自毁：无法直接观察，间接验证 = 之后同场景再次运行无泄漏。
        std::promise<std::vector<std::string>> pr2;
        auto fut2 = pr2.get_future();
        async::run(pipelineBody(std::move(pr2)));
        std::vector<std::string> trace2;
        check(awaitFuture(fut2, trace2) && trace2.size() == 4,
              "同一协程工厂可重复 run（帧生命周期正确回收）");

        // 恢复线程断言：恢复发生在完成调度器线程上，而非调用线程。
        std::promise<std::thread::id> tidPr;
        auto tidFut = tidPr.get_future();
        async::run(tidBody(std::move(tidPr)));
        std::thread::id resumeTid{};
        check(awaitFuture(tidFut, resumeTid), "线程断言协程完成");
        check(resumeTid != callerTid,
              "co_await 恢复线程 != 调用线程（完成调度器线程，I1 协程版）");
    }

    // =====================================================================
    std::cout << "== C2. 惰性：未 co_await 的 Task 安全放弃（R5）==\n";
    {
        const int before = CoroMockConnection::queryCalls;
        {
            auto t = async::queryAsync("SELECT lazy");
            check(!t.await_ready(), "Task 构造即挂起（惰性，await_ready 恒 false）");
        } // t 析构：未 co_await，什么都不执行
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        check(CoroMockConnection::queryCalls == before,
              "未 await 即析构 → 零驱动调用（惰性安全放弃）");
    }

    // =====================================================================
    std::cout << "== C4. 语句超时：opts.timeout 与回调形态同源（D7）==\n";
    {
        CoroMockConnection::queryDelayMs = 400;
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::run(timeoutBody(std::move(pr)));
        async::QueryResult r;
        check(awaitFuture(fut, r, 3000), "超时路径完成");
        check(r.status.code == common::ErrorCode::QueryTimeout,
              "超时后 status = QueryTimeout");
        check(r.status.retryable, "超时标记为可重试（与回调形态一致）");
        CoroMockConnection::queryDelayMs = 0;
    }

    // =====================================================================
    std::cout << "== C5. 治理 fail-fast：审计拦截经协程层同源生效（D2）==\n";
    {
        CfgFlags f;
        f.auditBlock = true;
        applyConfig(f);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "reload 切换审计开关");

        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::run([](std::promise<async::QueryResult> p) -> async::Task<void> {
            auto r = co_await async::queryAsync("DELETE FROM t");
            p.set_value(std::move(r));
        }(std::move(pr)));
        async::QueryResult r;
        check(awaitFuture(fut, r, 3000), "被拦截路径完成");
        check(r.status.code == common::ErrorCode::SqlBlocked,
              "无 WHERE 的 DELETE 被审计拦截（SqlBlocked）");

        applyConfig(CfgFlags{});
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "reload 恢复基础配置");
    }

    // =====================================================================
    std::cout << "== C6. 事务：commit / rollback 与回调形态同源（§8.5）==\n";
    {
        CoroMockConnection::resetLog();
        common::TransactionOptions txOpts;
        std::promise<async::OpResult> pr;
        auto fut = pr.get_future();
        async::run(txBody(txOpts, false, std::move(pr)));
        async::OpResult r;
        check(awaitFuture(fut, r), "提交路径完成");
        check(r.status.ok() && CoroMockConnection::logHas("commit") &&
              !CoroMockConnection::logHas("rollback"),
              "fn 成功 → 提交");

        CoroMockConnection::resetLog();
        std::promise<async::OpResult> pr2;
        auto fut2 = pr2.get_future();
        async::run(txBody(txOpts, true, std::move(pr2)));
        async::OpResult r2;
        check(awaitFuture(fut2, r2), "回滚路径完成");
        check(!r2.status.ok() && CoroMockConnection::logHas("rollback") &&
              !CoroMockConnection::logHas("commit"),
              "fn 返回错误 → 回滚且状态非 Ok");
    }

    // =====================================================================
    std::cout << "== C7. 异常传播：内层协程抛出，外层 co_await 捕获 ==\n";
    {
        std::promise<std::string> pr;
        auto fut = pr.get_future();
        async::run(catcher(std::move(pr)));
        std::string msg;
        check(awaitFuture(fut, msg), "异常路径协程完成");
        check(msg == "caught:boom",
              "异常沿 continuation 链传播到外层 try/catch（实测 " + msg + "）");
    }

    // =====================================================================
    std::cout << "== C8. 显式数据源重载 ==\n";
    {
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::run(dsBody(std::move(pr)));
        async::QueryResult r;
        check(awaitFuture(fut, r), "数据源形态完成");
        check(r.status.ok() &&
              std::get<std::string>(r.rows.rows()[0].at("echo")) == "SELECT ds",
              "queryAsync(ds, sql, params) 路由到指定数据源");
    }

    // =====================================================================
    std::cout << "== C9. 生命周期：shutdown 排水在途协程，之后新任务被拒（T11）==\n";
    {
        CoroMockConnection::queryDelayMs = 150;
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::run(drainBody(std::move(pr)));
        check(async::detail::inFlight() >= 1, "在途协程操作计入 inFlight");
        const auto t0 = std::chrono::steady_clock::now();
        DBMW::shutdown(std::chrono::milliseconds(3000));
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        async::QueryResult r;
        check(awaitFuture(fut, r, 1000),
              "排水等待在途协程操作完成且结果送达");
        check(r.status.ok(), "排水完成的协程结果正常");
        check(async::detail::inFlight() == 0, "排水后 inFlight 归零");
        check(ms >= 100, "shutdown 确实等了在途语句（实测 " + std::to_string(ms) + "ms）");

        // shutdown 后：新协程任务快速失败（ConfigError），协程帧不悬垂。
        std::promise<async::QueryResult> pr2;
        auto fut2 = pr2.get_future();
        async::run(afterShutdownBody(std::move(pr2)));
        async::QueryResult r2;
        check(awaitFuture(fut2, r2, 2000), "shutdown 后协程任务仍能收尾");
        check(r2.status.code == common::ErrorCode::ConfigError,
              "shutdown 后新协程操作被快速拒绝（ConfigError）");
    }

    // 收尾统计
    std::cout << "\n----------------------------------------\n";
    std::cout << "通过 " << g_passed << " 项，失败 " << g_failed << " 项\n";
    return g_failed == 0 ? 0 : 1;
}
