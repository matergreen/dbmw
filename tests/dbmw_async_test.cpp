// dbmw v0.2.0 异步 API 行为验证（设计 §14：T5–T13）。
//
// 复用与 dbmw_core_test 相同的手法：mock 驱动跑真实的连接池 / 引擎管线，
// 不依赖任何第三方测试框架与真实数据库。
//
// 运行顺序有依赖：
//   - 各节通过 DBMW::reload 切换治理开关（审计/限流/缓存/熔断）；
//   - 生命周期（T11）必须放在最后 —— 它会 shutdown 掉全局引擎。
#include "dbmw/dbmw.h"
#include "dbmw/async/dbmw_async.h"
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
#include <variant>
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
// Mock 驱动（异步版：增加时延/取消开关）
// ---------------------------------------------------------------------------
class AsyncMockConnection : public core::IDatabaseConnection {
public:
    static std::atomic<int> alive;
    static std::atomic<int> connectCalls;
    static std::atomic<int> queryCalls;
    static std::atomic<int> executeCalls;
    static std::atomic<int> cancelCalls;
    // >0 时每次 query 睡这么久（毫秒）：制造"正在运行"的语句供取消/超时打断。
    static std::atomic<int> queryDelayMs;
    // 每次 query() 进入时置 true：测试用它确认语句已真正在驱动里跑起来
    // （Handle::Running 在借连接前就置位，取消测试不能只等 Running）。
    static std::atomic<bool> queryEntered;
    // >0 时前 N 次 query 失败（可重试错误）。
    static std::atomic<int> queryFailuresRemaining;
    // true = 模拟"驱动未实现取消"（cancel 返回 NotSupported）。
    static std::atomic<bool> cancelUnsupported;
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

    static std::string joined() {
        std::lock_guard<std::mutex> lk(logMtx);
        std::string s;
        for (auto &x: log) { if (!s.empty()) s += " | "; s += x; }
        return s;
    }

    Status connect(const config::DataSourceConfig &cfg) override {
        (void) cfg;
        ++connectCalls;
        open_ = true;
        ++alive;
        std::lock_guard<std::mutex> lk(logMtx);
        log.push_back("connect");
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
        if (queryFailuresRemaining.fetch_sub(1) > 0)
            return Status::databaseError(common::ErrorCode::QueryError,
                                         "mock connection lost", "08006");
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
        if (options.readOnly || options.isolation != common::IsolationLevel::Default) {
            std::lock_guard<std::mutex> lk(logMtx);
            log.push_back(std::string("options:") +
                          (options.readOnly ? "readonly" : "readwrite"));
        }
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
        if (cancelUnsupported.load()) return IDatabaseConnection::cancel();
        ++cancelCalls;
        std::lock_guard<std::mutex> lk(logMtx);
        log.push_back("cancel");
        return Status::OK();
    }

    void close() override {
        if (open_) { open_ = false; --alive; }
    }

    bool isOpen() const override { return open_; }
    bool inTransaction() const override { return tx_; }
    bool allowsLiteralInterpolation() const override { return true; }

private:
    bool open_ = false;
    bool tx_ = false;
};

std::atomic<int> AsyncMockConnection::alive{0};
std::atomic<int> AsyncMockConnection::connectCalls{0};
std::atomic<int> AsyncMockConnection::queryCalls{0};
std::atomic<int> AsyncMockConnection::executeCalls{0};
std::atomic<int> AsyncMockConnection::cancelCalls{0};
std::atomic<int> AsyncMockConnection::queryDelayMs{0};
std::atomic<bool> AsyncMockConnection::queryEntered{false};
std::atomic<int> AsyncMockConnection::queryFailuresRemaining{0};
std::atomic<bool> AsyncMockConnection::cancelUnsupported{false};
std::vector<std::string> AsyncMockConnection::log;
std::mutex AsyncMockConnection::logMtx;

class AsyncMockDriver : public driver::IDriver {
public:
    const char *name() const override { return "amock"; }

    std::unique_ptr<core::IDatabaseConnection> createConnection() override {
        return std::make_unique<AsyncMockConnection>();
    }
};

// 模拟已停止/队列满的完成调度器：所有即时投递都拒绝。
// 异步引擎必须仍然在非调用线程交付回调，不能 caller-runs。
class RejectingCompletionExecutor final : public async::IExecutor {
public:
    bool tryPost(Task) override { return false; }
    void postAfter(Task, std::chrono::milliseconds) override {}
    void shutdown(std::chrono::milliseconds) override {}
    [[nodiscard]] async::ExecutorStats stats() const override { return {}; }
};

// ---------------------------------------------------------------------------
// 配置与工具
// ---------------------------------------------------------------------------
struct CfgFlags {
    bool auditBlock = false;      // sql_audit: block_no_where_dml + action=block
    bool readOnlyGroup = false;   // 组 read_only + enforce_read_only
    bool rateLimit = false;       // global_qps = 1
    bool cache = false;           // query_cache on
    int circuitThreshold = 0;     // >0 时启用熔断阈值
    int circuitOpenMs = 60000;    // 熔断开放时长
    int retryBackoffMs = 20;
};

static std::string buildConfig(const CfgFlags &f) {
    std::string audit = f.auditBlock || f.readOnlyGroup
        ? R"("sql_audit": { "enabled": true, "action": "block", "block_no_where_dml": true, "enforce_read_only": true },)"
        : R"("sql_audit": { "enabled": false },)";
    std::string rate = f.rateLimit
        ? R"("rate_limit": { "enabled": true, "global_qps": 1, "per_fingerprint_qps": 0, "burst": 0, "fingerprint_mode": "off" },)"
        : R"("rate_limit": { "enabled": false },)";
    std::string cache = f.cache
        ? R"("query_cache": { "enabled": true, "ttl_ms": 60000, "max_entries": 100 },)"
        : R"("query_cache": { "enabled": false },)";
    std::string circuit = f.circuitThreshold > 0
        ? R"("circuit_breaker": { "failure_threshold": )" + std::to_string(f.circuitThreshold) +
          R"(, "open_interval_ms": )" + std::to_string(f.circuitOpenMs) + R"( },)"
        : R"("circuit_breaker": { "failure_threshold": 0 },)";
    std::string groups = f.readOnlyGroup
        ? R"( "groups": [ { "name": "ro", "primary": "main", "replicas": [], "read_only": true } ] )"
        : R"( "groups": [] )";
    return R"({
  "default_datasource": "main",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 4, "borrow_timeout_ms": 2000 },
  "retry": { "max_attempts": 3, "initial_backoff_ms": )" +
           std::to_string(f.retryBackoffMs) + R"(, "max_backoff_ms": )" +
           std::to_string(f.retryBackoffMs) + R"(, "retry_writes": false },
  )" + circuit + rate + audit + cache + R"(
  "async": { "enabled": true, "threads": 1, "queue_size": 64 },
  "datasources": [
    { "name": "main", "type": "amock", "host": "localhost" }
  ],
  )" + groups + "\n}\n";
}

static std::string g_configPath;

static bool applyConfig(const CfgFlags &f) {
    std::ofstream(g_configPath) << buildConfig(f);
    return true;
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

// 回调式便捷封装：返回 (结果, 完成时刻)。
template <class R>
struct AsyncOutcome {
    R result;
    std::chrono::steady_clock::time_point doneAt;
    std::thread::id threadId;
};

template <class R>
static AsyncOutcome<R> awaitResult(std::future<R> &&f, int timeoutMs = 5000) {
    AsyncOutcome<R> o;
    if (f.wait_for(std::chrono::milliseconds(timeoutMs)) == std::future_status::ready) {
        o.result = f.get();
        o.doneAt = std::chrono::steady_clock::now();
    }
    return o;
}

// ---------------------------------------------------------------------------
int main() {
    g_configPath = (std::filesystem::temp_directory_path() /
                    "dbmw_async_test.json").string();

    driver::DriverRegistry::instance().registerDriver(
        "amock", [] { return std::make_unique<AsyncMockDriver>(); });

    CfgFlags base;
    applyConfig(base);
    if (!DBMW::init(g_configPath).ok()) {
        std::cout << "init failed\n";
        return 1;
    }

    // =====================================================================
    std::cout << "== A1. 基础：回调式 query / future 式 execute（T13 前置）==\n";
    {
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        const auto callerTid = std::this_thread::get_id();
        std::thread::id cbTid{};
        auto h = async::query("SELECT 1", [&](async::QueryResult &&r) {
            cbTid = std::this_thread::get_id();
            pr.set_value(std::move(r));
        });
        check(h.valid(), "query 返回有效 Handle");
        auto out = awaitResult(std::move(fut));
        if (!out.result.status.ok())
            std::cout << "  [DEBUG] A1 query status: code="
                      << static_cast<int>(out.result.status.code) << " msg="
                      << out.result.status.message << "\n";
        check(out.result.status.ok(), "回调式 query 成功");
        check(out.result.rows.rowCount() == 1 &&
              std::get<std::string>(out.result.rows.rows()[0].at("echo")) == "SELECT 1",
              "结果行经值语义完整送达");
        check(cbTid != callerTid, "完成回调不在调用线程上（I1）");

        auto ef = async::execute("UPDATE t SET v = 1");
        auto eout = awaitResult(std::move(ef));
        check(eout.result.status.ok() && eout.result.affected == 1,
              "future 式 execute 成功且 affected 正确");

        // Handle 一次性：完成后状态为 Done，再 cancel 报错（T8 的一部分）。
        check(h.state() == async::Handle::State::Done, "完成后 Handle 状态为 Done");
        check(h.cancel().code == common::ErrorCode::QueryError,
              "对已完成的 Handle 调 cancel 返回错误");

        async::Handle invalid;
        check(!invalid.valid() && invalid.state() == async::Handle::State::Done,
              "默认构造 Handle 无效且视作 Done");
    }

    // =====================================================================
    std::cout << "== A2. 重试：postAfter 退避，worker 不睡眠阻塞（T7/D6）==\n";
    {
        AsyncMockConnection::queryCalls = 0;
        AsyncMockConnection::executeCalls = 0;
        AsyncMockConnection::queryFailuresRemaining = 2; // 前 2 次失败，第 3 次成功
        // 完成时刻在回调内记录（awaitResult 按顺序 await，取结果时刻≠完成时刻）。
        std::promise<async::QueryResult> retryPr;
        std::promise<async::ExecResult> markerPr;
        auto retryFut = retryPr.get_future();
        auto markerFut = markerPr.get_future();
        auto retryDone = std::chrono::steady_clock::time_point::max();
        auto markerDone = std::chrono::steady_clock::time_point::max();
        async::query("SELECT retry", [&](async::QueryResult &&r) {
            retryDone = std::chrono::steady_clock::now();
            retryPr.set_value(std::move(r));
        });
        const auto t0 = std::chrono::steady_clock::now();
        // 重试等待期间提交无故障的 marker（用 execute：与 query 的失败计数器
        // 隔离，不受重试场景注入的失败影响）：单 worker 下若重试用 sleep 阻塞，
        // marker 就得等 2 个退避间隔 + 全部重试之后才能跑。
        async::execute("UPDATE marker SET v = 1", [&](async::ExecResult &&r) {
            markerDone = std::chrono::steady_clock::now();
            markerPr.set_value(std::move(r));
        });
        auto retryOut = awaitResult(std::move(retryFut), 8000);
        auto markerOut = awaitResult(std::move(markerFut), 8000);
        const auto markerMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            markerDone - t0).count();
        const auto retryMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            retryDone - t0).count();
        check(retryOut.result.status.ok() &&
              AsyncMockConnection::queryCalls == 3 &&
              AsyncMockConnection::executeCalls == 1,
              "可重试错误经退避后第三次成功（query 3 次 + marker 1 次）");
        check(markerOut.result.status.ok(), "重试等待期间 marker 正常完成");
        check(markerMs < 100,
              "marker 立即执行（实测 " + std::to_string(markerMs) +
                  "ms）—— worker 未被退避睡眠占用");
        // 退避时长有抖动且受调度精度影响，不做绝对下限断言；
        // 逻辑必然性：marker 在退避窗口内完成，必早于重试的第 3 次尝试。
        check(retryMs > markerMs,
              "重试整体耗时长于 marker（" + std::to_string(retryMs) + "ms vs " +
                  std::to_string(markerMs) + "ms，含抖动）");
    }

    // =====================================================================
    std::cout << "== A3. 取消：Running 转发 / Queued 免池 / Done 报错（T8）==\n";
    {
        // --- Running：慢语句运行中取消，转发到驱动 ---
        AsyncMockConnection::resetLog();
        AsyncMockConnection::cancelCalls = 0;
        AsyncMockConnection::queryEntered = false;
        AsyncMockConnection::queryDelayMs = 250;
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        auto h = async::query("SELECT slow", [&](async::QueryResult &&r) {
            pr.set_value(std::move(r));
        });
        // 等 queryEntered 而不是 Handle::Running：Running 在借连接前就置位，
        // 而 cancel 转发需要会话已被钉住（attempt 执行中）。
        check(waitUntil([&] { return AsyncMockConnection::queryEntered.load(); }, 2000),
              "慢语句进入驱动执行（会话已钉住）");
        const auto cstat = h.cancel();
        check(cstat.ok() && AsyncMockConnection::cancelCalls == 1,
              "Running 取消转发到驱动 cancel");
        auto out = awaitResult(std::move(fut));
        check(out.result.status.code == common::ErrorCode::Cancelled,
              "取消后结果被改写为 Cancelled");
        check(AsyncMockConnection::logHas("cancel"), "驱动侧收到 cancel");
        AsyncMockConnection::queryDelayMs = 0;

        // --- Queued：worker 被慢语句占满，后续操作排队中被取消 ---
        AsyncMockConnection::resetLog();
        AsyncMockConnection::queryDelayMs = 300;
        std::promise<async::QueryResult> pr1, pr2;
        auto fut1 = pr1.get_future();
        auto fut2 = pr2.get_future();
        const auto connectsBefore = AsyncMockConnection::connectCalls.load();
        auto h1 = async::query("SELECT busy", [&](async::QueryResult &&r) {
            pr1.set_value(std::move(r));
        });
        auto h2 = async::query("SELECT queued", [&](async::QueryResult &&r) {
            pr2.set_value(std::move(r));
        });
        check(h2.state() == async::Handle::State::Queued,
              "worker 占满时新操作停留在 Queued");
        check(h2.cancel().ok(), "Queued 取消立即返回 OK");
        auto out1 = awaitResult(std::move(fut1), 8000);
        auto out2 = awaitResult(std::move(fut2), 8000);
        check(out1.result.status.ok(), "占住 worker 的语句正常完成");
        check(out2.result.status.code == common::ErrorCode::Cancelled,
              "排队中被取消的操作以 Cancelled 收尾");
        check(AsyncMockConnection::connectCalls.load() == connectsBefore,
              "Queued 取消未触碰连接池（连接创建数不变）");
        AsyncMockConnection::queryDelayMs = 0;
    }

    // =====================================================================
    std::cout << "== A4. 语句超时：QueryTimeout + 未送达取消提示（T9/D7）==\n";
    {
        AsyncMockConnection::queryDelayMs = 400;
        AsyncMockConnection::cancelUnsupported = true; // 模拟驱动不支持取消
        async::Options opts;
        opts.timeout = std::chrono::milliseconds(60);
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::query("SELECT slow2", [&](async::QueryResult &&r) {
            pr.set_value(std::move(r));
        }, opts);
        auto res = awaitResult(std::move(fut), 8000);
        if (res.result.status.code != common::ErrorCode::QueryTimeout)
            std::cout << "  [DEBUG] A4: code=" << static_cast<int>(res.result.status.code)
                      << " msg=" << res.result.status.message << "\n";
        check(res.result.status.code == common::ErrorCode::QueryTimeout,
              "超时后结果为 QueryTimeout");
        check(res.result.status.retryable,
              "超时状态标记为可重试");
        check(res.result.status.message.find("could not cancel") != std::string::npos,
              "驱动不支持取消时 message 附提示");
        AsyncMockConnection::queryDelayMs = 0;
        AsyncMockConnection::cancelUnsupported = false;
    }

    // =====================================================================
    std::cout << "== A5. 缓存：命中零驱动调用，写失效（T6 + T12 缓存交互）==\n";
    {
        CfgFlags f = base;
        f.cache = true;
        applyConfig(f);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "热加载开启查询缓存");

        AsyncMockConnection::queryCalls = 0;
        auto miss = awaitResult(async::query("SELECT cacheable"));
        check(miss.result.status.ok() && AsyncMockConnection::queryCalls == 1,
              "首次异步查询未命中缓存（1 次驱动调用）");

        AsyncMockConnection::queryCalls = 0;
        auto hit = awaitResult(async::query("SELECT cacheable"));
        check(hit.result.status.ok() && AsyncMockConnection::queryCalls == 0 &&
              hit.result.rows.rowCount() == 1 &&
              std::get<std::string>(hit.result.rows.rows()[0].at("echo")) ==
                  "SELECT cacheable",
              "第二次异步查询缓存命中，mock 驱动零调用");

        // 跨路径一致性：异步写入的缓存，同步读得到；同步写失效后异步回源。
        AsyncMockConnection::queryCalls = 0;
        common::ResultSet syncRows;
        check(DBMW::query("SELECT cacheable", syncRows).ok() &&
              AsyncMockConnection::queryCalls == 0 &&
              syncRows.rowCount() == 1,
              "同步 query 命中异步写入的缓存（跨路径共享）");

        std::int64_t affected = 0;
        check(DBMW::execute("UPDATE t SET v = 1", affected).ok(),
              "同步写执行成功");
        AsyncMockConnection::queryCalls = 0;
        auto refetch = awaitResult(async::query("SELECT cacheable"));
        check(refetch.result.status.ok() && AsyncMockConnection::queryCalls == 1,
              "写后缓存失效，异步查询重新回源");

        applyConfig(base);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "恢复基础配置");
    }

    // =====================================================================
    std::cout << "== A6. 管线 fail-fast：审计 / 限流 / 熔断（T5 + I1）==\n";
    {
        // --- 审计拦截：无 WHERE 的 DELETE ---
        CfgFlags f = base;
        f.auditBlock = true;
        applyConfig(f);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "热加载开启审计拦截");
        const auto callerTid = std::this_thread::get_id();
        std::thread::id cbTid{};
        std::promise<async::ExecResult> pr;
        auto fut = pr.get_future();
        async::execute("DELETE FROM t", [&](async::ExecResult &&r) {
            cbTid = std::this_thread::get_id();
            pr.set_value(std::move(r));
        });
        auto blocked = awaitResult(std::move(fut), 3000);
        check(blocked.result.status.code == common::ErrorCode::SqlBlocked,
              "无 WHERE 的 DELETE 被审计拦截（SqlBlocked）");
        check(cbTid != callerTid,
              "拦截结果经完成调度器投递，不在调用线程栈上（I1）");

        // --- 限流：global_qps=1，第二次立即被拒 ---
        CfgFlags rf = base;
        rf.rateLimit = true;
        applyConfig(rf);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "热加载开启限流");
        auto first = awaitResult(async::query("SELECT rl"));
        auto second = awaitResult(async::query("SELECT rl"));
        check(first.result.status.ok(), "限流下的第一次操作放行");
        check(second.result.status.code == common::ErrorCode::RateLimited,
              "第二次操作被限流快速拒绝（RateLimited）");

        // --- 熔断：语义对齐同步测试 #22（阈值 2、半开恢复）---
        // remaining=2 时第一次查询的重试中途熔断打开（attempt1/2 各计一次失败，
        // 第 3 次尝试被逐尝试闸门拦下）→ CircuitOpen 且恰好 2 次驱动调用。
        // open_interval 取 2000ms：两次退避名义 ~40ms，但慢 runner（macOS 托管
        // 机调度抖动大）上重试间隔可膨胀到数百 ms——窗口必须远大于最坏重试
        // 间隔，否则熔断在重试结束前过期转半开、探测放行（实测踩过：100ms
        // 窗口在 macOS CI 上 calls=3 且收尾 Ok）。随后冷却 2100ms > 2000ms，
        // 下一次查询作为半开探测放行。
        CfgFlags cf = base;
        cf.circuitThreshold = 2;
        cf.circuitOpenMs = 2000;
        applyConfig(cf);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "热加载开启熔断（阈值 2，开放 2000ms）");
        AsyncMockConnection::queryCalls = 0;
        AsyncMockConnection::queryFailuresRemaining = 2;
        auto f1 = awaitResult(async::query("SELECT cb"), 8000);
        if (f1.result.status.code != common::ErrorCode::CircuitOpen
            || AsyncMockConnection::queryCalls != 2)
            std::cout << "  [DEBUG] A6 f1: code="
                      << static_cast<int>(f1.result.status.code) << " msg="
                      << f1.result.status.message << " calls="
                      << AsyncMockConnection::queryCalls.load() << "\n";
        check(f1.result.status.code == common::ErrorCode::CircuitOpen &&
              AsyncMockConnection::queryCalls == 2,
              "重试途中熔断打开：第一次查询以 CircuitOpen 收尾且恰好 2 次驱动调用");
        AsyncMockConnection::queryCalls = 0;
        const auto t0 = std::chrono::steady_clock::now();
        auto f2 = awaitResult(async::query("SELECT cb"), 3000);
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        check(f2.result.status.code == common::ErrorCode::CircuitOpen &&
              AsyncMockConnection::queryCalls == 0,
              "熔断开放期间异步查询快速失败且零驱动调用");
        check(ms < 500, "快速失败（实测 " + std::to_string(ms) + "ms）");
        // 冷却 2500ms > 开放 2000ms（留出 f1 收尾与 f2 快速失败的耗时余量）：
        // 下一次查询作为半开探测放行。
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));
        AsyncMockConnection::queryFailuresRemaining = 0;
        AsyncMockConnection::queryCalls = 0;
        auto f3 = awaitResult(async::query("SELECT cb"), 3000);
        check(f3.result.status.ok() && AsyncMockConnection::queryCalls == 1,
              "冷却后半开探测成功并关闭熔断");

        applyConfig(base);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "恢复基础配置");
    }

    // =====================================================================
    std::cout << "== A7. 异步事务：提交 / 回滚 / 异常 / 只读拦截（T10）==\n";
    {
        // --- 成功提交 ---
        AsyncMockConnection::resetLog();
        auto okOut = awaitResult(async::transaction([](core::Session &s) {
            std::int64_t n = 0;
            return s.execute("UPDATE a SET v = 1", n);
        }));
        check(okOut.result.status.ok() && AsyncMockConnection::logHas("begin") &&
              AsyncMockConnection::logHas("commit") &&
              !AsyncMockConnection::logHas("rollback"),
              "fn 成功的异步事务自动提交");

        // --- fn 失败回滚 ---
        AsyncMockConnection::resetLog();
        auto failOut = awaitResult(async::transaction([](core::Session &s) {
            std::int64_t n = 0;
            if (auto st = s.execute("UPDATE a SET v = 1", n); !st.ok()) return st;
            return Status::error(common::ErrorCode::QueryError, "biz error");
        }));
        check(!failOut.result.status.ok() &&
              failOut.result.status.code == common::ErrorCode::QueryError &&
              AsyncMockConnection::logHas("rollback") &&
              !AsyncMockConnection::logHas("commit"),
              "fn 失败的异步事务自动回滚");

        // --- fn 抛异常回滚 ---
        AsyncMockConnection::resetLog();
        auto throwOut = awaitResult(async::transaction([](core::Session &) -> Status {
            throw std::runtime_error("user callback blew up");
        }));
        check(!throwOut.result.status.ok() &&
              AsyncMockConnection::logHas("rollback"),
              "fn 抛异常的异步事务回滚且不外泄异常");

        // --- withSession：不开事务 ---
        AsyncMockConnection::resetLog();
        std::promise<async::OpResult> wsPr;
        auto wsFut = wsPr.get_future();
        async::withSession([](core::Session &s) {
            common::ResultSet rs;
            return s.query("SELECT 1", rs);
        }, [&](async::OpResult &&r) { wsPr.set_value(std::move(r)); });
        auto wsOut = awaitResult(std::move(wsFut));
        check(wsOut.result.status.ok() && !AsyncMockConnection::logHas("begin"),
              "withSession 不自动开事务");

        // --- 只读组拦截写（审计 enforce_read_only）---
        CfgFlags f = base;
        f.readOnlyGroup = true;
        applyConfig(f);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "热加载只读组");
        auto roOut = awaitResult(async::transaction(
            "ro", common::TransactionOptions{}, [](core::Session &s) {
                std::int64_t n = 0;
                return s.execute("UPDATE a SET v = 1", n);
            }));
        check(roOut.result.status.code == common::ErrorCode::SqlBlocked,
              "只读组上的异步写被拦截（SqlBlocked）");

        applyConfig(base);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "恢复基础配置");
    }

    // =====================================================================
    std::cout << "== A8. 一致性矩阵：同步 vs 异步逐项相等（T12/R1）==\n";
    {
        common::ErrorCode syncCode, asyncCode;
        int syncCalls, asyncCalls;

        // --- 场景 1：可重试错误，第 3 次成功（基础配置：熔断关闭）---
        applyConfig(base);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "热加载场景 1 配置（无熔断）");
        {
            AsyncMockConnection::queryCalls = 0;
            AsyncMockConnection::queryFailuresRemaining = 2;
            common::ResultSet rs;
            syncCode = DBMW::query("SELECT m1", rs).code;
            syncCalls = AsyncMockConnection::queryCalls.load();
        }
        {
            AsyncMockConnection::queryCalls = 0;
            AsyncMockConnection::queryFailuresRemaining = 2;
            auto out = awaitResult(async::query("SELECT m1"), 8000);
            asyncCode = out.result.status.code;
            asyncCalls = AsyncMockConnection::queryCalls.load();
        }
        check(syncCode == common::ErrorCode::Ok && asyncCode == common::ErrorCode::Ok &&
              syncCalls == 3 && asyncCalls == 3,
              "重试场景：最终状态码与驱动调用数同步=异步（Ok / 3 次）");

        // --- 场景 2：熔断（阈值 2，开放 60s）：重试途中打开 + 后续快速失败 ---
        CfgFlags f = base;
        f.circuitThreshold = 2;
        applyConfig(f);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "热加载场景 2 配置（熔断阈值 2）");
        {
            AsyncMockConnection::queryCalls = 0;
            AsyncMockConnection::queryFailuresRemaining = 2;
            common::ResultSet rs;
            const auto first = DBMW::query("SELECT m2", rs);
            const auto second = DBMW::query("SELECT m2", rs);
            syncCode = second.code;
            syncCalls = AsyncMockConnection::queryCalls.load();
            check(first.code == common::ErrorCode::CircuitOpen,
                  "同步：重试途中熔断打开（第一次查询即 CircuitOpen）");
        }
        {
            // reload 重建数据源以重置熔断状态
            check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
                  "重载数据源重置熔断");
            AsyncMockConnection::queryCalls = 0;
            AsyncMockConnection::queryFailuresRemaining = 2;
            auto r1 = awaitResult(async::query("SELECT m2"), 8000);
            auto r2 = awaitResult(async::query("SELECT m2"), 3000);
            asyncCode = r2.result.status.code;
            asyncCalls = AsyncMockConnection::queryCalls.load();
        }
        check(syncCode == common::ErrorCode::CircuitOpen &&
              asyncCode == common::ErrorCode::CircuitOpen &&
              syncCalls == 2 && asyncCalls == 2,
              "熔断场景：重试途中打开 + 后续快速失败，同步=异步"
              "（CircuitOpen / 恰好 2 次驱动调用）");
        AsyncMockConnection::queryFailuresRemaining = 0;

        // --- 场景 3：写默认不重试（先 reload 清掉打开的熔断）---
        {
            applyConfig(base); // 重建数据源，重置熔断状态
            check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
                  "重载数据源重置熔断（写场景前）");
            AsyncMockConnection::executeCalls = 0;
            std::int64_t n = 0;
            const auto s = DBMW::execute("UPDATE t SET v = 1", n);
            syncCode = s.code;
            syncCalls = AsyncMockConnection::executeCalls.load();
        }
        {
            AsyncMockConnection::executeCalls = 0;
            auto a = awaitResult(async::execute("UPDATE t SET v = 1"));
            asyncCode = a.result.status.code;
            asyncCalls = AsyncMockConnection::executeCalls.load();
        }
        check(syncCode == common::ErrorCode::Ok && asyncCode == common::ErrorCode::Ok &&
              syncCalls == 1 && asyncCalls == 1,
              "写场景：单次执行语义同步=异步（Ok / 1 次）");

        applyConfig(base);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(),
              "恢复基础配置");
    }

    // =====================================================================
    std::cout << "== A9. 完成调度器过载：回调仍不在调用栈内执行 ==\n";
    {
        async::setCompletionExecutor(std::make_shared<RejectingCompletionExecutor>());
        const auto caller = std::this_thread::get_id();
        std::promise<std::pair<common::Status, std::thread::id>> promise;
        auto future = promise.get_future();
        async::query("SELECT completion_fallback", [&promise](async::QueryResult &&r) {
            promise.set_value({std::move(r.status), std::this_thread::get_id()});
        });
        const auto delivered = future.get();
        check(delivered.first.ok() && delivered.second != caller,
              "完成调度器拒绝投递时，保底队列异步交付回调");
        // 后续生命周期用例仍走保底调度器，不再将已拒绝的对象留在全局。
        async::setCompletionExecutor(nullptr);
    }

    std::cout << "== A10. 生命周期：shutdown 排水，之后新操作被拒（T11/§9.3）==\n";
    {
        AsyncMockConnection::queryDelayMs = 150;
        std::promise<async::QueryResult> pr;
        auto fut = pr.get_future();
        async::query("SELECT drain", [&](async::QueryResult &&r) {
            pr.set_value(std::move(r));
        });
        check(async::detail::inFlight() >= 1, "在途操作计入 inFlight");
        const auto t0 = std::chrono::steady_clock::now();
        DBMW::shutdown(std::chrono::milliseconds(3000));
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        auto out = awaitResult(std::move(fut), 1000);
        check(out.result.status.ok(),
              "排水等待在途操作完成且结果正常送达");
        check(async::detail::inFlight() == 0, "排水后 inFlight 归零");
        check(ms >= 100, "shutdown 确实等了在途语句（实测 " + std::to_string(ms) + "ms）");

        auto rejected = awaitResult(async::query("SELECT after"), 1000);
        check(rejected.result.status.code == common::ErrorCode::ConfigError,
              "shutdown 后新异步操作被快速拒绝");
        AsyncMockConnection::queryDelayMs = 0;
    }

    // 收尾统计
    std::cout << "\n----------------------------------------\n";
    std::cout << "通过 " << g_passed << " 项，失败 " << g_failed << " 项\n";
    return g_failed == 0 ? 0 : 1;
}
