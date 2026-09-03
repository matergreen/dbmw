// dbmw 核心层行为验证：用 mock 驱动跑真实的连接池/事务/参数插值逻辑。
#include "dbmw/dbmw.h"
#include "dbmw/core/connection_pool.h"
#include "dbmw/core/idatabase_connection.h"
#include "dbmw/config/config_loader.h"
#include "dbmw/driver/driver_registry.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <set>
#include <sstream>
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
// Mock 驱动
// ---------------------------------------------------------------------------
class MockConnection : public core::IDatabaseConnection {
public:
    static std::atomic<int> alive;
    static std::atomic<bool> connectFails;
    static std::atomic<bool> pingFails;
    static std::atomic<bool> queryBreaks;
    static std::atomic<int> queryFailuresRemaining;
    static std::atomic<int> executeFailuresRemaining;
    static std::atomic<int> queryCalls;
    static std::atomic<int> executeCalls;
    // >= 0 时：先允许这么多次 execute 成功，之后的那一次失败。
    // 用于验证批量执行的原子性（中途失败要整批回滚）。
    static std::atomic<int> executeOkBeforeFail;
    // 置 true 时 cancel() 抛异常，用于验证看门狗线程不会因此崩溃进程。
    static std::atomic<bool> cancelThrows;
    // 默认 true：模拟"驱动未实现取消"（基类行为返回 NotSupported）。
    // 需要验证真实取消路径时临时置 false。
    static std::atomic<bool> cancelUnsupported;
    static std::vector<std::string> log;

    static void resetLog() { log.clear(); }

    static std::string joined() {
        std::string s;
        for (auto &x: log) { if (!s.empty()) s += " | "; s += x; }
        return s;
    }

    common::Status connect(const config::DataSourceConfig &cfg) override {
        (void) cfg;
        if (connectFails.load()) return Status::error(common::ErrorCode::ConnectionFailed, "mock connect failed");
        open_ = true;
        ++alive;
        log.push_back("connect");
        return Status::OK();
    }

    common::Status ping() override {
        if (!open_ || pingFails.load()) return Status::error(common::ErrorCode::PingFailed, "mock ping failed");
        return Status::OK();
    }

    common::Status query(const std::string &sql, common::ResultSet &out) override {
        if (!open_) return Status::error(common::ErrorCode::NotConnected, "closed");
        ++queryCalls;
        if (queryBreaks.load() || queryFailuresRemaining.fetch_sub(1) > 0)
            return Status::databaseError(common::ErrorCode::QueryError,
                                         "mock connection lost", "08006");
        log.push_back("query:" + sql);
        common::Row r;
        r.set("echo", std::string(sql));
        out.addRow(std::move(r));
        if (sql == "MULTI") {
            common::Row second;
            second.set("echo", std::string("second"));
            out.addRow(std::move(second));
            common::Row third;
            third.set("echo", std::string("third"));
            out.addRow(std::move(third));
        }
        return Status::OK();
    }

    common::Status execute(const std::string &sql, std::int64_t &affected) override {
        if (!open_) return Status::error(common::ErrorCode::NotConnected, "closed");
        ++executeCalls;
        if (executeFailuresRemaining.fetch_sub(1) > 0)
            return Status::databaseError(common::ErrorCode::QueryError,
                                         "mock connection lost", "08006");
        if (executeOkBeforeFail.load() >= 0 && executeOkBeforeFail.fetch_sub(1) == 0)
            return Status::databaseError(common::ErrorCode::QueryError,
                                         "mock write failed midway", "08006");
        log.push_back("execute:" + sql);
        affected = 1;
        return Status::OK();
    }

    common::Status begin() override {
        if (tx_) return Status::error(common::ErrorCode::TxError, "already in tx");
        tx_ = true;
        log.push_back("begin");
        return Status::OK();
    }

    common::Status begin(const common::TransactionOptions &options) override {
        if (options.readOnly || options.isolation != common::IsolationLevel::Default) {
            log.push_back(std::string("options:")
                          + (options.readOnly ? "readonly" : "readwrite") + ":"
                          + std::to_string(static_cast<int>(options.isolation)));
        }
        return begin();
    }

    common::Status commit() override {
        if (!tx_) return Status::error(common::ErrorCode::TxError, "no tx");
        tx_ = false;
        log.push_back("commit");
        return Status::OK();
    }

    common::Status rollback() override {
        if (!tx_) return Status::error(common::ErrorCode::TxError, "no tx");
        tx_ = false;
        log.push_back("rollback");
        return Status::OK();
    }

    common::Status savepoint(const std::string &name) override {
        if (!tx_) return Status::error(common::ErrorCode::TxError, "no tx");
        log.push_back("savepoint:" + name);
        return Status::OK();
    }

    common::Status releaseSavepoint(const std::string &name) override {
        if (!tx_) return Status::error(common::ErrorCode::TxError, "no tx");
        log.push_back("release:" + name);
        return Status::OK();
    }

    common::Status rollbackToSavepoint(const std::string &name) override {
        if (!tx_) return Status::error(common::ErrorCode::TxError, "no tx");
        log.push_back("rollback_to:" + name);
        return Status::OK();
    }

    // 模拟"驱动实现了取消"：内置驱动会在这里发 KILL / cancel_query / SQLCancelHandle。
    // cancelThrows 用来验证取消实现自身抛异常时，看门狗线程不会把进程带崩。
    common::Status cancel() override {
        if (cancelUnsupported.load()) return IDatabaseConnection::cancel();
        log.push_back("cancel");
        if (cancelThrows.load()) throw std::runtime_error("mock cancel blew up");
        return Status::OK();
    }

    void close() override {
        if (open_) { open_ = false; --alive; log.push_back("close"); }
    }

    bool isOpen() const override { return open_; }

    // 必须反映真实事务状态：基类 executeBatch 靠它决定要不要自己包事务。
    bool inTransaction() const override { return tx_; }

    bool allowsLiteralInterpolation() const override { return true; }

private:
    bool open_ = false;
    bool tx_ = false;
};

std::atomic<int> MockConnection::alive{0};
std::atomic<bool> MockConnection::connectFails{false};
std::atomic<bool> MockConnection::pingFails{false};
std::atomic<bool> MockConnection::queryBreaks{false};
std::atomic<int> MockConnection::queryFailuresRemaining{0};
std::atomic<int> MockConnection::executeFailuresRemaining{0};
std::atomic<int> MockConnection::queryCalls{0};
std::atomic<int> MockConnection::executeCalls{0};
std::atomic<int> MockConnection::executeOkBeforeFail{-1};
std::atomic<bool> MockConnection::cancelThrows{false};
std::atomic<bool> MockConnection::cancelUnsupported{true};
std::vector<std::string> MockConnection::log;

class MockDriver : public driver::IDriver {
public:
    const char *name() const override { return "mock"; }

    std::unique_ptr<core::IDatabaseConnection> createConnection() override {
        return std::make_unique<MockConnection>();
    }
};

static config::DataSourceConfig mockCfg(const std::string &name = "mock") {
    config::DataSourceConfig c;
    c.name = name;
    c.type = "mock";
    c.host = "localhost";
    return c;
}

static std::shared_ptr<core::ConnectionPool> makePool(int min, int max, int timeoutMs = 300) {
    return std::make_shared<core::ConnectionPool>(
        std::make_unique<MockDriver>(), mockCfg(), min, max,
        std::chrono::milliseconds(timeoutMs));
}

// ---------------------------------------------------------------------------
int main() {
    std::cout << "== 1. 借出与归还（连接复用） ==\n";
    {
        MockConnection::alive = 0;
        auto pool = makePool(1, 2);
        {
            common::ErrorCode ec;
            std::string err;
            auto h = pool->borrow(ec, err);
            check(h != nullptr, "首次借出成功");
            check(ec == common::ErrorCode::Ok, "错误码为 Ok");
            check(pool->borrowedCount() == 1, "borrowedCount == 1");
        }
        check(pool->borrowedCount() == 0, "句柄析构后归还");
        check(MockConnection::alive == 1, "连接未被销毁（复用而非重连）");
        pool->shutdown(std::chrono::milliseconds(0));
    }
    check(MockConnection::alive == 0, "shutdown 后连接全部关闭");

    std::cout << "== 2. 池耗尽：等待超时而非无限阻塞 ==\n";
    {
        MockConnection::alive = 0;
        auto pool = makePool(0, 1, 150);
        common::ErrorCode ec;
        std::string err;
        auto h1 = pool->borrow(ec, err);
        check(h1 != nullptr, "借出唯一连接");

        const auto t0 = std::chrono::steady_clock::now();
        auto h2 = pool->borrow(ec, err, std::chrono::milliseconds(150));
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        check(h2 == nullptr, "第二次借出失败（池已满）");
        check(ec == common::ErrorCode::PoolExhausted, "错误码为 PoolExhausted");
        check(pool->stats().borrowTimeouts == 1 && pool->stats().waiting == 0,
              "连接池统计记录一次借出超时且无遗留等待者");
        check(ms >= 140 && ms < 2000, "在超时后返回而非死等（实测 " + std::to_string(ms) + "ms）");
        h1.reset();
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 3. 错误码：连接失败不再被误报为 PoolExhausted ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::connectFails = true;
        auto pool = makePool(0, 4);
        common::ErrorCode ec;
        std::string err;
        auto h = pool->borrow(ec, err);
        check(h == nullptr, "连接失败时借出失败");
        check(ec == common::ErrorCode::ConnectionFailed, "错误码为 ConnectionFailed");
        check(pool->totalCount() == 0, "失败后未虚占名额");
        MockConnection::connectFails = false;
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 4. 心跳：清除死连接并补足到 min ==\n";
    {
        MockConnection::alive = 0;
        auto pool = makePool(2, 4);
        check(MockConnection::alive == 2, "预热出 2 条连接");
        // ping 失败 + 补建也失败：死连接被清除且无法补足
        MockConnection::pingFails = true;
        MockConnection::connectFails = true;
        pool->healthCheck();
        check(pool->totalCount() == 0, "失效连接被清除且未虚占名额");
        check(MockConnection::alive == 0, "所有旧连接都已 close");

        // 故障恢复后，心跳补足回 min
        MockConnection::pingFails = false;
        MockConnection::connectFails = false;
        pool->healthCheck();
        check(MockConnection::alive == 2, "补足回 min=2 条");
        check(pool->idleCount() == 2, "补建的连接进入空闲队列");
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 5. 池已关闭后借出返回 PoolClosed ==\n";
    {
        MockConnection::alive = 0;
        auto pool = makePool(1, 2);
        pool->shutdown(std::chrono::milliseconds(0));
        common::ErrorCode ec;
        std::string err;
        auto h = pool->borrow(ec, err);
        check(h == nullptr, "关闭后借出失败");
        check(ec == common::ErrorCode::PoolClosed, "错误码为 PoolClosed");
    }

    std::cout << "== 6. 生命周期：Handle 比池活得更久也不崩（原 UAF 场景）==\n";
    {
        MockConnection::alive = 0;
        std::unique_ptr<core::ConnectionPool::Handle> leaked;
        {
            auto pool = makePool(1, 2);
            common::ErrorCode ec;
            std::string err;
            leaked = pool->borrow(ec, err);
            check(leaked != nullptr, "借出成功");
        } // 池在此销毁，leaked 仍然持有连接
        check(MockConnection::alive == 1, "池销毁时借出的连接尚未关闭");
        leaked.reset(); // 归还：此时 weak_ptr 已失效，应直接 close 而不是访问已释放内存
        check(MockConnection::alive == 0, "Handle 析构后连接被安全关闭（无 UAF）");
    }

    std::cout << "== 7. 事务：成功提交 ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::resetLog();
        auto pool = makePool(0, 2);
        core::DataSource ds(pool, "mock");
        std::int64_t n = 0;
        auto st = ds.transaction([&](core::Session &s) {
            if (auto r = s.execute("UPDATE a SET v=1", n); !r.ok()) return r;
            return s.execute("UPDATE b SET v=2", n);
        });
        check(st.ok(), "事务成功返回 Ok");
        const std::string log = MockConnection::joined();
        check(log == "connect | begin | execute:UPDATE a SET v=1 | execute:UPDATE b SET v=2 | commit",
              "顺序为 begin -> 两条语句 -> commit\n          实际: " + log);
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 8. 事务：回调返回失败则回滚 ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::resetLog();
        auto pool = makePool(0, 2);
        core::DataSource ds(pool, "mock");
        std::int64_t n = 0;
        auto st = ds.transaction([&](core::Session &s) -> Status {
            if (auto r = s.execute("UPDATE a SET v=1", n); !r.ok()) return r;
            return Status::error(common::ErrorCode::TxError, "boom");
        });
        check(!st.ok(), "事务失败被返回");
        check(st.code == common::ErrorCode::TxError, "错误码透传为 TxError");
        check(MockConnection::joined().find("rollback") != std::string::npos, "触发了 rollback");
        check(MockConnection::joined().find("commit") == std::string::npos, "没有误提交");
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 9. 事务：回调抛异常也回滚 ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::resetLog();
        auto pool = makePool(0, 2);
        core::DataSource ds(pool, "mock");
        auto st = ds.transaction([](core::Session &s) -> Status {
            throw std::runtime_error("kaboom");
        });
        check(!st.ok(), "异常被捕获，未逃逸出 transaction");
        check(st.code == common::ErrorCode::TxError, "错误码为 TxError");
        check(MockConnection::joined().find("rollback") != std::string::npos, "异常路径也回滚了");
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 10. 事务：回调内部自行提交，外层不重复提交 ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::resetLog();
        auto pool = makePool(0, 2);
        core::DataSource ds(pool, "mock");
        std::int64_t n = 0;
        auto st = ds.transaction([&](core::Session &s) -> Status {
            s.execute("UPDATE a SET v=1", n);
            s.commit();
            return s.execute("UPDATE b SET v=2", n);
        });
        check(st.ok(), "自行提交后外层不报错");
        const std::string log = MockConnection::joined();
        check(log.find("commit | execute:UPDATE b SET v=2") != std::string::npos,
              "commit 发生在中间，末尾无二次 commit\n          实际: " + log);
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 11. 参数插值：跳过引号与注释内的 ? ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::resetLog();
        auto pool = makePool(0, 2);
        core::DataSource ds(pool, "mock");
        common::ResultSet rs;
        common::Params p;
        p.emplace_back(std::string("O'Brien"));
        p.emplace_back(std::int64_t(42));
        auto st = ds.query("SELECT * FROM t WHERE name = ? AND note = 'a?b' /* ? */ AND id = ?",
                           p, rs);
        check(st.ok(), "参数化查询成功");
        const std::string got = MockConnection::joined();
        const std::string want =
            "connect | query:SELECT * FROM t WHERE name = 'O''Brien' AND note = 'a?b' /* ? */ AND id = 42";
        check(got == want, "字面量正确转义且注释/引号内的 ? 未被替换\n          实际: " + got +
                           "\n          期望: " + want);
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 12. 参数数量不符应报错而非静默出错 ==\n";
    {
        MockConnection::alive = 0;
        auto pool = makePool(0, 2);
        core::DataSource ds(pool, "mock");
        common::ResultSet rs;
        common::Params p;
        p.emplace_back(std::int64_t(1));
        p.emplace_back(std::int64_t(2));
        auto st = ds.query("SELECT ?", p, rs);
        check(!st.ok(), "参数过多时报错");
        check(st.code == common::ErrorCode::QueryError, "错误码为 QueryError");
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 13. shutdown 宽限期：等待借用中的连接归还 ==\n";
    {
        MockConnection::alive = 0;
        auto pool = makePool(1, 2);
        common::ErrorCode ec;
        std::string err;
        auto h = pool->borrow(ec, err);
        std::thread releaser([&h]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            h.reset(); // 在宽限期内归还
        });
        pool->shutdown(std::chrono::milliseconds(3000));
        releaser.join();
        check(MockConnection::alive == 0, "宽限期内归还的连接被正常关闭");
    }

    std::cout << "== 14. 连接生命周期：到达 maxLifetime 后轮换 ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::resetLog();
        auto pool = std::make_shared<core::ConnectionPool>(
            std::make_unique<MockDriver>(), mockCfg(), 1, 2,
            std::chrono::milliseconds(300), std::chrono::milliseconds(0),
            std::chrono::milliseconds(20), std::chrono::milliseconds(0));
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        common::ErrorCode ec;
        std::string err;
        auto h = pool->borrow(ec, err);
        check(h != nullptr, "过期连接被替换后仍可借出");
        const auto log = MockConnection::joined();
        check(log == "connect | close | connect",
              "借出时关闭过期连接并新建连接\n          实际: " + log);
        const auto stats = pool->stats();
        check(stats.connectionsCreated == 2 && stats.connectionsClosed == 1,
              "生命周期指标记录创建 2、关闭 1");
        h.reset();
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 15. 结构化错误：SQLSTATE 可直接判断重试策略 ==\n";
    {
        const auto deadlock = common::Status::databaseError(
            common::ErrorCode::QueryError, "serialization failure", "40001", 1213);
        check(deadlock.code == common::ErrorCode::Deadlock && deadlock.retryable,
              "序列化冲突被归类为可重试 Deadlock");
        check(deadlock.sqlState == "40001" && deadlock.nativeCode == 1213,
              "保留 SQLSTATE 与厂商错误码");

        const auto constraint = common::Status::databaseError(
            common::ErrorCode::QueryError, "duplicate key", "23505");
        check(constraint.code == common::ErrorCode::ConstraintViolation && !constraint.retryable,
              "约束冲突不可自动重试");

        const auto disconnected = common::Status::databaseError(
            common::ErrorCode::QueryError, "connection lost", "08006");
        check(disconnected.connectionBroken && disconnected.retryable,
              "连接类错误标记 connectionBroken 和 retryable");

        MockConnection connection;
        check(connection.cancel().code == common::ErrorCode::NotSupported,
              "未实现取消的驱动明确返回 NotSupported");
    }

    std::cout << "== 16. 空闲回收：只回收到 min，不破坏池下限 ==\n";
    {
        MockConnection::alive = 0;
        auto pool = std::make_shared<core::ConnectionPool>(
            std::make_unique<MockDriver>(), mockCfg(), 1, 3,
            std::chrono::milliseconds(300), std::chrono::milliseconds(20));
        common::ErrorCode ec;
        std::string err;
        auto h1 = pool->borrow(ec, err);
        auto h2 = pool->borrow(ec, err);
        h1.reset();
        h2.reset();
        check(pool->idleCount() == 2, "测试池中已有两条空闲连接");
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        pool->healthCheck();
        check(pool->totalCount() == 1 && pool->idleCount() == 1,
              "超时空闲连接被回收到 min=1");
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 17. 可观测性：操作事件不依赖 SQL 日志 ==\n";
    {
        int events = 0;
        common::OperationEvent last;
        common::Observability::setObserver([&](const common::OperationEvent &event) {
            ++events;
            last = event;
        });
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "observed");
        common::ResultSet rs;
        const auto status = ds.query("SELECT secret_value", rs);
        check(status.ok() && events == 1, "查询产生一个观测事件");
        check(last.dataSource == "observed" &&
              last.type == common::OperationType::Query && last.rowCount == 1 &&
              last.status.ok(), "事件包含数据源、类型、行数和状态");
        common::Observability::setObserver({});
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 18. 故障连接：connectionBroken 后不再归池 ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::queryBreaks = true;
        auto pool = makePool(1, 1);
        core::DataSource ds(pool, "mock");
        common::ResultSet rs;
        const auto status = ds.query("SELECT 1", rs);
        check(status.connectionBroken, "驱动错误携带 connectionBroken");
        check(pool->totalCount() == 0 && MockConnection::alive == 0,
              "坏连接被立即关闭且未回到连接池");
        MockConnection::queryBreaks = false;
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 19. 事务选项：隔离级别、只读与整体超时 ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::resetLog();
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "mock");
        common::TransactionOptions options;
        options.isolation = common::IsolationLevel::Serializable;
        options.readOnly = true;
        const auto configured = ds.transaction(options, [](core::Session &) {
            return Status::OK();
        });
        check(configured.ok() && MockConnection::joined().find("options:readonly:4") != std::string::npos,
              "隔离级别和只读属性传入驱动");

        options = {};
        options.timeout = std::chrono::milliseconds(20);
        const auto timed = ds.transaction(options, [](core::Session &) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            return Status::OK();
        });
        check(timed.code == common::ErrorCode::QueryTimeout && timed.retryable,
              "事务超过整体期限后返回 QueryTimeout");
        check(MockConnection::joined().find("rollback") != std::string::npos,
              "超时事务执行回滚");
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 20. 安全配置：环境变量密码与热加载清理 ==\n";
    {
        const std::string path = "/tmp/dbmw_config_loader_test.json";
        setenv("DBMW_TEST_PASSWORD", "from-env", 1);
        {
            std::ofstream file(path);
            file << R"({
              "default_datasource": "secure",
              "pool": {"min": 0, "max": 2},
              "datasources": [{
                "name": "secure", "type": "mysql", "host": "localhost",
                "password_env": "DBMW_TEST_PASSWORD"
              }]
            })";
        }
        config::GlobalConfig loaded;
        std::string error;
        const bool first = config::ConfigLoader::loadFromFile(path, loaded, error);
        const bool second = config::ConfigLoader::loadFromFile(path, loaded, error);
        check(first && second && loaded.datasources.size() == 1,
              "重复加载不会累积旧数据源");
        check(loaded.datasources.front().password == "from-env" &&
              loaded.datasources.front().describe().find("from-env") == std::string::npos,
              "密码从环境变量解析且 describe() 不泄漏密码");
        std::remove(path.c_str());
        unsetenv("DBMW_TEST_PASSWORD");
    }

    std::cout << "== 21. 韧性：查询安全重试，写入默认不重试 ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::queryCalls = 0;
        MockConnection::executeCalls = 0;
        MockConnection::queryFailuresRemaining = 2;
        MockConnection::executeFailuresRemaining = 2;
        config::RetryConfig retry;
        retry.max_attempts = 3;
        retry.initial_backoff_ms = 0;
        retry.max_backoff_ms = 0;
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "resilient", retry);
        common::ResultSet rs;
        const auto queryStatus = ds.query("SELECT 1", rs);
        check(queryStatus.ok() && MockConnection::queryCalls == 3,
              "可重试查询在第三次成功");
        std::int64_t affected = 0;
        const auto executeStatus = ds.execute("UPDATE t SET v=1", affected);
        check(!executeStatus.ok() && MockConnection::executeCalls == 1,
              "写入即使错误可重试也默认只执行一次");
        MockConnection::queryFailuresRemaining = 0;
        MockConnection::executeFailuresRemaining = 0;
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 22. 熔断：连续故障快速失败并半开恢复 ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::queryCalls = 0;
        MockConnection::queryFailuresRemaining = 2;
        config::CircuitBreakerConfig breaker;
        breaker.failure_threshold = 2;
        breaker.open_interval_ms = 20;
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "breaker", {}, breaker);
        common::ResultSet rs;
        ds.query("SELECT 1", rs);
        ds.query("SELECT 1", rs);
        const auto open = ds.query("SELECT 1", rs);
        check(open.code == common::ErrorCode::CircuitOpen && MockConnection::queryCalls == 2,
              "熔断开启后不再触达驱动");
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        const auto recovered = ds.query("SELECT 1", rs);
        check(recovered.ok() && MockConnection::queryCalls == 3,
              "冷却后半开探测成功并关闭熔断");
        MockConnection::queryFailuresRemaining = 0;
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 23. 热加载：新配置原子切换并排空旧连接池 ==\n";
    {
        driver::DriverRegistry::instance().registerDriver(
            "mock", [] { return std::make_unique<MockDriver>(); });
        config::GlobalConfig first;
        first.default_datasource = "old";
        first.pool.min = 0;
        first.pool.max = 1;
        first.datasources.push_back(mockCfg("old"));

        config::GlobalConfig second = first;
        second.default_datasource = "new";
        second.datasources.clear();
        second.datasources.push_back(mockCfg("new"));

        core::DatabaseManager manager;
        check(manager.init(first).ok(), "旧配置初始化成功");
        const auto oldSource = manager.getDefault();
        std::atomic<bool> started{false};
        Status inFlight;
        std::thread worker([&] {
            inFlight = oldSource->withSession([&](core::Session &) {
                started = true;
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                return Status::OK();
            });
        });
        while (!started.load()) std::this_thread::yield();
        const auto start = std::chrono::steady_clock::now();
        const auto reloaded = manager.init(second, std::chrono::milliseconds(500));
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        worker.join();
        check(reloaded.ok() && inFlight.ok() && elapsed >= 35 && elapsed < 1000,
              "热加载等待在途会话安全排空");
        common::ResultSet oldResult;
        const auto oldStatus = oldSource->query("SELECT 1", oldResult);
        check(manager.getDefault() && manager.getDefault()->name() == "new" &&
              oldStatus.code == common::ErrorCode::PoolClosed,
              "默认数据源已切换且旧句柄快速返回 PoolClosed");
        manager.shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 24. 读写路由：副本轮询、主库写入与写后读 ==\n";
    {
        auto primaryPool = makePool(0, 1);
        auto replicaPool = makePool(0, 1);
        auto primary = std::make_shared<core::DataSource>(primaryPool, "primary");
        auto replica = std::make_shared<core::DataSource>(replicaPool, "replica");
        core::DataSource group("app", primary, {replica},
                               std::chrono::milliseconds(30), true);
        std::vector<std::string> targets;
        common::Observability::setObserver([&](const common::OperationEvent &event) {
            if (event.type == common::OperationType::Query ||
                event.type == common::OperationType::Execute)
                targets.push_back(event.dataSource);
        });
        common::ResultSet rs;
        std::int64_t affected = 0;
        group.query("SELECT 1", rs);
        group.execute("UPDATE t SET v=1", affected);
        rs.clear();
        group.query("SELECT 1", rs);
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
        rs.clear();
        group.query("SELECT 1", rs);
        common::Observability::setObserver({});
        check(targets == std::vector<std::string>({"replica", "primary", "primary", "replica"}),
              "读走副本、写走主库、写后一致性窗口内读主库");
        primaryPool->shutdown(std::chrono::milliseconds(0));
        replicaPool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 25. 大数据 API：逐行消费与批量执行 ==\n";
    {
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "bulk");
        std::uint64_t rows = 0;
        int callbacks = 0;
        const auto streamed = ds.queryEach(
            "MULTI", {}, [&](const common::Row &) { return ++callbacks < 2; }, rows);
        check(streamed.ok() && callbacks == 2 && rows == 2,
              "逐行回调可提前停止并返回已消费行数");
        rows = 0;
        const auto callbackFailure = ds.queryEach(
            "MULTI", {}, [](const common::Row &) -> bool {
                throw std::runtime_error("consumer failed");
            }, rows);
        check(callbackFailure.code == common::ErrorCode::QueryError && rows == 1,
              "逐行回调异常被转换为状态且不会越过驱动资源清理");

        common::BatchResult batchResult;
        common::ParamBatch batch{
            common::Params{std::int64_t(1)},
            common::Params{std::int64_t(2)},
            common::Params{std::int64_t(3)}};
        const auto batched = ds.executeBatch("UPDATE t SET v=?", batch, batchResult);
        check(batched.ok() && batchResult.affected.size() == 3 &&
              batchResult.totalAffected() == 3,
              "批量执行保留每组影响行数与合计");
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 26. 事务保存点：局部回滚后继续提交 ==\n";
    {
        MockConnection::resetLog();
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "mock");
        const auto status = ds.transaction([](core::Session &session) {
            if (auto st = session.savepoint("before_optional"); !st.ok()) return st;
            std::int64_t affected = 0;
            if (auto st = session.execute("UPDATE optional SET v=1", affected); !st.ok())
                return st;
            if (auto st = session.rollbackToSavepoint("before_optional"); !st.ok()) return st;
            return session.releaseSavepoint("before_optional");
        });
        const auto log = MockConnection::joined();
        check(status.ok() && log.find("savepoint:before_optional") != std::string::npos &&
              log.find("rollback_to:before_optional") != std::string::npos &&
              log.find("release:before_optional | commit") != std::string::npos,
              "保存点创建、局部回滚、释放后事务提交");
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 27. 事务超时：看门狗线程必须吞掉驱动异常（回归：曾 abort 进程） ==\n";
    {
        MockConnection::resetLog();
        MockConnection::cancelUnsupported = false;
        MockConnection::cancelThrows = true;
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "watcher");
        common::TransactionOptions opt;
        opt.timeout = std::chrono::milliseconds(60);
        const auto status = ds.transaction(opt, [](core::Session &) {
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
            return Status::OK();
        });
        // 能走到这一行本身就说明进程没被 std::terminate 干掉。
        check(status.code == common::ErrorCode::QueryTimeout,
              "取消实现抛异常时仍返回 QueryTimeout，而不是终止进程");
        check(status.message.find("could not cancel") != std::string::npos,
              "取消没生效就如实说明，不假装已中断");
        check(MockConnection::joined().find("rollback") != std::string::npos,
              "取消失败后事务照样回滚");
        MockConnection::cancelThrows = false;
        MockConnection::cancelUnsupported = true;
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 28. 事务超时：驱动支持取消时如实上报已中断 ==\n";
    {
        MockConnection::resetLog();
        MockConnection::cancelUnsupported = false;
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "cancellable");
        common::TransactionOptions opt;
        opt.timeout = std::chrono::milliseconds(60);
        const auto status = ds.transaction(opt, [](core::Session &) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            return Status::OK();
        });
        check(status.code == common::ErrorCode::QueryTimeout &&
              status.message.find("could not cancel") == std::string::npos,
              "取消成功时错误信息不含 could not cancel");
        check(MockConnection::joined().find("cancel") != std::string::npos,
              "到期确实调用了驱动的取消原语");
        // 驱动不支持取消时（基类默认 NotSupported）也必须如实说明。
        MockConnection::resetLog();
        MockConnection::cancelUnsupported = true;
        const auto unsupporting = ds.transaction(opt, [](core::Session &) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            return Status::OK();
        });
        check(unsupporting.code == common::ErrorCode::QueryTimeout &&
              unsupporting.message.find("could not cancel") != std::string::npos,
              "驱动不支持取消时不谎报已中断（超时是软上限，不是硬中断）");
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 29. 熔断：事务与会话同样受保护（回归：曾完全绕过闸门） ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::resetLog();
        MockConnection::queryFailuresRemaining = 2;
        config::CircuitBreakerConfig breaker;
        breaker.failure_threshold = 2;
        breaker.open_interval_ms = 5000;
        auto pool = makePool(0, 2);
        core::DataSource ds(pool, "tx-breaker", {}, breaker);
        int reached = 0;
        for (int i = 0; i < 2; ++i) {
            ds.transaction([&](core::Session &s) {
                ++reached;
                common::ResultSet rs;
                return s.query("SELECT 1", rs);
            });
        }
        const auto blocked = ds.transaction(
            [&](core::Session &) { ++reached; return Status::OK(); });
        check(blocked.code == common::ErrorCode::CircuitOpen && reached == 2,
              "熔断开启后事务快速失败，不再触达驱动（实际触达 "
              + std::to_string(reached) + " 次）");
        const auto sessionBlocked = ds.withSession([](core::Session &) { return Status::OK(); });
        check(sessionBlocked.code == common::ErrorCode::CircuitOpen,
              "熔断开启后 withSession 同样快速失败");
        MockConnection::queryFailuresRemaining = 0;
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 30. 读写分离：withSession 里的写同样触发写后读 ==\n";
    {
        auto primaryPool = makePool(0, 1);
        auto replicaPool = makePool(0, 1);
        auto primary = std::make_shared<core::DataSource>(primaryPool, "primary");
        auto replica = std::make_shared<core::DataSource>(replicaPool, "replica");
        core::DataSource group("app", primary, {replica},
                               std::chrono::milliseconds(300), true);
        std::vector<std::string> targets;
        common::Observability::setObserver([&](const common::OperationEvent &event) {
            if (event.type == common::OperationType::Query) targets.push_back(event.dataSource);
        });
        common::ResultSet rs;
        group.query("SELECT 1", rs);
        group.withSession([](core::Session &s) {
            std::int64_t affected = 0;
            return s.execute("UPDATE t SET v=1", affected);
        });
        rs.clear();
        group.query("SELECT 2", rs);
        common::Observability::setObserver({});
        std::string actual;
        for (const auto &t: targets) { if (!actual.empty()) actual += ","; actual += t; }
        check(targets == std::vector<std::string>({"replica", "primary"}),
              "withSession 中写过后，窗口内的读打到主库（回归：曾打到从库读到旧数据）"
              "\n          实际: " + actual);
        primaryPool->shutdown(std::chrono::milliseconds(0));
        replicaPool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 31. 批量执行：中途失败整批回滚（回归：MySQL/ODBC 曾留部分写入） ==\n";
    {
        MockConnection::resetLog();
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "atomic-batch");
        common::ParamBatch batch{
            common::Params{std::int64_t(1)},
            common::Params{std::int64_t(2)},
            common::Params{std::int64_t(3)}};
        MockConnection::executeOkBeforeFail = 2; // 前两组成功，第三组失败
        common::BatchResult result;
        const auto failed = ds.executeBatch("UPDATE t SET v=?", batch, result);
        const auto log = MockConnection::joined();
        check(!failed.ok() && result.affected.empty(),
              "批量失败后不保留部分影响行数（已回滚，留着会误导调用方）");
        check(log.find("rollback") != std::string::npos && log.find("commit") == std::string::npos,
              "中途失败触发回滚而不是提交\n          实际: " + log);

        MockConnection::resetLog();
        MockConnection::executeOkBeforeFail = -1;
        const auto inTx = ds.transaction([&](core::Session &s) {
            common::BatchResult inner;
            return s.executeBatch("UPDATE t SET v=?", batch, inner);
        });
        const auto log2 = MockConnection::joined();
        check(inTx.ok() && log2.find("begin | execute:UPDATE t SET v=1") != std::string::npos,
              "已在事务中时批量执行沿用外层事务，不再嵌套 begin\n          实际: " + log2);
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 32. 重试退避：抖动必须真随机（回归：曾用 std::hash<thread::id>） ==\n";
    {
        MockConnection::alive = 0;
        MockConnection::queryFailuresRemaining = 99;
        config::RetryConfig retry;
        retry.max_attempts = 2;
        retry.initial_backoff_ms = 120; // 抖动区间约 [0,31)ms
        retry.max_backoff_ms = 1000;
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "jitter", retry);
        std::vector<long long> samples;
        common::ResultSet rs;
        for (int i = 0; i < 6; ++i) {
            const auto t0 = std::chrono::steady_clock::now();
            ds.query("SELECT 1", rs);
            samples.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - t0).count());
        }
        const std::set<long long> distinct(samples.begin(), samples.end());
        // 6 次采样全部撞到同一抖动值的概率约 1e-6，不会误报。
        check(distinct.size() > 1,
              "多次退避时长互不相同（实测 " + std::to_string(distinct.size())
              + "/6 种；恒定抖动会让并发重试整齐惊群）");
        const auto minUs = *std::min_element(samples.begin(), samples.end());
        const auto maxUs = *std::max_element(samples.begin(), samples.end());
        check(minUs >= 120000 && maxUs < 400000,
              "退避落在 [基础, 基础+区间] 内（实测 "
              + std::to_string(minUs / 1000) + "ms ~ " + std::to_string(maxUs / 1000) + "ms）");
        MockConnection::queryFailuresRemaining = 0;
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 33. 连接池：建连/校验失败必须释放名额 ==\n";
    {
        MockConnection::alive = 0;
        auto pool = makePool(0, 1);
        common::ErrorCode ec;
        std::string err;

        MockConnection::connectFails = true;
        auto bad = pool->borrow(ec, err);
        check(bad == nullptr && ec == common::ErrorCode::ConnectionFailed,
              "建连失败返回 ConnectionFailed");
        check(pool->totalCount() == 0,
              "建连失败后名额被释放，池不会被永久占满（实测 total="
              + std::to_string(pool->totalCount()) + "）");

        MockConnection::connectFails = false;
        auto good = pool->borrow(ec, err);
        check(good != nullptr, "名额已释放，后续借出可以重新建连");
        good.reset();

        MockConnection::resetLog();
        MockConnection::pingFails = true;
        auto recovered = pool->borrow(ec, err);
        check(recovered != nullptr &&
              MockConnection::joined().find("close | connect") != std::string::npos,
              "ping 失败的连接被丢弃并换新\n          实际: " + MockConnection::joined());
        check(pool->totalCount() == 1,
              "失效连接没有留在总额里（实测 total=" + std::to_string(pool->totalCount()) + "）");
        recovered.reset();
        MockConnection::pingFails = false;
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 34. 事务契约：重复 begin 报错，会话结束不留悬挂事务 ==\n";
    {
        MockConnection::resetLog();
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "reentrant");
        Status second;
        const auto outer = ds.withSession([&](core::Session &s) {
            const auto first = s.begin();
            second = s.begin(); // 事务已开启，必须被拒绝
            return first;
        });
        check(outer.ok() && second.code == common::ErrorCode::TxError,
              "事务已开启时再次 begin 返回 TxError"
              "（MySQL 过去会隐式 COMMIT 上一事务，静默丢数据）");

        MockConnection::resetLog();
        ds.withSession([](core::Session &s) { return s.begin(); });
        check(MockConnection::joined().find("rollback") != std::string::npos,
              "会话结束时事务仍开着会自动回滚，不会把半个事务的连接还池"
              "\n          实际: " + MockConnection::joined());
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 35. SQL 诊断：完整参数渲染与默认脱敏 ==\n";
    {
        common::Observability::clearSlowSqlStats();
        config::ObservabilityConfig observability;
        observability.slow_sql.enabled = true;
        observability.slow_sql.threshold_ms = 0;
        observability.slow_sql.retain_rendered_sql = true;
        observability.sql_log.enabled = true;
        observability.sql_log.mode = "full";
        observability.sql_log.level = "info";
        observability.sql_log.include_string_values = true;
        common::Observability::configure(observability);

        common::OperationEvent captured;
        common::Observability::setObserver(
            [&](const common::OperationEvent &event) { captured = event; });
        auto pool = makePool(0, 1);
        core::DataSource ds(pool, "diagnostics");
        common::ResultSet rs;
        const common::Params first{std::string("O'Brien"), std::int64_t(42)};
        std::ostringstream sqlLog;
        auto *previousLogBuffer = std::cerr.rdbuf(sqlLog.rdbuf());
        const auto status = ds.query(
            "SELECT * FROM users WHERE name=? AND id=?", first, rs);
        std::cerr.rdbuf(previousLogBuffer);
        check(status.ok() && captured.renderedSql ==
              "SELECT * FROM users WHERE name='O''Brien' AND id=42",
              "完整 SQL 使用驱动字面量规则渲染字符串和数值参数");
        check(sqlLog.str().find(
                  "statement=SELECT * FROM users WHERE name='O''Brien' AND id=42") !=
              std::string::npos,
              "sql_log.mode=full 时日志实际打印完整带参 SQL");
        check(captured.sqlTemplate == "SELECT * FROM users WHERE name=? AND id=?" &&
              captured.slow && captured.sqlFingerprint != 0,
              "观测事件包含模板、慢 SQL 标记和稳定指纹");

        observability.sql_log.enabled = false;
        common::Observability::configure(observability);

        rs.clear();
        ds.query("SELECT * FROM users WHERE name=? AND id=?",
                 common::Params{std::string("Bob"), std::int64_t(7)}, rs);
        const auto aggregates = common::Observability::slowSqlStats();
        const auto recent = common::Observability::recentSlowSql();
        check(aggregates.size() == 1 && aggregates.front().count == 2 &&
              aggregates.front().histogram.size() ==
                  aggregates.front().histogramBucketsMs.size() + 1,
              "相同参数化模板聚合为一条慢 SQL 并维护耗时直方图");
        check(recent.size() == 2 && recent.front().renderedSql.find("'Bob'") !=
              std::string::npos,
              "最近慢 SQL 按时间倒序保留配置允许的完整 SQL");
        rs.clear();
        ds.query("SELECT $$literal ?$$ AS marker, ? AS value",
                 common::Params{std::int64_t(9)}, rs);
        check(captured.renderedSql ==
              "SELECT $$literal ?$$ AS marker, 9 AS value",
              "PostgreSQL dollar-quoted 文本里的问号不会被误当作占位符");

        observability.sql_log.include_string_values = false;
        common::Observability::configure(observability);
        common::Observability::clearSlowSqlStats();
        rs.clear();
        ds.query("SELECT * FROM users WHERE token=?",
                 common::Params{std::string("secret-token")}, rs);
        const auto redacted = common::Observability::recentSlowSql();
        check(redacted.size() == 1 && redacted.front().renderedSql.find("secret-token") ==
              std::string::npos && redacted.front().renderedSql.find("<redacted>") !=
              std::string::npos,
              "字符串参数默认可脱敏，完整 SQL 不会意外泄漏敏感值");
        common::Observability::setObserver({});
        common::Observability::configure(config::ObservabilityConfig{});
        common::Observability::clearSlowSqlStats();
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 36. 连接池指标：容量、利用率、高水位和借出耗时 ==\n";
    {
        auto pool = makePool(0, 2);
        common::ErrorCode ec;
        std::string error;
        auto first = pool->borrow(ec, error);
        auto second = pool->borrow(ec, error);
        const auto busy = pool->stats();
        check(busy.minConnections == 0 && busy.maxConnections == 2 &&
              busy.borrowed == 2 && busy.utilization() == 1.0,
              "连接池快照包含容量、当前借用量和利用率");
        check(busy.borrowRequests == 2 && busy.borrowSuccesses == 2 &&
              busy.maxBorrowed == 2 && busy.totalBorrowWait.count() >= 0,
              "连接池累计借出次数、借用高水位和等待耗时正确更新");
        first.reset();
        second.reset();
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 37. 可观测配置：解析、边界校验与物理池列表 ==\n";
    {
        const std::string path = "/tmp/dbmw_observability_test.json";
        {
            std::ofstream file(path);
            file << R"({
              "default_datasource":"metrics",
              "observability":{
                "sql_log":{"enabled":true,"mode":"full","sample_rate":0.5,
                           "include_string_values":true},
                "slow_sql":{"enabled":true,"threshold_ms":123,
                            "aggregate_capacity":12,"recent_capacity":5,
                            "histogram_buckets_ms":[10,100,1000]},
                "pool_metrics":{"enabled":true}
              },
              "datasources":[{"name":"metrics","type":"mock"}]
            })";
        }
        config::GlobalConfig parsed;
        std::string error;
        const bool loaded = config::ConfigLoader::loadFromFile(path, parsed, error);
        check(loaded && parsed.observability.sql_log.mode == "full" &&
              parsed.observability.sql_log.include_string_values &&
              parsed.observability.slow_sql.threshold_ms == 123 &&
              parsed.observability.slow_sql.histogram_buckets_ms.size() == 3,
              "观测配置从 JSON 完整解析并保留安全开关与容量限制");

        core::DatabaseManager manager;
        const auto initialized = manager.init(parsed);
        const auto pools = manager.allPoolStats();
        check(initialized.ok() && pools.size() == 1 &&
              pools.front().dataSource == "metrics" &&
              pools.front().stats.maxConnections ==
                  static_cast<std::size_t>(parsed.pool.max),
              "全部连接池接口返回物理数据源明细而不是只给组聚合值");
        manager.shutdown(std::chrono::milliseconds(0));
        common::Observability::configure(config::ObservabilityConfig{});
        std::remove(path.c_str());
    }

    // ---- 统计/可观测性修复回归测试（对应第 5 轮审查结论 P1-1/P1-2/P1-3/P2）----
    std::cout << "== 38. 慢 SQL 聚合：字面量归一化避免指纹爆炸 (P1-2) ==\n";
    {
        common::Observability::clearSlowSqlStats();
        config::ObservabilityConfig obs;
        obs.slow_sql.enabled = true;
        obs.slow_sql.threshold_ms = 0;       // 全部算慢，便于聚合
        obs.slow_sql.aggregate_capacity = 100;
        obs.slow_sql.recent_capacity = 100;
        obs.slow_sql.histogram_buckets_ms = {10, 100, 1000};
        common::Observability::configure(obs);
        std::vector<std::uint64_t> eventFingerprints;
        common::Observability::setObserver([&](const common::OperationEvent &event) {
            eventFingerprints.push_back(event.sqlFingerprint);
        });

        // 仅字面量不同的同类查询应聚合为同一条（字符串拼接型 SQL 不再撑爆聚合表）
        const std::vector<std::string> variants = {
            "SELECT * FROM t WHERE id=1",
            "SELECT * FROM t WHERE id=2",
            "SELECT * FROM t WHERE id=3",
            "SELECT * FROM t WHERE id=99"};
        for (const auto &sql: variants) {
            common::OperationEvent e;
            e.dataSource = "db";
            e.type = common::OperationType::Query;
            e.status = common::Status::OK();
            e.duration = std::chrono::milliseconds(5);
            common::Observability::emitSql(e, sql);
        }
        auto agg = common::Observability::slowSqlStats(1000);
        check(agg.size() == 1 && agg.front().count == variants.size(),
              "不同字面量的同类查询聚合为一条（实测 " + std::to_string(agg.size())
              + " 条，count=" + std::to_string(agg.empty() ? 0 : agg.front().count) + "）");
        check(!eventFingerprints.empty() &&
              std::all_of(eventFingerprints.begin(), eventFingerprints.end(),
                          [&](const auto fp) { return fp == agg.front().fingerprint; }),
              "Observer、日志查询键和慢 SQL 聚合使用同一结构指纹");
        check(agg.front().sqlTemplate == "SELECT * FROM t WHERE id=?",
              "聚合模板保存结构 SQL，不保留第一条查询的真实字面量");

        // 结构不同的查询应保持独立
        common::OperationEvent u;
        u.dataSource = "db";
        u.type = common::OperationType::Query;
        u.status = common::Status::OK();
        u.duration = std::chrono::milliseconds(5);
        common::Observability::emitSql(u, "UPDATE t SET v=1 WHERE id=5");
        agg = common::Observability::slowSqlStats(1000);
        check(agg.size() == 2, "结构不同的查询保持独立（实测 "
              + std::to_string(agg.size()) + " 条）");
        check(agg.front().histogram.size() == agg.front().histogramBucketsMs.size() + 1,
              "聚合项的直方图长度与分桶配置一致");

        common::Observability::clearSlowSqlStats();
        common::Observability::emitSql(u, "SELECT * FROM users WHERE token='secret-a'");
        common::Observability::emitSql(u, "SELECT * FROM users WHERE token='secret-b'");
        agg = common::Observability::slowSqlStats(1000);
        check(agg.size() == 1 && agg.front().count == 2 &&
              agg.front().sqlTemplate.find("secret-") == std::string::npos,
              "retain_rendered_sql=false 时慢 SQL 模板不泄漏拼接字符串中的敏感值");

        common::Observability::clearSlowSqlStats();
        common::Observability::emitSql(
            u, "DO $body$ BEGIN RAISE NOTICE 'first'; END $body$");
        common::Observability::emitSql(
            u, "DO $body$ BEGIN RAISE NOTICE 'second'; END $body$");
        agg = common::Observability::slowSqlStats(1000);
        check(agg.size() == 2 &&
              std::all_of(agg.begin(), agg.end(), [](const auto &item) {
                  return item.sqlTemplate.find("RAISE NOTICE") == std::string::npos &&
                         item.sqlTemplate.find("<body_hash:") != std::string::npos;
              }),
              "PostgreSQL tagged dollar-quoted 内容以哈希区分且不会泄漏代码块原文");

        common::Observability::setObserver({});
        common::Observability::clearSlowSqlStats();
        common::Observability::configure(config::ObservabilityConfig{});
    }

    std::cout << "== 39. SQL 截断：诊断标记不破坏字符串字面量 (P2) ==\n";
    {
        common::Observability::clearSlowSqlStats();
        config::ObservabilityConfig obs;
        obs.sql_log.enabled = true;
        obs.sql_log.mode = "template";
        obs.sql_log.slow_only = false;
        obs.sql_log.log_success = true;
        obs.sql_log.sample_rate = 1.0;
        obs.sql_log.max_sql_length = 24; // 短到能截断进字符串字面量内部
        common::Observability::configure(obs);

        common::OperationEvent captured;
        bool fired = false;
        common::Observability::setObserver([&](const common::OperationEvent &e) {
            captured = e;
            fired = true;
        });
        common::OperationEvent e;
        e.dataSource = "db";
        e.type = common::OperationType::Query;
        e.status = common::Status::OK();
        e.duration = std::chrono::milliseconds(1);
        const std::string sql = "SELECT 'a fairly long string literal that is truncated'";
        common::Observability::emitSql(e, sql);

        // 截断点若落在字符串字面量内部，marker 之前应补一个右引号，使单引号成对。
        auto balancedQuotes = [](const std::string &s) {
            int depth = 0;
            bool esc = false;
            for (std::size_t i = 0; i < s.size(); ++i) {
                const char c = s[i];
                if (esc) { esc = false; continue; }
                if (c == '\\') { esc = true; continue; }
                if (c == '\'') {
                    if (i + 1 < s.size() && s[i + 1] == '\'') { ++i; continue; }
                    depth += (depth == 0) ? 1 : -1;
                }
            }
            return depth == 0;
        };
        check(fired && balancedQuotes(captured.sqlTemplate),
              "被截断的 SQL 模板中单引号成对，诊断标记不会塞进字符串字面量\n"
              "          实际: " + captured.sqlTemplate);

        common::Observability::setObserver({});
        common::Observability::clearSlowSqlStats();
        common::Observability::configure(config::ObservabilityConfig{});
    }

    std::cout << "== 40. 慢 SQL 排行：按平均耗时排序而非累计耗时 (P2-8) ==\n";
    {
        common::Observability::clearSlowSqlStats();
        config::ObservabilityConfig obs;
        obs.slow_sql.enabled = true;
        obs.slow_sql.threshold_ms = 0;
        obs.slow_sql.aggregate_capacity = 100;
        obs.slow_sql.recent_capacity = 100;
        obs.slow_sql.histogram_buckets_ms = {10, 100, 1000};
        common::Observability::configure(obs);

        common::OperationEvent slow;
        slow.dataSource = "db";
        slow.type = common::OperationType::Query;
        slow.status = common::Status::OK();
        slow.duration = std::chrono::milliseconds(1000); // 单次很慢，只跑一次
        common::Observability::emitSql(slow, "SELECT very_slow_once");

        for (int i = 0; i < 1000; ++i) { // 单次很快但跑很多次，累计耗时大
            common::OperationEvent fast;
            fast.dataSource = "db";
            fast.type = common::OperationType::Query;
            fast.status = common::Status::OK();
            fast.duration = std::chrono::milliseconds(5);
            common::Observability::emitSql(fast, "SELECT frequent_fast");
        }
        auto agg = common::Observability::slowSqlStats(1000);
        check(agg.size() == 2, "两条查询各自聚合");
        check(agg[0].sqlTemplate == "SELECT very_slow_once" && agg[0].count == 1 &&
              agg[0].totalDuration >= std::chrono::milliseconds(1000),
              "榜首按平均耗时（单次 1000ms）而非累计耗时排序\n"
              "          榜首: " + agg[0].sqlTemplate);

        common::Observability::clearSlowSqlStats();
        common::Observability::configure(config::ObservabilityConfig{});
    }

    std::cout << "== 41. 连接池指标：借出超时也计入等待耗时 (P1-3) ==\n";
    {
        MockConnection::alive = 0;
        auto pool = makePool(0, 1, 150); // 最多 1 条，超时 150ms
        common::ErrorCode ec;
        std::string err;
        auto h1 = pool->borrow(ec, err);
        check(h1 != nullptr, "借出唯一连接");

        const auto before = pool->stats();
        auto h2 = pool->borrow(ec, err, std::chrono::milliseconds(150));
        check(h2 == nullptr && ec == common::ErrorCode::PoolExhausted, "池满时借出超时失败");
        const auto after = pool->stats();
        check(after.borrowRequests == before.borrowRequests + 1, "超时的借出也计入 borrowRequests");
        check(after.borrowSuccesses == before.borrowSuccesses, "超时的借出不计入 borrowSuccesses");
        check(after.borrowTimeouts == before.borrowTimeouts + 1, "记录一次借出超时");
        // 关键：等待耗时（含失败这次）应覆盖到 ~150ms，而不是只统计成功借用的近 0 等待
        check(after.totalBorrowWait >= std::chrono::milliseconds(140) &&
              after.maxBorrowWait >= std::chrono::milliseconds(140),
              "totalBorrowWait / maxBorrowWait 包含失败借用的等待（实测 max="
              + std::to_string(after.maxBorrowWait.count()) + "us）");

        h1.reset();
        pool->shutdown(std::chrono::milliseconds(0));
    }

    std::cout << "== 42. 观测全关：emitSql 零开销短路且不丢观察者回调 (P1-1) ==\n";
    {
        common::Observability::clearSlowSqlStats();
        config::ObservabilityConfig obs; // 慢 SQL 与 SQL 日志都关，但注册观察者
        obs.slow_sql.enabled = false;
        obs.sql_log.enabled = false;
        common::Observability::configure(obs);

        int events = 0;
        std::uint64_t capturedFp = 1;
        common::Observability::setObserver([&](const common::OperationEvent &e) {
            ++events;
            capturedFp = e.sqlFingerprint;
        });
        common::OperationEvent e;
        e.dataSource = "db";
        e.type = common::OperationType::Query;
        e.status = common::Status::OK();
        e.duration = std::chrono::milliseconds(10);
        common::Observability::emitSql(e, "SELECT 1 FROM t WHERE id=12345");
        check(events == 1, "无慢 SQL/日志配置时观察者回调仍被调用");
        check(capturedFp == 0, "观测全关时跳过指纹计算（零开销短路）");
        check(common::Observability::slowSqlStats(1000).empty(),
              "观测全关时不产生任何慢 SQL 聚合");

        // 彻底关闭（无观察者）：emitSql 应为空操作且不抛异常
        common::Observability::setObserver({});
        common::Observability::configure(config::ObservabilityConfig{});
        bool threw = false;
        try { common::Observability::emitSql(e, "SELECT 1"); } catch (...) { threw = true; }
        check(!threw && common::Observability::slowSqlStats(1000).empty(),
              "观测彻底关闭时 emitSql 为空操作且不抛异常");

        common::Observability::setObserver({});
        common::Observability::configure(config::ObservabilityConfig{});
        common::Observability::clearSlowSqlStats();
    }

    std::cout << "== 43. 慢 SQL 热加载：分桶变化后统计保持同一时间范围 ==\n";
    {
        common::Observability::clearSlowSqlStats();
        config::ObservabilityConfig obs;
        obs.slow_sql.enabled = true;
        obs.slow_sql.threshold_ms = 0;
        obs.slow_sql.histogram_buckets_ms = {10, 100};
        common::Observability::configure(obs);
        common::OperationEvent event;
        event.dataSource = "db";
        event.type = common::OperationType::Query;
        event.status = common::Status::OK();
        event.duration = std::chrono::milliseconds(20);
        common::Observability::emitSql(event, "SELECT * FROM t WHERE id=1");
        common::Observability::emitSql(event, "SELECT * FROM t WHERE id=2");

        obs.slow_sql.histogram_buckets_ms = {5, 50, 500};
        common::Observability::configure(obs);
        check(common::Observability::slowSqlStats(1000).empty(),
              "无法重分桶的旧聚合在配置热更新时整体清除，而不是只清空 histogram");

        common::Observability::emitSql(event, "SELECT * FROM t WHERE id=3");
        const auto stats = common::Observability::slowSqlStats(1000);
        const auto histogramTotal = stats.empty() ? 0ULL :
            std::accumulate(stats.front().histogram.begin(), stats.front().histogram.end(), 0ULL);
        check(stats.size() == 1 && stats.front().count == 1 && histogramTotal == 1,
              "热更新后的 count、累计耗时和直方图从同一批样本重新开始");
        common::Observability::clearSlowSqlStats();
        common::Observability::configure(config::ObservabilityConfig{});
    }

    std::cout << "\n----------------------------------------\n";
    std::cout << "通过 " << g_passed << " 项，失败 " << g_failed << " 项\n";
    return g_failed == 0 ? 0 : 1;
}
