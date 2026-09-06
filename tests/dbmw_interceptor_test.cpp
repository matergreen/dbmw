// M1 SPI 拦截器层单测：覆盖 InterceptorRegistry 生命周期、I11 异常吞掉、
// I9 分层（开关 false 时零分发）、I7 递归防护。
//
// 注：detail::runOnRoute / runBeforeExecution / runAfterExecution 这些函数
// 没有专属头文件但有 external linkage（前向声明后即可链接），是 dbmw
// 测试拦截器内部调度的标准入口——避免在头文件暴露实现细节。
#include "dbmw/core/interceptor.h"
#include "dbmw/common/context.h"

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace dbmw;
using namespace dbmw::core;
using namespace dbmw::common;

namespace dbmw::core::detail {
    void runOnRoute(const std::string &dataSource, const std::string &sql,
                    common::OperationType type, common::SqlContext &ctx);
    common::Status runBeforeExecution(const ExecutionView &view);
    void runAfterExecution(const ExecutionView &view);
    std::size_t currentInterceptorDepth() noexcept;
}

static int g_failed = 0;
static int g_passed = 0;

static void check(bool cond, const std::string &name) {
    if (cond) { ++g_passed; std::cout << "  [PASS] " << name << "\n"; }
    else { ++g_failed; std::cout << "  [FAIL] " << name << "\n"; }
}

// 测试用拦截器：把每次回调写进一个 call_log，按顺序记录 (event, sql/type)。
struct RecordingInterceptor : public ISqlInterceptor {
    std::vector<std::string> log;
    bool rejectBefore = false;   // beforeExecution 强制返回 ErrorCode

    void onRoute(const std::string &ds, const std::string &sql,
                 common::OperationType type, common::SqlContext &ctx) override {
        log.push_back("onRoute:" + ds + ":" + std::to_string(static_cast<int>(type)) + ":" + sql);
        // 路由期可写 ctx：测试里把 traceId 透传（演示可写字段）
        ctx.traceId = "filled-by-interceptor";
    }

    common::Status beforeExecution(const ExecutionView &view) override {
        log.push_back("beforeExecution:" + view.dataSource + ":" + view.sql);
        if (rejectBefore) return common::Status::error(
                common::ErrorCode::SqlBlocked, "rejected by test");
        return common::Status::OK();
    }

    void afterExecution(const ExecutionView &view) override {
        log.push_back("afterExecution:" + view.dataSource + ":" + view.sql);
    }

    void onCompletion(const ExecutionView &view) override {
        log.push_back("onCompletion:" + view.dataSource + ":" + view.sql);
    }
};

// 会主动抛异常的拦截器：验证 I11（异常吞掉，绝不传播）。
struct ThrowingInterceptor : public ISqlInterceptor {
    void onRoute(const std::string &, const std::string &,
                 common::OperationType, common::SqlContext &) override {
        throw std::runtime_error("onRoute boom");
    }
    common::Status beforeExecution(const ExecutionView &) override {
        throw std::runtime_error("beforeExecution boom");
    }
    void afterExecution(const ExecutionView &) override {
        throw std::runtime_error("afterExecution boom");
    }
    void onCompletion(const ExecutionView &) override {
        throw std::runtime_error("onCompletion boom");
    }
};

struct ConcurrentInterceptor : public ISqlInterceptor {
    std::atomic<int> beforeCount{0};
    std::atomic<int> completionCount{0};
    std::mutex mutex;
    std::condition_variable cv;

    void onRoute(const std::string &, const std::string &,
                 common::OperationType, common::SqlContext &) override {}
    common::Status beforeExecution(const ExecutionView &) override {
        ++beforeCount;
        cv.notify_all();
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(1), [&] { return beforeCount.load() == 2; });
        return common::Status::OK();
    }
    void afterExecution(const ExecutionView &) override {}
    void onCompletion(const ExecutionView &) override { ++completionCount; }
};

struct ReentrantRouteInterceptor : public ISqlInterceptor {
    std::atomic<int> routeCount{0};
    void onRoute(const std::string &ds, const std::string &sql,
                 common::OperationType type, common::SqlContext &ctx) override {
        if (++routeCount == 1) detail::runOnRoute(ds, sql, type, ctx);
    }
    common::Status beforeExecution(const ExecutionView &) override {
        return common::Status::OK();
    }
    void afterExecution(const ExecutionView &) override {}
    void onCompletion(const ExecutionView &) override {}
};

int main() {
    // ---- 清理可能残留的状态（其他用例共享同一进程级注册表）----
    InterceptorRegistry::clear();
    InterceptorRegistry::setEnabled(false);

    std::cout << "== M1 SPI：注册表 + 开关 + 默认关 ==\n";
    {
        check(!InterceptorRegistry::enabled(),
              "默认 enabled() = false（热路径零分配）");
        InterceptorRegistry::setEnabled(true);
        check(InterceptorRegistry::enabled(), "setEnabled(true) 后立即可见");
        InterceptorRegistry::setEnabled(false);
        check(!InterceptorRegistry::enabled(), "setEnabled(false) 后立即可见");

        auto itc = std::make_shared<RecordingInterceptor>();
        InterceptorRegistry::add(itc);
        const auto snap = InterceptorRegistry::snapshot();
        check(snap.size() == 1 && snap[0].get() == itc.get(),
              "add 后 snapshot 返回该拦截器");

        auto itc2 = std::make_shared<RecordingInterceptor>();
        InterceptorRegistry::add(itc2);
        check(InterceptorRegistry::snapshot().size() == 2,
              "两次 add 后 snapshot 含两个拦截器");

        InterceptorRegistry::clear();
        check(InterceptorRegistry::snapshot().empty(), "clear 后 snapshot 为空");

        // 两次连续 enabled 翻转之间的时序：不影响隔离
        InterceptorRegistry::setEnabled(false);
    }

    std::cout << "== M1 SPI：开关关时全路径零分发 ==\n";
    {
        InterceptorRegistry::clear();
        InterceptorRegistry::setEnabled(false);
        auto rec = std::make_shared<RecordingInterceptor>();
        InterceptorRegistry::add(rec);

        SqlContext ctx;
        ctx.traceId = "pre";
        const std::string ds = "ds-zero";
        const std::string sql = "SELECT 1";
        detail::runOnRoute(ds, sql, common::OperationType::Query, ctx);

        SqlContext emptyCtx;
        ExecutionView empty{ds, sql, common::OperationType::Query,
                            nullptr, nullptr, 0, std::chrono::microseconds{0},
                            common::Status::OK(), false, 0, emptyCtx};
        const auto beforeSt = detail::runBeforeExecution(empty);
        detail::runAfterExecution(empty);

        check(rec->log.empty(),
              "开关 false 时 onRoute/beforeExecution/afterExecution 都不被调用");
        check(beforeSt.ok(),
              "开关 false 时 runBeforeExecution 返回 OK（不短路拦截逻辑）");
        check(ctx.traceId == "pre",
              "开关 false 时不动 ctx（路由层的可写字段不被覆盖）");
    }

    std::cout << "== M1 SPI：开关开时按注册顺序回调 + ctx 可写 ==\n";
    {
        InterceptorRegistry::clear();
        InterceptorRegistry::setEnabled(false); // 先关，add 完再开避免污染

        auto first = std::make_shared<RecordingInterceptor>();
        auto second = std::make_shared<RecordingInterceptor>();
        InterceptorRegistry::add(first);
        InterceptorRegistry::add(second);
        InterceptorRegistry::setEnabled(true);

        SqlContext ctx;
        detail::runOnRoute("ds-order", "SELECT 2", common::OperationType::Query, ctx);

        check(first->log.size() == 1 && second->log.size() == 1,
              "两个拦截器都被调用一次");
        check(first->log[0].rfind("onRoute:ds-order:", 0) == 0 &&
              second->log[0].rfind("onRoute:ds-order:", 0) == 0,
              "回调都拿到正确的 ds/type/sql");
        check(ctx.traceId == "filled-by-interceptor",
              "onRoute 中对 ctx 的写入对调用方可见");

        SqlContext ctx2;
        const std::string ds = "ds-order";
        const std::string sql = "SELECT 2";
        SqlContext captured; // 用作 ExecutionView.ctx 的存储
        ExecutionView view{ds, sql, common::OperationType::Query,
                           nullptr, nullptr, 7, std::chrono::microseconds{123},
                           common::Status::OK(), false, 0, captured};
        const auto beforeSt = detail::runBeforeExecution(view);
        detail::runAfterExecution(view);

        check(beforeSt.ok(), "两个拦截器都没拒绝 → beforeExecution 返回 OK");
        check(first->log.size() == 3 && second->log.size() == 3,
              "三次分发（onRoute + before + after）后两拦截器各记录 3 项");
        check(first->log[1].rfind("beforeExecution:", 0) == 0 &&
              second->log[1].rfind("beforeExecution:", 0) == 0,
              "before 各被记一次（索引 1）");
        check(first->log[2].rfind("afterExecution:", 0) == 0 &&
              second->log[2].rfind("afterExecution:", 0) == 0,
              "after 各被记一次（索引 2），顺序一致");

        // 清理，恢复默认
        InterceptorRegistry::clear();
        InterceptorRegistry::setEnabled(false);
    }

    std::cout << "== M1 SPI：beforeExecution 短路拦截（拒绝）==\n";
    {
        InterceptorRegistry::clear();
        InterceptorRegistry::setEnabled(false);

        auto rejecter = std::make_shared<RecordingInterceptor>();
        rejecter->rejectBefore = true;
        auto passthrough = std::make_shared<RecordingInterceptor>();
        InterceptorRegistry::add(rejecter);
        InterceptorRegistry::add(passthrough);
        InterceptorRegistry::setEnabled(true);

        SqlContext captured;
        ExecutionView view{"ds-rej", "INSERT 1", common::OperationType::Execute,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, captured};
        const auto st = detail::runBeforeExecution(view);
        check(!st.ok() && st.code == common::ErrorCode::SqlBlocked,
              "第一个拦截器拒绝 → 整体短路，状态正确返回");
        check(passthrough->log.empty(),
              "第一个拒绝后第二个拦截器不被调用（短路语义）");

        InterceptorRegistry::clear();
        InterceptorRegistry::setEnabled(false);
    }

    std::cout << "== M1 SPI：I11 — 拦截器回调异常被吞掉 ==\n";
    {
        InterceptorRegistry::clear();
        InterceptorRegistry::setEnabled(false);

        auto thrower = std::make_shared<ThrowingInterceptor>();
        auto catcher = std::make_shared<RecordingInterceptor>(); // 应当被调用，证明 thrower 异常没污染链路
        InterceptorRegistry::add(thrower);
        InterceptorRegistry::add(catcher);
        InterceptorRegistry::setEnabled(true);

        SqlContext ctx;
        bool threw = false;
        try {
            detail::runOnRoute("ds-throw", "SELECT 9", common::OperationType::Query, ctx);
        } catch (...) {
            threw = true;
        }
        check(!threw, "onRoute 中拦截器抛异常不传播（I11）");

        SqlContext captured;
        ExecutionView view{"ds-throw", "SELECT 9", common::OperationType::Query,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, captured};
        common::Status beforeSt;
        threw = false;
        try {
            beforeSt = detail::runBeforeExecution(view);
        } catch (...) {
            threw = true;
        }
        check(!threw, "beforeExecution 中拦截器抛异常不传播");
        check(beforeSt.ok(),
              "第一个拦截器抛异常的 beforeExecution 返回 OK（被吞后保持 OK，"
              "因为拦截器的拒绝路径只有显式非 ok 才会短路）");
        check(catcher->log.size() >= 1 &&
              catcher->log[0].rfind("onRoute:", 0) == 0,
              "thrower 抛异常之后，catcher 依然被调用（顺序未被打断）");

        threw = false;
        try {
            detail::runAfterExecution(view);
        } catch (...) {
            threw = true;
        }
        check(!threw, "afterExecution 中拦截器抛异常不传播");

        InterceptorRegistry::clear();
        InterceptorRegistry::setEnabled(false);
    }

    std::cout << "== M1 SPI：currentInterceptorDepth 用于诊断 ==\n";
    {
        // 顶层调用时 depth = 0；嵌套 SQL（被拦截器触发）应涨，退出后归零。
        // runBeforeExecution 内部使用线程局部回调深度，退出后应归零。
        InterceptorRegistry::clear();
        InterceptorRegistry::setEnabled(true);
        const auto beforeLevel = detail::currentInterceptorDepth();
        SqlContext captured;
        ExecutionView view{"ds-depth", "SELECT depth", common::OperationType::Query,
                           nullptr, nullptr, 0, std::chrono::microseconds{0},
                           common::Status::OK(), false, 0, captured};
        detail::runBeforeExecution(view);
        const auto afterLevel = detail::currentInterceptorDepth();
        check(beforeLevel == 0 && afterLevel == 0,
              "进 runBeforeExecution 前后 depth 均为 0（DepthGuard 退出时已归零）");
        InterceptorRegistry::setEnabled(false);
        InterceptorRegistry::clear();
    }

    std::cout << "== M1 SPI：递归与并发隔离 ==\n";
    {
        InterceptorRegistry::clear();
        InterceptorRegistry::setEnabled(true);
        auto reentrant = std::make_shared<ReentrantRouteInterceptor>();
        InterceptorRegistry::add(reentrant);
        SqlContext routeContext;
        detail::runOnRoute("ds-route", "SELECT recursive", OperationType::Query,
                           routeContext);
        check(reentrant->routeCount.load() == 1,
              "onRoute 内再次触发路由时被递归保护拦截");

        InterceptorRegistry::clear();
        auto concurrent = std::make_shared<ConcurrentInterceptor>();
        InterceptorRegistry::add(concurrent);
        auto run = [&](const std::string &sql) {
            SqlContext ctx;
            ExecutionView view{"ds-concurrent", sql, OperationType::Query,
                               nullptr, nullptr, 0, std::chrono::microseconds{0},
                               Status::OK(), false, 0, ctx};
            auto guard = detail::makeInterceptorGuard(view);
            detail::runBeforeExecution(view);
        };
        std::thread first(run, "SELECT first");
        std::thread second(run, "SELECT second");
        first.join();
        second.join();
        check(concurrent->beforeCount.load() == 2,
              "两个线程的拦截深度互不抑制");
        check(concurrent->completionCount.load() == 2,
              "两个并发顶层调用各完成一次 onCompletion");

        InterceptorRegistry::setEnabled(false);
        InterceptorRegistry::clear();
    }

    std::cout << "\n----------------------------------------\n";
    std::cout << "通过 " << g_passed << " 项，失败 " << g_failed << " 项\n";
    // 收尾：保证进程退出时不留静态共享状态污染其他测试
    InterceptorRegistry::clear();
    InterceptorRegistry::setEnabled(false);
    return g_failed == 0 ? 0 : 1;
}
