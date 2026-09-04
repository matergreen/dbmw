// =============================================================================
// dbmw v0.2.0 异步 API 示例：回调 / future / 协程 / 取消 / 自定义执行器
//
// 与 basic_usage 相同的运行约定：未启用真实驱动时各操作返回
// DriverDisabled / NotSupported，示例仍可编译运行并打印 [OK]/[NOTE]。
//
// 四种形态怎么选：
//   - 回调式：热路径首选。零额外分配（对比一次网络 RTT 可忽略的是
//     future 式的 packaged_task），且能拿到 Handle 做取消。
//   - future 式：便利形态，适合脚本/测试；无取消能力。
//   - 协程式：线性写法 + 零额外语义（治理/重试/取消/超时与回调完全同源），
//     需要 -DDBMW_ENABLE_ASYNC_CORO=ON 且调用方 TU 以 C++20 编译。
//   - 取消：任何形态都建议给慢语句设 Options.timeout 兜底；
//     主动取消只有回调式支持（Handle::cancel）。
//
// 运行：async_example [配置文件路径]（默认 config/datasources.json.example）
// =============================================================================
#include "dbmw/dbmw.h"
#include "dbmw/async/dbmw_async.h"

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

#if defined(DBMW_ENABLE_ASYNC_CORO)
#include "dbmw/async/task.h"
#endif

namespace {
    void report(const char *tag, const dbmw::common::Status &st) {
        std::cout << (st.ok() ? "[OK]   " : "[NOTE] ")
                  << tag << ": " << st.message
                  << " (code=" << dbmw::common::errorCodeToString(st.code) << ")"
                  << std::endl;
    }

    // 回调完成的同步点：示例里用 future 桥一下，业务代码里通常直接在
    // 回调里继续你自己的逻辑（注意：回调跑在完成调度器线程上，须短小）。
    template <class R>
    R awaitCallback(void (*launch)(std::function<void(R &&)>)) {
        std::promise<R> pr;
        auto fut = pr.get_future();
        launch([&pr](R &&r) { pr.set_value(std::move(r)); });
        return fut.get();
    }
} // namespace

#if defined(DBMW_ENABLE_ASYNC_CORO)
// 协程体：必须是具名函数（或生命周期明确的仿函数），不要用捕获局部引用的
// lambda 协程——闭包临时对象先于异步完成销毁，捕获会悬垂。
dbmw::async::Task<void> coroDemo() {
    // GCC 13 已知缺陷（PR109227 系）：co_await 表达式的实参里出现非平凡的
    // 花括号临时（如 {Value(...)}）会触发编译器内部错误（ICE）。规避：参数
    // 先具名构造再传入。GCC 14+ / Clang / MSVC 不受影响。
    dbmw::common::Params params;
    params.push_back(dbmw::common::Value(std::int64_t(1)));

    // co_await 挂起 → 回调式 query 在引擎内推进 → 完成调度器线程恢复本协程。
    auto q = co_await dbmw::async::queryAsync(
        "SELECT id, name FROM users WHERE id = ?", params);

    if (q.status.ok()) {
        std::cout << "[OK]   协程式 query 返回 " << q.rows.rowCount() << " 行" << std::endl;
    } else {
        report("协程式 query", q.status);
    }

    // 事务也是一条 co_await；fn 内部用同步 Session 方法（禁止嵌套 dbmw::async::*）。
    dbmw::common::TransactionOptions txOpts;
    auto tx = co_await dbmw::async::transactionAsync(txOpts,
        [](dbmw::core::Session &s) -> dbmw::common::Status {
            std::int64_t affected = 0;
            if (const auto st = s.execute("UPDATE users SET active = 1", affected); !st.ok())
                return st; // 返回非 Ok → 引擎自动回滚
            return dbmw::common::Status::OK();
        });
    report("协程式 transaction", tx.status);
}
#endif

int main(int argc, char **argv) {
    const std::string configPath =
        (argc > 1) ? argv[1] : "config/datasources.json.example";

    auto st = dbmw::DBMW::init(configPath);
    if (!st.ok()) {
        std::cerr << "[FAIL] init: " << st.message << std::endl;
        return 1;
    }
    std::cout << "[OK]   init from " << configPath << std::endl;

    // -------------------------------------------------------------------------
    // 1) 回调式（热路径）：完成回调由完成调度器投递，绝不在调用栈上执行。
    // -------------------------------------------------------------------------
    {
        auto h = dbmw::async::query(
            "SELECT id, name FROM users WHERE id = ?",
            {dbmw::common::Value(std::int64_t(1))},
            [](dbmw::async::QueryResult &&r) {
                if (r.status.ok())
                    std::cout << "[OK]   回调式 query 返回 " << r.rows.rowCount()
                              << " 行（回调线程: " << "完成调度器"
                              << "）" << std::endl;
                else
                    std::cout << "[NOTE] 回调式 query: " << r.status.message << std::endl;
            });
        // Handle 可丢弃（不 cancel 合法）；需要取消时保存它。
        (void) h;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // -------------------------------------------------------------------------
    // 2) future 式（便利形态）：无取消；未取值即析构是合法用法。
    // -------------------------------------------------------------------------
    {
        auto fut = dbmw::async::execute(
            "UPDATE users SET active = 1 WHERE id = ?",
            {dbmw::common::Value(std::int64_t(1))});
        auto r = fut.get();
        if (r.status.ok())
            std::cout << "[OK]   future 式 execute 受影响 " << r.affected << " 行" << std::endl;
        else
            report("future 式 execute", r.status);
    }

#if defined(DBMW_ENABLE_ASYNC_CORO)
    // -------------------------------------------------------------------------
    // 3) 协程式：run() 是 fire-and-forget 的受控启动，协程跑完自毁；
    //    未 co_await 的 Task 直接析构 = 安全放弃。
    // -------------------------------------------------------------------------
    {
        dbmw::async::run(coroDemo());
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
#else
    std::cout << "[NOTE] 协程层未启用：加 -DDBMW_ENABLE_ASYNC_CORO=ON 重新构建"
              << "（且本 TU 以 C++20 编译）后可见第 3 段演示" << std::endl;
#endif

    // -------------------------------------------------------------------------
    // 4) 取消：慢语句运行中 cancel；配合 Options.timeout 做兜底。
    // -------------------------------------------------------------------------
    {
        dbmw::async::Options opts;
        opts.timeout = std::chrono::milliseconds(2000); // 语句整体期限
        // 注意：空参数要写 dbmw::common::Params{}，裸 {} 会与
        // (dataSource, sql, cb, opts) 重载二义（{} 也能构造空 string）。
        auto h = dbmw::async::query("SELECT report_all_users()", dbmw::common::Params{},
            [](dbmw::async::QueryResult &&r) {
                std::cout << "[NOTE] 被取消/完成的慢查询: code="
                          << dbmw::common::errorCodeToString(r.status.code)
                          << std::endl;
            }, opts);
        // 500ms 后主动放弃：Queued → 不碰池；Running → 转发驱动 cancel
        // （驱动不支持时语句照跑完，状态仍如实标 Cancelled）。
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        if (const auto c = h.cancel(); c.ok())
            std::cout << "[OK]   已请求取消（Handle 状态 Running/Queued）" << std::endl;
        else
            std::cout << "[NOTE] cancel: " << c.message << "（已完成则无事可做）" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }

    // -------------------------------------------------------------------------
    // 5) 自定义执行器（asio 接入）——适配器形状示意：
    //
    //   class AsioExecutor : public dbmw::async::IExecutor {
    //   public:
    //       explicit AsioExecutor(asio::io_context &io) : io_(io) {}
    //       bool tryPost(Task task) override {
    //           if (task) asio::post(io_, std::move(task));
    //           return true;            // asio 队列无界，不会拒绝
    //       }
    //       void postAfter(Task task, std::chrono::milliseconds delay) override {
    //           if (task) timer_ = ...; // 用 asio::steady_timer 到期 post
    //       }
    //       void shutdown(std::chrono::milliseconds) override { /* io 停止即可 */ }
    //       dbmw::async::ExecutorStats stats() const override { return {}; }
    //   private:
    //       asio::io_context &io_;
    //   };
    //
    //   在任何异步调用之前：
    //       dbmw::async::setExecutor(std::make_shared<AsioExecutor>(io));
    //   之后完成回调 / 协程恢复全部发生在 io_context 线程上，
    //   协程里 co_await 即可与 asio 的定时器/网络事件自然交织。
    //   注意：postAfter 到期任务必须短小（引擎只投递重试/超时检查）。
    // -------------------------------------------------------------------------

    dbmw::DBMW::shutdown(std::chrono::milliseconds(3000));
    std::cout << "[OK]   shutdown（在途操作已排空）" << std::endl;
    return 0;
}
