#ifndef DBMW_ASYNC_TASK_H
#define DBMW_ASYNC_TASK_H

// ---------------------------------------------------------------------------
// 协程层（可选，C++20）：dbmw v0.2.0 设计 §5.3。
//
// 开关：dbmw 构建时 -DDBMW_ENABLE_ASYNC_CORO=ON 才会编译/导出本层；
// 关闭时 task.cpp 不参与构建，本头文件存在但不可用（直接 #error，
// 让误用者在编译期就拿到明确原因，而不是链接期的一堆 undefined symbol）。
//
// 使用方要求：包含本头文件的 TU 必须以 C++20 编译（Task / promise /
// awaiter 全部是头文件模板，协程状态机在调用方 TU 内实例化）。
//
// 语义要点（与设计一致）：
//   - Task<T> 是惰性任务：创建只分配协程帧并立刻挂起，co_await 才启动；
//     未被 co_await 就析构 = 安全放弃，什么都不执行（R5）。
//   - 协程恢复线程 = 完成调度器线程（默认主执行器；注入 asio 适配器后
//     即在 io_context 线程恢复）。取消/超时/治理与回调形态完全同源。
//   - 顶层任务用 run() 受控启动：跑完后协程帧在最终挂起点自毁，
//     fire-and-forget 不会悬垂；不提供 detach。
//
// 编译器兼容性（GCC 13 已知缺陷，PR109227 系）：
//   - co_await 表达式的实参里出现**非平凡的花括号临时**（如
//     `co_await queryAsync(sql, {Value(1)})`）会触发 GCC 13 的
//     internal compiler error（build_special_member_call）。
//   - 规避：参数先具名构造再传入（`Params p{Value(1)}; co_await ...(sql, p)`）。
//     空 `{}`、具名变量、lambda 实参不受影响（有测试覆盖）。
//   - GCC 14+ / Clang / MSVC 无此问题。
// ---------------------------------------------------------------------------

#if !defined(DBMW_ENABLE_ASYNC_CORO)
#error "dbmw/async/task.h requires building dbmw with -DDBMW_ENABLE_ASYNC_CORO=ON (C++20)"
#endif

#include "dbmw/async/dbmw_async.h"

#include <coroutine>
#include <exception>
#include <functional>
#include <optional>
#include <type_traits>
#include <utility>

namespace dbmw::async {

    template <class T>
    class Task;

    void run(Task<void> t);

    namespace detail {

        // GCC15 -Wtemplate-body：基类体内引用派生类，须先声明。
        template <class T>
        class TaskPromise;

        // ---- promise 基座：continuation 登记 + 异常存帧（R5：结果存帧内）----
        template <class T>
        class TaskPromiseBase {
        public:
            Task<T> get_return_object() noexcept {
                return Task<T>{std::coroutine_handle<TaskPromise<T>>::from_promise(
                    *static_cast<TaskPromise<T> *>(this))};
            }

            // 惰性：创建即挂起，co_await（run）时才启动协程体。
            std::suspend_always initial_suspend() noexcept { return {}; }

            // 最终挂起：有 continuation 就对称转移回等待者；没有（run() 启动的
            // 顶层任务）则在此自毁协程帧——fire-and-forget 的受控收尾。
            // 注意：自毁前若帧内仍有未观察的异常，直接 terminate——静默吞掉
            // 比崩溃更危险。
            struct FinalAwaiter {
                bool await_ready() const noexcept { return false; }

                std::coroutine_handle<> await_suspend(
                    std::coroutine_handle<TaskPromise<T>> h) noexcept {
                    auto &p = h.promise();
                    std::coroutine_handle<> cont = p.continuation_;
                    if (cont == nullptr) {
                        if (p.exception_) std::terminate();
                        h.destroy();
                        return std::noop_coroutine();
                    }
                    return cont;
                }

                void await_resume() const noexcept {}
            };

            FinalAwaiter final_suspend() noexcept { return {}; }

            void unhandled_exception() noexcept {
                exception_ = std::current_exception();
            }

        private:
            friend class Task<T>;

            std::coroutine_handle<> continuation_;
            std::exception_ptr exception_;
        };

        // 非 void：结果经 return_value 存进帧内 optional。
        template <class T>
        class TaskPromise final : public TaskPromiseBase<T> {
        public:
            void return_value(T v) { value_.emplace(std::move(v)); }

        private:
            friend class Task<T>;

            std::optional<T> value_;
        };

        // void：无结果体。
        template <>
        class TaskPromise<void> final : public TaskPromiseBase<void> {
        public:
            void return_void() noexcept {}
        };

        // ---- 回调 API → 协程的桥（设计 §5.3："回调 API 的薄封装"）----
        //
        // co_await 处发生的事：await_suspend 记下协程句柄并经 launch 发起
        // 回调式操作；引擎回调（完成调度器线程）把结果写进帧内 awaiter，
        // 然后 resume——因此协程恢复线程恒为完成调度器线程。
        template <class R>
        class OpAwaiter {
        public:
            using Callback = std::function<void(R &&)>;
            using Launcher = std::function<void(Callback)>;

            explicit OpAwaiter(Launcher launch) : launch_(std::move(launch)) {}

            bool await_ready() const noexcept { return false; }

            void await_suspend(std::coroutine_handle<> self) {
                self_ = self;
                // 把 launch 挪到栈上再调用：即便引擎回调被同步触发并（顶层
                // detached 场景）自毁了协程帧，本调用链也不会再落在被毁的
                // 帧上。I1 保证引擎回调一律经完成调度器投递，这只是双保险。
                Launcher launch = std::move(launch_);
                launch(Callback([this](R &&r) {
                    result_ = std::move(r);
                    self_.resume();
                }));
            }

            R await_resume() { return std::move(result_); }

        private:
            Launcher launch_;
            R result_{};
            std::coroutine_handle<> self_;
        };

    } // namespace detail

    // ---- 惰性任务 ----
    //
    // 两种用法：
    //   1. co_await queryAsync(...) / co_await someTask —— 本类型兼任 awaiter；
    //   2. 返回 Task 的协程可互相 co_await（continuation 链）。
    //
    // 约束：
    //   - 不可拷贝；可移动，但移动后的 Task 不可再 await；
    //   - 被等待中的 Task 所属协程帧由"等待它的协程"保证存活（协程常规
    //     规则：销毁一个仍在 await 的协程帧是调用方错误）。
    template <class T>
    class [[nodiscard]] Task {
    public:
        using promise_type = detail::TaskPromise<T>;

        Task(Task &&other) noexcept : h_(std::exchange(other.h_, {})) {}

        Task &operator=(Task &&other) noexcept {
            if (this != &other) {
                if (h_) h_.destroy();
                h_ = std::exchange(other.h_, {});
            }
            return *this;
        }

        Task(const Task &) = delete;
        Task &operator=(const Task &) = delete;

        ~Task() {
            if (h_) h_.destroy();
        }

        // ---- awaiter 接口（co_await Task 时由编译器调用）----

        bool await_ready() const noexcept { return false; }

        // 对称转移：先登记等待者（continuation），再把执行权交给惰性协程体。
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
            h_.promise().continuation_ = awaiting;
            return h_;
        }

        T await_resume() {
            auto &p = h_.promise();
            if (p.exception_) std::rethrow_exception(p.exception_);
            if constexpr (!std::is_void_v<T>)
                return std::move(*p.value_);
        }

    private:
        friend class detail::TaskPromiseBase<T>;
        friend void run(Task<void> t);

        explicit Task(std::coroutine_handle<promise_type> h) noexcept : h_(h) {}

        std::coroutine_handle<promise_type> h_;
    };

    // 顶层任务启动器：fire-and-forget 的受控形态。
    //
    // 返回后协程继续在完成调度器线程上推进；跑完在最终挂起点自毁，
    // 调用方无需（也无法）再持有它。协程体内未捕获的异常 = terminate。
    inline void run(Task<void> t) {
        auto h = std::exchange(t.h_, {});
        if (h && !h.done()) h.resume();
    }

    // ---- 协程式门面（§5.3 + 两处对称补齐）----
    //
    // 以下工厂全部是惰性 Task：调用只是构造协程帧，co_await 才发起操作；
    // 语义（治理/重试/取消/超时/缓存）与回调形态完全同源。
    // 相对设计 §5.3 的两处补齐（不改语义，只是镜像 §5.2 的重载面）：
    //   - queryAsync 增加显式数据源重载；
    //   - executeKeysAsync：默认数据源的生成键形态（§5.2 的 executeKeys
    //     回调/future 形态都有，协程层理应对称）。

    Task<QueryResult> queryAsync(std::string sql, common::Params params = {},
                                 Options opts = {});
    Task<QueryResult> queryAsync(std::string dataSource, std::string sql,
                                 common::Params params, Options opts = {});

    Task<ExecResult> executeAsync(std::string sql, common::Params params = {},
                                  Options opts = {});
    // 显式数据源 + 生成键形态（镜像回调式 execute(ds, sql, params, ExecKeysCallback)）。
    Task<ExecKeysResult> executeAsync(std::string dataSource, std::string sql,
                                      common::Params params, Options opts = {});
    Task<ExecKeysResult> executeKeysAsync(std::string sql, common::Params params = {},
                                          Options opts = {});

    Task<BatchResult> executeBatchAsync(std::string sql, common::ParamBatch batch,
                                        Options opts = {});

    Task<OpResult> transactionAsync(common::TransactionOptions txOpts,
                                    core::SessionFn fn);

} // namespace dbmw::async

#endif // DBMW_ASYNC_TASK_H
