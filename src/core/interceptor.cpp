#include "dbmw/core/interceptor.h"

#include <atomic>
#include <mutex>
#include <utility>
#include <vector>

namespace dbmw::core {

    namespace {
        // 注册表全局状态：
        //   - `enabled_`：热路径必须 O(1) 原子读；
        //   - `interceptors_`：注册表本身，由 mutex 保护。
        std::atomic<bool> &enabledFlag() {
            static std::atomic<bool> v{false};
            return v;
        }

        std::mutex &registryMtx() {
            static std::mutex m;
            return m;
        }

        std::vector<std::shared_ptr<ISqlInterceptor>> &registry() {
            static std::vector<std::shared_ptr<ISqlInterceptor>> r;
            return r;
        }

        // 执行嵌套与回调递归都必须按线程隔离。进程级原子会让一个线程的
        // 拦截器回调错误抑制另一个线程的正常 SQL。
        thread_local std::size_t g_executionDepth = 0;
        thread_local std::size_t g_callbackDepth = 0;

        // 一次性递增/递减 RAII 助手，便于埋点路径使用对称的进出栈。
        class CallbackGuard {
        public:
            CallbackGuard() noexcept { ++g_callbackDepth; }
            ~CallbackGuard() noexcept { --g_callbackDepth; }
        };

        // 单次拦截器调用的异常吞掉包装（I11）。
        template <typename Fn>
        void safeCall(Fn &&fn) noexcept {
            try { std::forward<Fn>(fn)(); } catch (...) { /* 吞掉 */ }
        }

    } // namespace

    void InterceptorRegistry::add(std::shared_ptr<ISqlInterceptor> interceptor) {
        if (!interceptor) return;
        std::lock_guard<std::mutex> lk(registryMtx());
        registry().push_back(std::move(interceptor));
    }

    void InterceptorRegistry::clear() {
        std::lock_guard<std::mutex> lk(registryMtx());
        registry().clear();
    }

    InterceptorRegistry::Snapshot InterceptorRegistry::snapshot() {
        std::lock_guard<std::mutex> lk(registryMtx());
        return registry();  // vector 复制
    }

    bool InterceptorRegistry::enabled() noexcept {
        // 拦截器开关与"是否有注册项"是两层语义。这里仅查 enabled flag；
        // 即便开启，没有注册的拦截器也无副作用——分两次原子读是为了让热路径
        // "开关 false 时根本不走" 严格成立。
        return enabledFlag().load(std::memory_order_acquire);
    }

    void InterceptorRegistry::setEnabled(bool v) noexcept {
        enabledFlag().store(v, std::memory_order_release);
    }

// ------------------------------------------------------------------------
// 公开的 RAII 助手：拦截器入口的栈展开
// ------------------------------------------------------------------------
//
// 类的定义在 interceptor.h 中（InterceptionView 的 auto 推断需要完整类型可见），
// 实现放在这里：析构时才走 runOnRoute/snapshot，对调用方零开销。
//
// 嵌套类同样需要定义 ctor（藏在 .cpp 即可——只在头文件 forward declare 时需要）。
// 实际析构分发逻辑紧跟其后，便于审阅。
//
// 注意：本类未导出在头文件——与设计稿 §3.5 的"在数据库管理器内部嵌入埋点"
// 一致，调用方不应直接看到 InterceptorGuard；它由埋点宏（如
//   DBMW_INTERCEPT_ROUTE(...);
//   DBMW_INTERCEPT_BEGIN(view);
//   DBMW_INTERCEPT_AFTER(view);
//   DBMW_INTERCEPT_COMPLETE(view);
// 封装）。本类作为实现细节，对数据库管理器可见。
//
// 此处签名不对外暴露；真正消费方会在 database_manager.cpp 内的匿名命名空间
// 提供一个 `runInterceptorsXXX(...)` 函数调用本类，把字面量包装集中起来。

    detail::InterceptorGuard::InterceptorGuard(const ExecutionView &view)
        : view_(view),
          active_(InterceptorRegistry::enabled() && g_executionDepth == 0 &&
                  g_callbackDepth == 0) {
        if (active_) ++g_executionDepth;
    }

    detail::InterceptorGuard::~InterceptorGuard() noexcept {
        if (active_) {
            CallbackGuard callbackGuard;
            try {
                for (auto &it : InterceptorRegistry::snapshot()) {
                    safeCall([&]{ it->onCompletion(view_); });
                }
            } catch (...) {
                // snapshot() 的分配失败也不能从析构函数逃逸；埋点永远不能
                // 改变业务异常路径。
            }
        }
        if (active_) --g_executionDepth;
    }

    // 命令式助手（被埋点调用），用 C++17 free function 导出。
    // 不放在头文件以避免污染 API 面；通过数据库管理器 namespace-internal 调用。
    namespace detail {

        void runOnRoute(const std::string &dataSource, const std::string &sql,
                        common::OperationType type, common::SqlContext &ctx) {
            if (!InterceptorRegistry::enabled() || g_executionDepth > 0 ||
                g_callbackDepth > 0) return;
            CallbackGuard callbackGuard;
            for (auto &it : InterceptorRegistry::snapshot()) {
                safeCall([&]{ it->onRoute(dataSource, sql, type, ctx); });
            }
        }

        common::Status runBeforeExecution(const ExecutionView &view) {
            if (!InterceptorRegistry::enabled() || g_executionDepth > 1 ||
                g_callbackDepth > 0) return common::Status::OK();
            CallbackGuard callbackGuard;
            common::Status st;
            for (auto &it : InterceptorRegistry::snapshot()) {
                safeCall([&]{ st = it->beforeExecution(view); });
                // beforeExecution 可以拒绝执行；遇到第一个非 ok 即短路。
                if (!st.ok()) return st;
            }
            return st;
        }

        void runAfterExecution(const ExecutionView &view) {
            if (!InterceptorRegistry::enabled() || g_executionDepth > 1 ||
                g_callbackDepth > 0) return;
            CallbackGuard callbackGuard;
            for (auto &it : InterceptorRegistry::snapshot()) {
                safeCall([&]{ it->afterExecution(view); });
            }
        }

        void runOnRow(const ExecutionView &view, common::Row &row) {
            if (!InterceptorRegistry::enabled() || g_executionDepth > 1 ||
                g_callbackDepth > 0) return;
            CallbackGuard callbackGuard;
            for (auto &it : InterceptorRegistry::snapshot()) {
                safeCall([&]{ it->onRow(view, row); });
            }
        }

        // InterceptorGuard 由调用方用 `makeInterceptorGuard(view)` 构造，
        // RAII 析构保证 onCompletion 一定调用一次。
        detail::InterceptorGuard makeInterceptorGuard(const ExecutionView &view) {
            return detail::InterceptorGuard(view);
        }

        // 当前递归深度（仅供调试，调用方勿用以改变业务行为）。
        std::size_t currentInterceptorDepth() noexcept {
            return g_executionDepth + g_callbackDepth;
        }

    } // namespace detail

} // namespace dbmw::core
