#ifndef DBMW_CORE_INTERCEPTOR_H
#define DBMW_CORE_INTERCEPTOR_H

#include "dbmw/common/context.h"
#include "dbmw/common/observer.h"   // OperationType
#include "dbmw/common/types.h"      // Status, ResultSet, Params

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

namespace dbmw::core {

    // 一次语句执行的视图。
    //
    // 字段部分可读（dataSource/sql/type/params/result/affected/duration/status/
    // cached），部分只读非空：
    //   - `dataSource/sql/type/params`：执行前后均可用；
    //   - `result`：仅 `afterExecution` / `onCompletion` 期间携带结果集；
    //   - `affected/duration/status`：仅 `afterExecution` / `onCompletion` 期间有效。
    //
    // 生命周期仅限回调期间——实现**不得**保存该结构体或其指针。回调返回后
    // 视图所引用的字符串/容器生命周期结束。
    //
    // `depth` 字段：递归防护。当拦截器自身又触发了 dbmw 调用时，深度 ≥ 2，
    // 此时**不再向回调分发**该子调用——避免无限递归。
    struct ExecutionView {
        const std::string &dataSource;
        const std::string &sql;
        common::OperationType type;
        const common::Params *params;       // 无参数时为 nullptr
        common::ResultSet *result;          // 失败或非查询时为 nullptr
        std::int64_t affected = 0;
        std::chrono::microseconds duration{0};
        common::Status status;
        bool cached = false;
        std::size_t depth = 0;              // 0 = 业务顶层调用
        common::SqlContext &ctx;            // 可写：路由期可置 shadow/targetDataSource
    };

    // SQL 执行扩展点。
    //
    // 业务实现该接口，挂到 `DBMW::addInterceptor()`，由 dbmw 在以下时机回调：
    //   - `onRoute`：preGate 之后、选节点之前（路由决策 + 上下文标记）；
    //   - `beforeExecution`：驱动调用之前（返回非 ok → 中止执行，不进重试/写缓冲）；
    //   - `afterExecution`：驱动调用之后（可改写 view.result——M7 脱敏即此）；
    //   - `onCompletion`：恰好一次（由 InterceptorGuard RAII 保证）。
    //
    // 线程模型：所有回调都在"执行此次 SQL"的同一线程上——
    //   · 同步路径：调用线程；
    //   · 异步路径：AsyncEngine 的 worker。
    // 因此 `current()` 的 thread_local 与调用方一致（异步路径由 §3.2 的 Op
    // 快照 + worker 安装方案保证同步/异步一致）。
    //
    // 异常模型：回调内异常被吞掉（I11），绝不传播到业务。
    class ISqlInterceptor {
    public:
        virtual ~ISqlInterceptor() = default;

        // 路由决策：preGate 之后、选节点之前。
        // 可写 ctx（如置 shadow = true 把流量引到影子库，或填 targetDataSource）。
        virtual void onRoute(const std::string &dataSource, const std::string &sql,
                             common::OperationType type, common::SqlContext &ctx) = 0;

        // 执行前：SQL 与参数已定，尚未下发驱动。
        // 返回非 ok 则中止执行——该语义为"拦截"，不触发重试、不入写缓冲。
        virtual common::Status beforeExecution(const ExecutionView &view) = 0;

        // 执行后：可改写 view.result（M7 脱敏即在此实现）。
        virtual void afterExecution(const ExecutionView &view) = 0;

        // 收尾：无论成功失败恰好调用一次（由 RAII 保证）。
        virtual void onCompletion(const ExecutionView &view) = 0;
    };

    // 拦截器注册表。线程安全（持 mutex）。建议在 init() 之前调用。
    //
    // 设计选择：注册表是**进程级**单例（与 DatabaseManager 共享生命周期），而非
    // 数据源级。这意味着拦截器对所有 SQL 生效——这是 SPI 路线的本意（横切关注点）。
    //
    // 拦截器按注册顺序调用（与"责任链"风格一致）。注册的拦截器在 shutdown 时
    // 由 `clearInterceptors()` 显式清理——避免静态析构顺序陷阱。
    class InterceptorRegistry {
    public:
        InterceptorRegistry() = delete;

        static void add(std::shared_ptr<ISqlInterceptor> interceptor);
        static void clear();

        // 复制拦截器列表快照——返回时即拷一份，调用方在锁外迭代。
        // 调用者还须用 `enabled()` 短路：开关 false 时本接口不应被调。
        // 但保留 implementation：locked context 中复制，确保调用过程无锁迭代。
        using Snapshot = std::vector<std::shared_ptr<ISqlInterceptor>>;
        static Snapshot snapshot();

        // 是否在执行路径上启用拦截器。原子读：热路径不该抢全局锁。
        static bool enabled() noexcept;

        // 总开关（DBMW 配置读取后调一次）。default = false。
        // 与 query_cache.enabled 一致——默认关闭的功能不该给每条语句留锁代价。
        static void setEnabled(bool v) noexcept;
    };

    // detail 命名空间：埋点指令的命令式助手。仅 dbmw 内部（database_manager.cpp）
    // 调用，不放进用户的 API 面。forward 声明到头文件以便同一个 TU 的 core 文件
    // 可以"detail::runOnRoute"裸调用（不强制 `core::detail::`，与已有的
    // async::detail 区分）。
    namespace detail {
        void runOnRoute(const std::string &dataSource, const std::string &sql,
                        common::OperationType type, common::SqlContext &ctx);
        common::Status runBeforeExecution(const ExecutionView &view);
        void runAfterExecution(const ExecutionView &view);

        // RAII 助手：构造时启动埋点计时/状态机，析构时触发 onCompletion 一次。
        // 定义放到头文件——调用方需要 `auto guard = makeInterceptorGuard(view)`
        // 形式的语法，使用 `auto` 时类型必须完整可见（不能只前向声明）。
        // 实现细节（onCompletion 的分发）放 interceptor.cpp，不放进头文件。
        class InterceptorGuard {
        public:
            explicit InterceptorGuard(const ExecutionView &view) : view_(view) {}
            ~InterceptorGuard();
            // 非可拷贝 / 非可移动：守 "onCompletion 恰好一次" 的不变量。
            InterceptorGuard(const InterceptorGuard &) = delete;
            InterceptorGuard &operator=(const InterceptorGuard &) = delete;
            InterceptorGuard(InterceptorGuard &&) = delete;
            InterceptorGuard &operator=(InterceptorGuard &&) = delete;
        private:
            const ExecutionView &view_;
        };

        InterceptorGuard makeInterceptorGuard(const ExecutionView &view);

        // 当前递归深度（仅供调试，调用方勿用以改变业务行为）。
        std::size_t currentInterceptorDepth() noexcept;
    } // namespace detail

} // namespace dbmw::core

#endif // DBMW_CORE_INTERCEPTOR_H
