#ifndef DBMW_ASYNC_EXECUTOR_H
#define DBMW_ASYNC_EXECUTOR_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>


namespace dbmw::async {

    // 执行器运行时统计（观测过载与排水进度用）。
    struct ExecutorStats {
        std::size_t threads = 0;
        std::size_t queueDepth = 0;       // 立即任务队列当前长度
        std::size_t active = 0;           // 正在执行任务的线程数
        std::uint64_t submitted = 0;
        std::uint64_t completed = 0;
        std::uint64_t rejected = 0;       // tryPost 因队列满被拒
        std::uint64_t delayedPending = 0; // 定时任务挂起数
    };

    // 任务执行器抽象：异步引擎唯一的调度出口。
    //
    // 默认实现是内置线程池（makeThreadPoolExecutor）；用户也可以注入自定义
    // 执行器（如 asio io_context 的适配器），让完成回调 / 协程恢复发生在
    // 自己的事件循环线程上。注入后 dbmw 不再自建线程池，且执行器必须活到
    // DBMW::shutdown() 返回。
    class IExecutor {
    public:
        virtual ~IExecutor() = default;

        using Task = std::function<void()>;

        // 非阻塞提交：队列满立即返回 false（调用方据此以 Overloaded 快速失败，
        // 这是显式背压信号，而不是隐式劣化）。
        virtual bool tryPost(Task task) = 0;

        // 延迟提交（重试退避 / 语句超时检查）。
        //
        // 不做容量拒绝：定时任务数量天然受在途操作数约束。到期任务在 timer
        // 线程上**内联执行**——语句超时检查必须在 worker 全被慢语句占住时
        // 仍然准点触发。因此约定：经 postAfter 投递的任务必须短小、不得
        // 长时间阻塞（引擎只投递重试重投递与超时检查这类快任务）。
        virtual void postAfter(Task task, std::chrono::milliseconds delay) = 0;

        // 停止接受新任务并排空在途任务（最多等 grace）。
        // 未到期的定时任务直接丢弃：调用方（异步引擎）的排水顺序保证到达
        // 这一步时在途操作已收尾，未收尾的定时重试也没有意义了。
        //
        // 注意：绝不能在执行器自身的任务线程内调用，否则 join 自己会死锁。
        virtual void shutdown(std::chrono::milliseconds grace
                              = std::chrono::milliseconds(5000)) = 0;

        [[nodiscard]] virtual ExecutorStats stats() const = 0;
    };

    // 内置实现：N worker（threads == 0 取 hardware_concurrency）
    // + 1 timer 线程 + 有界工作队列（默认容量 4096）。
    // worker 惰性启动：首个 tryPost / postAfter 才拉起线程。
    std::shared_ptr<IExecutor> makeThreadPoolExecutor(int threads, std::size_t queueSize);

} // namespace dbmw::async


#endif // DBMW_ASYNC_EXECUTOR_H
