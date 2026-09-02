#ifndef DBMW_CORE_WRITE_BUFFER_H
#define DBMW_CORE_WRITE_BUFFER_H

#include "dbmw/common/types.h"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>


namespace dbmw::core {
    // 有界内存写缓冲：主与所有候选都不可用时，把写请求缓入队列，
    // 后台 flush 线程在主恢复后补发。这是"软降级"——入队即返回 Buffered，
    // 不代表已提交；进程崩溃会丢数据，且绝不用于事务。
    class WriteBuffer {
    public:
        struct Config {
            bool enabled = false;
            int max_queue = 1000;
            int ttl_ms = 30000;          // 入队后最多保留时长，超时丢弃
            int flush_interval_ms = 1000; // 后台 flush 周期
        };

        explicit WriteBuffer(Config cfg) : cfg_(cfg), enabled_(cfg.enabled) {
            if (cfg_.max_queue < 1) cfg_.max_queue = 1;
            if (cfg_.flush_interval_ms < 10) cfg_.flush_interval_ms = 10;
        }

        ~WriteBuffer() { stop(); }

        // 入队一个在 flush 时执行的任务。
        // 返回 false 表示未受理（未启用 / 队列已满 / 已开始停机），调用方必须据此报错，
        // 绝不能把 false 当成"已缓冲"——那会让调用方以为写入最终会落地。
        bool enqueue(std::function<common::Status()> task);

        void start();
        void stop();
        [[nodiscard]] bool enabled() const { return enabled_; }

        // 当前积压条数（观测用）。
        [[nodiscard]] std::size_t pending() const;

    private:
        using Task = std::function<common::Status()>;
        // 任务 + 入队时刻（用于 TTL 判定）。
        using Item = std::pair<Task, std::chrono::steady_clock::time_point>;

        void run();

        Config cfg_;
        bool enabled_;
        mutable std::mutex mtx_;
        std::condition_variable cv_;
        std::queue<Item> q_;
        std::thread worker_;
        bool stop_ = false;
        bool running_ = false;
    };
} // namespace dbmw::core

#endif // DBMW_CORE_WRITE_BUFFER_H
