#include "dbmw/core/write_buffer.h"

#include "dbmw/common/logger.h"

#include <chrono>
#include <string>
#include <utility>


namespace dbmw::core {
    bool WriteBuffer::enqueue(std::function<common::Status()> task) {
        if (!enabled_ || !task) return false;
        std::lock_guard<std::mutex> lk(mtx_);
        // 停机中不再受理：这些任务不会有机会被补发，返回 true 等于骗调用方。
        if (stop_ || !running_) return false;
        if (static_cast<int>(q_.size()) >= cfg_.max_queue) return false;
        q_.emplace(std::move(task), std::chrono::steady_clock::now());
        cv_.notify_one();
        return true;
    }

    std::size_t WriteBuffer::pending() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return q_.size();
    }

    void WriteBuffer::start() {
        if (!enabled_) return;
        std::lock_guard<std::mutex> lk(mtx_);
        if (running_) return;
        running_ = true;
        stop_ = false;
        worker_ = std::thread(&WriteBuffer::run, this);
    }

    void WriteBuffer::stop() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (!running_) return;
            stop_ = true;
        }
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
        std::lock_guard<std::mutex> lk(mtx_);
        running_ = false;
    }

    void WriteBuffer::run() {
        using clock = std::chrono::steady_clock;
        // ttl_ms <= 0 表示不过期，与 pool 里 idle_timeout_ms / max_lifetime_ms 的
        // "0 = 不限" 保持一致。若按字面理解成"保留 0 毫秒"，每条写在第一轮
        // flush 时就会全部判过期丢掉，整个写缓冲等于配了个静默丢数据的开关。
        const bool ttlEnabled = cfg_.ttl_ms > 0;
        const auto ttl = std::chrono::milliseconds(cfg_.ttl_ms);

        while (true) {
            std::queue<Item> batch;
            bool finalPass = false;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                if (!stop_) {
                    // 固定节奏轮询，即使队列非空也要等满一个 flush 周期。
                    //
                    // 这一点是必须的：补发失败的任务会被重新入队，如果这里用
                    // "队列非空就立刻醒" 的条件，就会退化成 "取出 -> 失败 -> 放回
                    // -> 立刻再取" 的忙等——数据库真宕机时反而把 CPU 和连接一起打满，
                    // 一个用来兜底的机制变成了压垮系统的那一脚。
                    cv_.wait_for(lk, std::chrono::milliseconds(cfg_.flush_interval_ms),
                                 [this] { return stop_; });
                }
                finalPass = stop_;
                // 整批取走：本轮重新入队的任务留到下一轮，不会在同一轮里被反复重试。
                batch.swap(q_);
            }

            std::queue<Item> retry;
            while (!batch.empty()) {
                Item item = std::move(batch.front());
                batch.pop();

                if (ttlEnabled && clock::now() - item.second > ttl) {
                    DBMW_LOG_WARN("write buffer: dropping expired buffered write");
                    continue;
                }

                // 锁外执行：补发要走完整的网络往返，持锁会把 enqueue 全部堵死。
                const auto st = item.first();
                if (st.ok()) {
                    DBMW_LOG_INFO("write buffer: flushed buffered write");
                    continue;
                }
                // 停机的最后一轮只尝试一次，不再重排——否则 stop() 会被无限拖住。
                if (!finalPass && (st.retryable || st.connectionBroken)) {
                    retry.push(std::move(item));
                } else {
                    DBMW_LOG_WARN("write buffer: buffered write failed, dropped: " + st.message);
                }
            }

            if (!retry.empty()) {
                std::size_t dropped = 0;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    // 重试项必须回到队首。
                    //
                    // 本轮处理期间新入队的写正躺在 q_ 里；把重试项追加到它们后面，
                    // 同一行的 INSERT 就会排到后来的 UPDATE 之后，补发出来的结果
                    // 直接是错的。写缓冲一旦乱序，比不缓冲更糟。
                    while (!q_.empty()) {
                        retry.push(std::move(q_.front()));
                        q_.pop();
                    }
                    // 溢出时丢队尾（最新的写），保住已经排好的历史顺序。
                    while (!retry.empty()) {
                        if (static_cast<int>(q_.size()) >= cfg_.max_queue) {
                            ++dropped;
                        } else {
                            q_.push(std::move(retry.front()));
                        }
                        retry.pop();
                    }
                }
                if (dropped > 0) {
                    DBMW_LOG_WARN("write buffer: queue full, dropped "
                                  + std::to_string(dropped) + " buffered write(s)");
                }
            }

            if (finalPass) {
                std::size_t abandoned = 0;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    abandoned = q_.size();
                    std::queue<Item>().swap(q_);
                }
                if (abandoned > 0) {
                    DBMW_LOG_WARN("write buffer: abandoning " + std::to_string(abandoned)
                                  + " buffered write(s) on shutdown");
                }
                return;
            }
        }
    }
} // namespace dbmw::core
