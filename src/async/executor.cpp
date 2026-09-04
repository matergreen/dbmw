#include "dbmw/async/executor.h"
#include "dbmw/common/logger.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>


namespace dbmw::async {
    namespace {
        // 内置线程池执行器：N worker + 1 timer + 有界工作队列。
        //
        // 正确性要点：
        //  - worker 里执行的任务一律 catch(...) 兜底：worker 死亡等于服务死亡；
        //  - timer 线程**内联执行**到期任务：语句超时检查必须在 worker 全被
        //    慢语句占住时仍然准点触发（否则超时判定被慢语句自己堵在队列后面，
        //    永远赶不上）。代价是 postAfter 的任务必须短小、不得长阻塞——
        //    引擎侧只投递重试重投递（step1）与超时检查这类快任务，池的建连/
        //    ping 走 io.post（tryPost + 内联兜底），不经定时队列；
        //  - shutdown 后 tryPost 返回 false、postAfter 静默丢弃，
        //    未到期定时任务一并丢弃（见 IExecutor::shutdown 的说明）。
        class ThreadPoolExecutor final : public IExecutor {
        public:
            explicit ThreadPoolExecutor(const int threads, const std::size_t queueSize)
                : threadCount_(threads > 0
                                   ? static_cast<std::size_t>(threads)
                                   : std::thread::hardware_concurrency()),
                  queueLimit_(queueSize > 0 ? queueSize : 1) {
                if (threadCount_ == 0) threadCount_ = 1;
            }

            ~ThreadPoolExecutor() override {
                if (!stopping_.load(std::memory_order_acquire)) {
                    shutdown(std::chrono::milliseconds(0));
                }
            }

            bool tryPost(Task task) override {
                if (!task) return false;
                std::unique_lock<std::mutex> lk(mtx_);
                ensureStartedLocked();
                if (stopping_.load(std::memory_order_relaxed)) return false;
                if (queue_.size() >= queueLimit_) {
                    ++rejected_;
                    return false;
                }
                queue_.push_back(std::move(task));
                ++submitted_;
                cvWork_.notify_one();
                return true;
            }

            void postAfter(Task task, std::chrono::milliseconds delay) override {
                if (!task) return;
                std::unique_lock<std::mutex> lk(mtx_);
                ensureStartedLocked();
                if (stopping_.load(std::memory_order_relaxed)) return;
                delayed_.emplace(DelayedTask{
                    std::chrono::steady_clock::now()
                    + std::max<std::chrono::milliseconds>(delay, std::chrono::milliseconds(0)),
                    seq_++, std::move(task)
                });
                cvTimer_.notify_one();
            }

            void shutdown(const std::chrono::milliseconds grace) override {
                std::vector<std::thread> workers;
                std::thread timer;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    if (stopping_.exchange(true, std::memory_order_acq_rel)) return;
                    delayed_ = {}; // 未到期定时任务丢弃（语义见 IExecutor::shutdown）
                    cvWork_.notify_all();
                    cvTimer_.notify_all();
                    workers = std::move(workers_);
                    timer = std::move(timerThread_);
                }
                if (timer.joinable()) timer.join();
                for (auto &w: workers) {
                    if (w.joinable()) w.join();
                }
                // C++ 不能安全强杀正在访问执行器/连接池状态的 worker。
                // grace 由引擎排水阶段消耗；到这里后必须协作式 join，
                // 驱动若不支持 cancel，停机可能等到底层网络超时。
                (void) grace;
            }

            [[nodiscard]] ExecutorStats stats() const override {
                std::lock_guard<std::mutex> lk(mtx_);
                ExecutorStats out;
                out.threads = threadsStarted_ ? threadCount_ : 0;
                out.queueDepth = queue_.size();
                out.active = active_;
                out.submitted = submitted_;
                out.completed = completed_;
                out.rejected = rejected_;
                out.delayedPending = delayed_.size();
                return out;
            }

        private:
            struct DelayedTask {
                std::chrono::steady_clock::time_point deadline;
                std::uint64_t seq; // 同 deadline 时保持先进先出
                Task task;

                // 最小堆：deadline 最先者顶置。
                bool operator<(const DelayedTask &other) const {
                    if (deadline != other.deadline) return deadline > other.deadline;
                    return seq > other.seq;
                }
            };

            // 惰性启动：首次提交时才创建线程，未使用异步的进程零额外线程。
            // 调用方必须已持有 mtx_。线程入口会立刻抢锁，这里持锁 spawn 安全。
            void ensureStartedLocked() {
                if (threadsStarted_) return;
                threadsStarted_ = true;
                for (std::size_t i = 0; i < threadCount_; ++i) {
                    workers_.emplace_back([this] { workerLoop(); });
                }
                timerThread_ = std::thread([this] { timerLoop(); });
            }

            void workerLoop() {
                for (;;) {
                    Task task;
                    {
                        std::unique_lock<std::mutex> lk(mtx_);
                        cvWork_.wait(lk, [this] {
                            return stopping_.load(std::memory_order_relaxed) || !queue_.empty();
                        });
                        if (queue_.empty()) {
                            if (stopping_.load(std::memory_order_relaxed)) return;
                            continue;
                        }
                        task = std::move(queue_.front());
                        queue_.pop_front();
                        ++active_;
                    }
                    runGuarded(task);
                    {
                        std::lock_guard<std::mutex> lk(mtx_);
                        --active_;
                        ++completed_;
                    }
                }
            }

            void timerLoop() {
                for (;;) {
                    Task task;
                    {
                        std::unique_lock<std::mutex> lk(mtx_);
                        if (delayed_.empty()) {
                            cvTimer_.wait(lk, [this] {
                                return stopping_.load(std::memory_order_relaxed)
                                       || !delayed_.empty();
                            });
                            if (delayed_.empty()
                                && stopping_.load(std::memory_order_relaxed)) {
                                return;
                            }
                            continue;
                        }
                        const auto next = delayed_.top().deadline;
                        // 等到最近 deadline，或队列头部被更换 / 进入停机。
                        cvTimer_.wait_until(lk, next, [this, &next] {
                            return stopping_.load(std::memory_order_relaxed)
                                   || delayed_.empty() || delayed_.top().deadline != next;
                        });
                        if (stopping_.load(std::memory_order_relaxed) && delayed_.empty()) {
                            return;
                        }
                        if (delayed_.empty() || delayed_.top().deadline
                            > std::chrono::steady_clock::now()) {
                            continue; // 头部被更换或尚未到期，重新等
                        }
                        task = std::move(const_cast<DelayedTask &>(delayed_.top()).task);
                        delayed_.pop();
                    }
                    // 到期任务在 timer 线程上内联执行（理由见类头注释）：
                    // 超时检查不依赖 worker 可用性，重试重投递（step1）本身
                    // 也是"投递后即返回"的快任务。持锁范围外执行。
                    runGuarded(task);
                }
            }

            static void runGuarded(const Task &task) {
                if (!task) return;
                try {
                    task();
                } catch (const std::exception &e) {
                    DBMW_LOG_ERROR(std::string("async executor task threw: ") + e.what());
                } catch (...) {
                    DBMW_LOG_ERROR("async executor task threw unknown exception");
                }
            }

            mutable std::mutex mtx_;
            std::condition_variable cvWork_;
            std::condition_variable cvTimer_;
            std::deque<Task> queue_;
            std::priority_queue<DelayedTask> delayed_;
            std::vector<std::thread> workers_;
            std::thread timerThread_;
            std::size_t threadCount_;
            std::size_t queueLimit_;
            std::uint64_t seq_ = 0;
            std::size_t active_ = 0;
            std::uint64_t submitted_ = 0;
            std::uint64_t completed_ = 0;
            std::uint64_t rejected_ = 0;
            bool threadsStarted_ = false;
            std::atomic<bool> stopping_{false};
        };
    } // namespace

    std::shared_ptr<IExecutor> makeThreadPoolExecutor(const int threads,
                                                      const std::size_t queueSize) {
        return std::make_shared<ThreadPoolExecutor>(threads, queueSize);
    }
} // namespace dbmw::async
