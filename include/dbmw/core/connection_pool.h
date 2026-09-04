#ifndef DBMW_CORE_CONNECTION_POOL_H
#define DBMW_CORE_CONNECTION_POOL_H

#include "dbmw/core/idatabase_connection.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/driver/idriver.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>


namespace dbmw::core {
    // 异步借出的投递通道：由 async 层在调用 borrowAsync 时注入。
    //
    // 刻意不直接依赖 dbmw::async::IExecutor（两个 std::function 足够），
    // 避免 core 反向依赖 async 模块——池只关心"任务去哪跑、回调去哪投"，
    // 不关心线程池本身。
    struct AsyncIo {
        // 在执行器线程上跑阻塞 IO（建连 / ping）。必须不抛异常。
        std::function<void(std::function<void()>)> post;
        // 投递完成回调。保证不在池锁内、不在发起调用的栈上执行（I1 不变量）。
        // 必须不抛异常。
        std::function<void(std::function<void()>)> deliver;

        [[nodiscard]] bool usable() const { return static_cast<bool>(post) && static_cast<bool>(deliver); }
    };

    // 单个数据源的连接池，线程安全。
    //
    // 用法：通过 borrow() 获取一个 RAII 句柄 Handle；句柄析构时自动归还连接。
    // 借出前会做一次 ping 校验，失效连接会被丢弃并重建。
    //
    // 池化可按数据源开关（pool.enabled = false）：关闭后不再复用连接，每次 borrow
    // 新建、归还即关闭，也不再预热与心跳补连接。上层仍通过同一套 Handle 使用，
    // 无需区分两种模式。
    //
    // 生命周期：池的可变状态放在 shared_ptr<State> 中，Handle 只持有
    // weak_ptr<State>。因此即使池先被销毁，仍在栈上的 Handle 析构时也不会
    // 触碰已释放内存——它会直接关闭自己那条连接。
    class ConnectionPool : public std::enable_shared_from_this<ConnectionPool> {
        // 池的可变状态，独立于 ConnectionPool 对象存在，
        // 使已借出的 Handle 在池销毁后仍能安全收尾。完整定义在下方私有段。
        struct State;

    public:
        struct Stats {
            size_t minConnections = 0;
            size_t maxConnections = 0;
            size_t idle = 0;
            size_t total = 0;
            size_t borrowed = 0;
            size_t waiting = 0;
            std::uint64_t connectionsCreated = 0;
            std::uint64_t connectionsClosed = 0;
            std::uint64_t borrowTimeouts = 0;
            std::uint64_t validationFailures = 0;
            std::uint64_t leakWarnings = 0;
            size_t maxBorrowed = 0;
            size_t maxWaiting = 0;
            std::uint64_t borrowRequests = 0;
            std::uint64_t borrowSuccesses = 0;
            std::uint64_t connectionCreateFailures = 0;
            std::uint64_t invalidatedConnections = 0;
            std::uint64_t idleEvictions = 0;
            std::uint64_t lifetimeEvictions = 0;
            std::chrono::microseconds totalBorrowWait{0};
            std::chrono::microseconds maxBorrowWait{0};
            // 异步等待者队列当前长度（v0.2.0；追加在末尾保证旧代码聚合初始化兼容）。
            std::size_t asyncWaiting = 0;
            [[nodiscard]] double utilization() const {
                return maxConnections == 0 ? 0.0
                    : static_cast<double>(borrowed) / static_cast<double>(maxConnections);
            }
        };

        // 借出连接的默认等待上限。
        static constexpr std::chrono::milliseconds kDefaultBorrowTimeout{30000};

        ConnectionPool(std::unique_ptr<driver::IDriver> driver,
                       config::DataSourceConfig cfg,
                       int minConn, int maxConn,
                       std::chrono::milliseconds borrowTimeout = kDefaultBorrowTimeout,
                       std::chrono::milliseconds idleTimeout = std::chrono::milliseconds(0),
                       std::chrono::milliseconds maxLifetime = std::chrono::milliseconds(0),
                       std::chrono::milliseconds leakDetectionThreshold =
                           std::chrono::milliseconds(0),
                       std::chrono::milliseconds validationInterval =
                           std::chrono::milliseconds(500),
                       bool metricsEnabled = true,
                       bool pooled = true);

        ~ConnectionPool();

        ConnectionPool(const ConnectionPool &) = delete;

        ConnectionPool &operator=(const ConnectionPool &) = delete;

        // RAII 句柄：析构时自动把连接归还给池（池已销毁则直接关闭连接）。
        class Handle {
        public:
            ~Handle();

            Handle(const Handle &) = delete;

            Handle &operator=(const Handle &) = delete;

            // std::atomic 不可移动，因此移动操作必须手写。
            Handle(Handle &&other) noexcept
                : state_(std::move(other.state_)), conn_(std::move(other.conn_)),
                  createdAt_(other.createdAt_), borrowedAt_(other.borrowedAt_),
                  reusable_(other.reusable_.load()) {
            }

            Handle &operator=(Handle &&other) noexcept {
                if (this != &other) {
                    state_ = std::move(other.state_);
                    conn_ = std::move(other.conn_);
                    createdAt_ = other.createdAt_;
                    borrowedAt_ = other.borrowedAt_;
                    reusable_.store(other.reusable_.load());
                }
                return *this;
            }

            IDatabaseConnection *operator->() const { return conn_.get(); }
            [[nodiscard]] IDatabaseConnection *get() const { return conn_.get(); }

            // 标记连接不可复用；句柄析构时会关闭而不是放回空闲队列。
            //
            // 这个标记会被两条线程并发写：业务线程（语句失败后标记）和
            // 事务超时看门狗线程（cancel 之后标记）。因此必须是原子的，
            // 否则与 Handle 析构函数里的读取构成 data race（UB）。
            void invalidate() { reusable_.store(false); }

            [[nodiscard]] bool reusable() const { return reusable_.load(); }

        private:
            friend class ConnectionPool;

            Handle(std::weak_ptr<State> state, std::unique_ptr<IDatabaseConnection> conn,
                   std::chrono::steady_clock::time_point createdAt,
                   std::chrono::steady_clock::time_point borrowedAt)
                : state_(std::move(state)), conn_(std::move(conn)),
                  createdAt_(createdAt), borrowedAt_(borrowedAt) {
            }

            std::weak_ptr<State> state_;
            std::unique_ptr<IDatabaseConnection> conn_;
            std::chrono::steady_clock::time_point createdAt_;
            std::chrono::steady_clock::time_point borrowedAt_;
            std::atomic<bool> reusable_{true};
        };

        // 借出一条连接。
        //
        // 成功返回非 null 句柄；失败返回 nullptr，并通过 code/error 给出真实原因
        // （池已关闭 / 等待超时 / 连接建立失败 / 驱动未启用 等）。
        // timeout < 0 表示使用池的默认超时；timeout == 0 表示不等待。
        std::unique_ptr<Handle> borrow(common::ErrorCode &code, std::string &error,
                                       std::chrono::milliseconds timeout =
                                           std::chrono::milliseconds(-1)) const;

        // 便捷重载：只取错误描述，错误码可从返回的 Status 里另行判断。
        std::unique_ptr<Handle> borrow(std::string &error) const;

        // 异步借出（v0.2.0）：在任何线程调用都立即返回，绝不阻塞。
        //
        // 结果（含失败）经 io.deliver 投递到 io 指定的线程上执行——既不在
        // 池锁内、也不在调用栈上（I1 不变量）。决策表：
        //   - 池已关闭            → complete(nullptr, PoolClosed)
        //   - idle 有新鲜连接      → 直接交付（免 ping）
        //   - idle 有过期/待校验连接 → io.post 一个 ping 任务，活着交付、
        //                            死了递归走建连/入队
        //   - idle 空且 total < max → io.post 一个建连任务（占位后回滚或交付）
        //   - idle 空且 total == max → 挂入等待者队列，归还时直接交接；
        //                            deadline 到点由健康检查/归还路径清理，
        //                            交付 PoolExhausted
        // timeout 语义与 borrow() 一致：<0 = 池默认；0 = 不等待。
        // io 与 complete 会被拷贝进等待者（若入队），必须保持轻量且自包含。
        void borrowAsync(std::chrono::milliseconds timeout,
                         const AsyncIo &io,
                         std::function<void(std::unique_ptr<Handle>, common::Status)> complete) const;

        // 心跳调用：检查空闲连接健康度，失效的丢弃，并补足到 min。
        // 所有 ping / connect 都在锁外执行，不会阻塞业务线程借出连接。
        void healthCheck() const;

        // 关闭池：唤醒所有等待者、关闭空闲连接，并等待借用中的连接归还
        // （最多等 grace）。超时尚未归还的连接会在其 Handle 析构时自行关闭。
        void shutdown(std::chrono::milliseconds grace = std::chrono::milliseconds(5000)) const;

        [[nodiscard]] const std::string &name() const { return cfg_.name; }

        [[nodiscard]] size_t idleCount() const;

        [[nodiscard]] size_t totalCount() const;

        [[nodiscard]] size_t borrowedCount() const;

        [[nodiscard]] bool closed() const;

        [[nodiscard]] Stats stats() const;

    private:
        // 池的可变状态。独立于 ConnectionPool 对象存在，
        // 使已借出的 Handle 在池销毁后仍能安全收尾。
        //
        // 继承 enable_shared_from_this：归还路径的"直接交接"要在 State 方法里
        // 构造 Handle（其构造函数需要 weak_ptr<State>），只有从这里才拿得到。
        struct State : std::enable_shared_from_this<State> {
            struct IdleConnection {
                std::unique_ptr<IDatabaseConnection> conn;
                std::chrono::steady_clock::time_point createdAt;
                std::chrono::steady_clock::time_point returnedAt;
                // 上次 ping 校验时刻；借出时距此刻不足 validationInterval 就跳过
                // ping（省一次 RTT）。默认 epoch，保证刚建好的连接首次借出必校验。
                std::chrono::steady_clock::time_point lastValidated{};
            };

            // 异步借出等待者：池满时挂起（不占任何线程），
            // 归还路径直接把连接交给队首等待者（跳过 idle 队列与借出 ping）。
            struct AsyncWaiter {
                std::chrono::steady_clock::time_point enqueuedAt;
                std::chrono::steady_clock::time_point deadline;
                std::function<void(std::unique_ptr<Handle>, common::Status)> complete;
                AsyncIo io;
            };

            std::mutex mtx;
            std::condition_variable cv;
            std::queue<IdleConnection> idle;
            int total = 0;    // 池已知的连接总数 = 空闲 + 借用中 + 心跳检查中
            int borrowed = 0; // 当前借用中数量
            int waiting = 0;
            bool closed = false;
            bool metricsEnabled = true;
            // 池化开关。关闭后本对象退化为"连接工厂"：每次 borrow 新建一条连接、
            // 归还时直接关闭，空闲队列与 min/max 名额都不再参与。
            bool pooled = true;
            std::string poolName;
            std::chrono::milliseconds maxLifetime{0};
            std::chrono::milliseconds leakDetectionThreshold{0};
            std::chrono::milliseconds validationInterval{500};
            std::uint64_t connectionsCreated = 0;
            std::uint64_t connectionsClosed = 0;
            std::uint64_t borrowTimeouts = 0;
            std::uint64_t validationFailures = 0;
            std::uint64_t leakWarnings = 0;
            int minConnections = 0;
            int maxConnections = 0;
            int maxBorrowed = 0;
            int maxWaiting = 0;
            std::uint64_t borrowRequests = 0;
            std::uint64_t borrowSuccesses = 0;
            std::uint64_t connectionCreateFailures = 0;
            std::uint64_t invalidatedConnections = 0;
            std::uint64_t idleEvictions = 0;
            std::uint64_t lifetimeEvictions = 0;
            std::chrono::microseconds totalBorrowWait{0};
            std::chrono::microseconds maxBorrowWait{0};
            // 异步等待者队列（FIFO + 各自带 deadline）。
            std::deque<AsyncWaiter> asyncWaiters;

            // 归还连接。池已关闭时直接关闭连接，不再入池。
            void returnConn(std::unique_ptr<IDatabaseConnection> conn,
                            std::chrono::steady_clock::time_point createdAt,
                            std::chrono::steady_clock::time_point borrowedAt,
                            bool reusable);
        };

        // 建立一条新连接（网络 IO）。失败时填充 code/error 返回 nullptr。
        std::unique_ptr<IDatabaseConnection> createConnection(common::ErrorCode &code,
                                                             std::string &error) const;

        // 清理已过期的异步等待者：锁内弹出并组装错误（快照 total/borrowed），
        // 锁外经 io.deliver 投递 PoolExhausted。由健康检查与 borrowAsync 搭便车调用。
        void expireWaiters() const;

        // 投递建连任务到 io.post：网络 IO 全程锁外。slotReserved 表示调用方
        // 是否已预定了池名额（池化模式 ++total；非池化模式传 false）。
        void postCreateTask(const AsyncIo &io,
                            std::function<void(std::unique_ptr<Handle>, common::Status)> complete,
                            bool slotReserved) const;

        std::shared_ptr<State> state_;
        std::unique_ptr<driver::IDriver> driver_;
        config::DataSourceConfig cfg_;
        int minConn_;
        int maxConn_;
        std::chrono::milliseconds borrowTimeout_;
        std::chrono::milliseconds idleTimeout_;
        std::chrono::milliseconds maxLifetime_;
    };
} // namespace dbmw::core


#endif // DBMW_CORE_CONNECTION_POOL_H
