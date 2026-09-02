#include "dbmw/core/connection_pool.h"
#include "dbmw/common/logger.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>


namespace dbmw::core {
    void ConnectionPool::State::returnConn(
        std::unique_ptr<IDatabaseConnection> conn,
        const std::chrono::steady_clock::time_point createdAt,
        const std::chrono::steady_clock::time_point borrowedAt,
        const bool reusable) {
        if (!conn) return;
        const auto now = std::chrono::steady_clock::now();
        const bool leaked = leakDetectionThreshold > std::chrono::milliseconds(0) &&
                            now - borrowedAt >= leakDetectionThreshold;
        if (leaked) {
            DBMW_LOG_WARN("pool [" + poolName + "] possible connection leak: borrowed for "
                          + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - borrowedAt).count()) + "ms");
        }
        bool doClose = false;
        {
            std::lock_guard lk(mtx);
            if (leaked) ++leakWarnings;
            if (borrowed > 0) --borrowed;
            const bool expired = maxLifetime > std::chrono::milliseconds(0) &&
                                 now - createdAt >= maxLifetime;
            // 非池化模式不复用连接：归还即关闭，idle 队列始终为空。
            if (closed || expired || !reusable || !pooled) {
                doClose = true;
                ++connectionsClosed;
                if (!reusable) ++invalidatedConnections;
                else if (expired) ++lifetimeEvictions;
                if (total > 0) --total;
            } else {
                idle.push(IdleConnection{std::move(conn), createdAt, now});
            }
        }
        if (doClose) conn->close(); // close 可能涉及网络，放在锁外
        cv.notify_one();
    }

    ConnectionPool::ConnectionPool(std::unique_ptr<driver::IDriver> driver,
                                   config::DataSourceConfig cfg,
                                   const int minConn, const int maxConn,
                                   std::chrono::milliseconds borrowTimeout,
                                   std::chrono::milliseconds idleTimeout,
                                   std::chrono::milliseconds maxLifetime,
                                   std::chrono::milliseconds leakDetectionThreshold,
                                   std::chrono::milliseconds validationInterval,
                                   const bool metricsEnabled,
                                   const bool pooled)
        : state_(std::make_shared<State>()),
          driver_(std::move(driver)), cfg_(std::move(cfg)),
          minConn_(std::max(minConn, 0)),
          maxConn_(std::max(maxConn, 1)),
          borrowTimeout_(borrowTimeout),
          idleTimeout_(std::max(idleTimeout, std::chrono::milliseconds(0))),
          maxLifetime_(std::max(maxLifetime, std::chrono::milliseconds(0))) {
        if (minConn_ > maxConn_) minConn_ = maxConn_;
        state_->poolName = cfg_.name;
        state_->metricsEnabled = metricsEnabled;
        state_->minConnections = minConn_;
        state_->maxConnections = maxConn_;
        state_->maxLifetime = maxLifetime_;
        state_->leakDetectionThreshold = std::max(leakDetectionThreshold, std::chrono::milliseconds(0));
        state_->validationInterval = std::max(validationInterval, std::chrono::milliseconds(0));
        state_->pooled = pooled;

        // 预热到 min 条连接（失败只记日志，不致命）。
        // 非池化模式没有"池"可预热，跳过——每次 borrow 现建即可。
        for (int i = 0; pooled && i < minConn_; ++i) {
            auto code = common::ErrorCode::Ok;
            std::string err;
            auto conn = createConnection(code, err);
            if (!conn) {
                {
                    std::lock_guard<std::mutex> lk(state_->mtx);
                    ++state_->connectionCreateFailures;
                }
                DBMW_LOG_WARN("pool [" + name() + "] warmup failed: " + err);
                break; // 首条都建不上，后续也没必要重试
            }
            bool overflow = false;
            {
                std::lock_guard<std::mutex> lk(state_->mtx);
                if (state_->total >= maxConn_) overflow = true;
                else {
                    const auto now = std::chrono::steady_clock::now();
                    state_->idle.push(State::IdleConnection{std::move(conn), now, now});
                    ++state_->total;
                    ++state_->connectionsCreated;
                }
            }
            if (overflow) {
                conn->close();
                break;
            }
        }
    }

    ConnectionPool::~ConnectionPool() {
        shutdown(std::chrono::milliseconds(0));
    }

    ConnectionPool::Handle::~Handle() {
        if (!conn_) return;
        if (const auto st = state_.lock()) {
            st->returnConn(std::move(conn_), createdAt_, borrowedAt_,
                           reusable_.load()); // 池还在：归还
        } else {
            conn_->close(); // 池已销毁：只能自行关闭，绝不触碰已释放内存
        }
    }

    std::unique_ptr<ConnectionPool::Handle> ConnectionPool::borrow(std::string &error) const
    {
        auto code = common::ErrorCode::Ok;
        return borrow(code, error);
    }

    std::unique_ptr<ConnectionPool::Handle> ConnectionPool::borrow(common::ErrorCode &code,
                                                                  std::string &error,
                                                                  std::chrono::milliseconds timeout) const
    {
        if (timeout < std::chrono::milliseconds(0)) timeout = borrowTimeout_;
        if (timeout < std::chrono::milliseconds(0)) timeout = std::chrono::milliseconds(0);

        code = common::ErrorCode::Ok;
        error.clear();

        const auto borrowStarted = std::chrono::steady_clock::now();
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::unique_lock<std::mutex> lk(state_->mtx);
        const bool metrics = state_->metricsEnabled;
        if (metrics) ++state_->borrowRequests;
        // 等待耗时在所有退出路径都计入（含超时/失败），否则拥堵时指标反而"好看"（P1-3）。
        // pool_metrics 关闭时不累加任何仅用于上报的计数（P2-7）。
        const auto recordWait = [&] {
            if (!metrics) return;
            const auto waited = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - borrowStarted);
            state_->totalBorrowWait += waited;
            state_->maxBorrowWait = std::max(state_->maxBorrowWait, waited);
        };
        const auto recordSuccess = [&] {
            recordWait();
            if (!metrics) return;
            ++state_->borrowSuccesses;
            state_->maxBorrowed = std::max(state_->maxBorrowed, state_->borrowed);
        };

        // 非池化模式：不查空闲队列、不排队等待、不受 max 名额限制，
        // 直接新建一条连接；用完由 Handle 析构归还，returnConn 见 !pooled 即关闭。
        // 这样上层（DataSource / Session）完全不必区分两种模式。
        if (!state_->pooled) {
            if (state_->closed) {
                code = common::ErrorCode::PoolClosed;
                error = "datasource '" + name() + "' is closed";
                recordWait();
                return nullptr;
            }
            lk.unlock();
            auto conn = createConnection(code, error);
            lk.lock();
            if (!conn) {
                // createConnection 已填充 code/error（ConnectionFailed / DriverDisabled 等）
                ++state_->connectionCreateFailures;
                recordWait();
                return nullptr;
            }
            if (state_->closed) {
                conn->close();
                code = common::ErrorCode::PoolClosed;
                error = "datasource '" + name() + "' is closed";
                recordWait();
                return nullptr;
            }
            ++state_->total; // 让 stats() 的 total 反映当前在用的直连数
            ++state_->borrowed;
            ++state_->connectionsCreated;
            recordSuccess();
            const auto now = std::chrono::steady_clock::now();
            return std::unique_ptr<Handle>(new Handle(state_, std::move(conn), now, now));
        }

        for (;;) {
            if (state_->closed) {
                code = common::ErrorCode::PoolClosed;
                error = "pool '" + name() + "' is closed";
                recordWait();
                return nullptr;
            }

            // 1) 复用空闲连接：持锁取出 -> 锁外 ping 校验 -> 回锁判定
            if (!state_->idle.empty()) {
                auto item = std::move(state_->idle.front());
                state_->idle.pop();

                lk.unlock();
                const auto now = std::chrono::steady_clock::now();
                const bool expired = maxLifetime_ > std::chrono::milliseconds(0) &&
                                     now - item.createdAt >= maxLifetime_;
                // 节流：距上次校验不足 validationInterval 时跳过 ping，省一次 RTT。
                // 大多数连接刚被用过，毫秒级内失效概率极低；真正坏掉的连接由
                // 心跳线程按 heartbeat_interval_ms 独立体检兜底。
                const bool needsPing = state_->validationInterval <=
                                           std::chrono::milliseconds(0) ||
                                       (now - item.lastValidated) >=
                                           state_->validationInterval;
                const bool alive = !expired && item.conn &&
                                   (!needsPing || item.conn->ping().ok());
                if (alive) item.lastValidated = now; // 校验通过或免校验都刷新时间戳
                if (!alive && item.conn) item.conn->close();
                lk.lock();

                if (alive) {
                    if (state_->closed) {
                        code = common::ErrorCode::PoolClosed;
                        error = "pool '" + name() + "' is closed";
                        item.conn->close();
                        recordWait();
                        return nullptr;
                    }
                    ++state_->borrowed;
                    recordSuccess();
                    // 注意：Handle 构造函数是私有的，std::make_unique 无法访问，
                    // 这里必须直接 new。
                    return std::unique_ptr<Handle>(new Handle(
                        state_, std::move(item.conn), item.createdAt,
                        std::chrono::steady_clock::now()));
                }

                // 失效连接出局
                --state_->total;
                ++state_->connectionsClosed;
                if (expired) ++state_->lifetimeEvictions;
                else ++state_->validationFailures;
                if (state_->total < 0) state_->total = 0;
                // 腾出了一个名额，唤醒一个等待者，否则它要空等到自己的 deadline。
                state_->cv.notify_one();
                continue;
            }

            // 2) 无空闲且未达上限：先预定名额，再到锁外建立连接
            if (state_->total < maxConn_) {
                ++state_->total;

                lk.unlock();
                auto conn = createConnection(code, error);
                lk.lock();

                if (conn) {
                    if (state_->closed) {
                        code = common::ErrorCode::PoolClosed;
                        error = "pool '" + name() + "' is closed";
                        conn->close();
                        --state_->total;
                        recordWait();
                        return nullptr;
                    }
                    ++state_->borrowed;
                    ++state_->connectionsCreated;
                    recordSuccess();
                    const auto now = std::chrono::steady_clock::now();
                    return std::unique_ptr<Handle>(new Handle(
                        state_, std::move(conn), now, now));
                }

                // createConnection 已填充 code/error（ConnectionFailed / DriverDisabled 等）
                --state_->total;
                ++state_->connectionCreateFailures;
                if (state_->total < 0) state_->total = 0;
                // 预定名额已释放，唤醒一个等待者去接手，避免它干等到超时。
                state_->cv.notify_one();
                recordWait();
                return nullptr;
            }

            // 3) 池已满：等待归还，或超时报错（不再无限阻塞）
            if (timeout == std::chrono::milliseconds(0)) {
                if (metrics) ++state_->borrowTimeouts;
                code = common::ErrorCode::PoolExhausted;
                error = "pool '" + name() + "' exhausted: waited "
                        + std::to_string(timeout.count()) + "ms (max="
                        + std::to_string(maxConn_) + ", total="
                        + std::to_string(state_->total) + ", borrowed="
                        + std::to_string(state_->borrowed) + ")";
                recordWait();
                return nullptr;
            }
            ++state_->waiting;
            if (metrics) state_->maxWaiting = std::max(state_->maxWaiting, state_->waiting);
            const auto waitResult = state_->cv.wait_until(lk, deadline);
            --state_->waiting;
            if (waitResult == std::cv_status::timeout) {
                if (metrics) ++state_->borrowTimeouts;
                code = common::ErrorCode::PoolExhausted;
                error = "pool '" + name() + "' exhausted: waited "
                        + std::to_string(timeout.count()) + "ms (max="
                        + std::to_string(maxConn_) + ", total="
                        + std::to_string(state_->total) + ", borrowed="
                        + std::to_string(state_->borrowed) + ")";
                recordWait();
                return nullptr;
            }
        }
    }

    void ConnectionPool::healthCheck() const
    {
        // 非池化模式没有空闲连接可体检，也不维持 min 水位，心跳直接空转。
        if (!state_->pooled) return;

        // 阶段 1：逐条取出空闲连接在锁外探测。
        //        一次只取一条，保证心跳期间业务线程仍能借到其余连接；
        //        健康连接放回队尾，实现轮转，避免总探测同一批。
        const size_t snapshot = idleCount();
        for (size_t i = 0; i < snapshot; ++i) {
            State::IdleConnection item;
            bool retireForIdle = false;
            {
                std::lock_guard<std::mutex> lk(state_->mtx);
                if (state_->closed || state_->idle.empty()) break;
                item = std::move(state_->idle.front());
                state_->idle.pop();
                retireForIdle = idleTimeout_ > std::chrono::milliseconds(0) &&
                                std::chrono::steady_clock::now() - item.returnedAt >= idleTimeout_ &&
                                state_->total > minConn_;
            }

            const auto now = std::chrono::steady_clock::now();
            const bool expired = maxLifetime_ > std::chrono::milliseconds(0) &&
                                 now - item.createdAt >= maxLifetime_;
            const bool alive = !retireForIdle && !expired && item.conn && item.conn->ping().ok();

            {
                std::lock_guard<std::mutex> lk(state_->mtx);
                if (alive && !state_->closed) {
                    state_->idle.push(std::move(item));
                } else {
                    if (item.conn) item.conn->close();
                    --state_->total;
                    ++state_->connectionsClosed;
                    if (retireForIdle) ++state_->idleEvictions;
                    else if (expired) ++state_->lifetimeEvictions;
                    else ++state_->validationFailures;
                    if (state_->total < 0) state_->total = 0;
                }
            }
        }

        // 阶段 2：算出缺口，预定名额后到锁外建连（connect 是慢 IO，绝不在持锁时做）
        int need = 0;
        {
            std::lock_guard<std::mutex> lk(state_->mtx);
            if (state_->closed) return;
            const int want = minConn_ - state_->total;
            const int room = maxConn_ - state_->total;
            need = std::min(std::max(want, 0), std::max(room, 0));
            state_->total += need; // 预定名额，防止并发过度建连
        }

        std::vector<State::IdleConnection> fresh;
        fresh.reserve(static_cast<size_t>(need));
        for (int i = 0; i < need; ++i) {
            auto code = common::ErrorCode::Ok;
            std::string err;
            auto conn = createConnection(code, err);
            if (!conn) {
                {
                    std::lock_guard<std::mutex> lk(state_->mtx);
                    ++state_->connectionCreateFailures;
                }
                DBMW_LOG_WARN("pool [" + name() + "] heartbeat refill failed: " + err);
                break; // 建连失败通常不会立刻恢复，停止本轮补充
            }
            const auto now = std::chrono::steady_clock::now();
            fresh.push_back(State::IdleConnection{std::move(conn), now, now});
        }

        // 阶段 3：回写入池，并把未用掉的名额还回去
        {
            std::lock_guard<std::mutex> lk(state_->mtx);
            state_->total -= (need - static_cast<int>(fresh.size()));
            state_->connectionsCreated += fresh.size();
            if (state_->total < 0) state_->total = 0;
            for (auto &item: fresh) {
                if (state_->closed) item.conn->close();
                else state_->idle.push(std::move(item));
            }
            if (!fresh.empty()) state_->cv.notify_all();
        }
    }

    void ConnectionPool::shutdown(const std::chrono::milliseconds grace) const
    {
        std::unique_lock<std::mutex> lk(state_->mtx);
        state_->closed = true;
        state_->cv.notify_all(); // 唤醒所有等待借出的线程，让它们快速失败

        while (!state_->idle.empty()) {
            auto item = std::move(state_->idle.front());
            state_->idle.pop();
            lk.unlock();
            item.conn->close();
            lk.lock();
            ++state_->connectionsClosed;
        }

        if (grace > std::chrono::milliseconds(0)) {
            const auto deadline = std::chrono::steady_clock::now() + grace;
            while (state_->borrowed > 0) {
                if (state_->cv.wait_until(lk, deadline) == std::cv_status::timeout) break;
            }
        }

        // 仍未归还的连接由各自的 Handle 析构时关闭（State::returnConn 见 closed 即关闭）
        state_->total = state_->borrowed;
    }

    size_t ConnectionPool::idleCount() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        return state_->idle.size();
    }

    size_t ConnectionPool::totalCount() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        return static_cast<size_t>(state_->total);
    }

    size_t ConnectionPool::borrowedCount() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        return static_cast<size_t>(state_->borrowed);
    }

    bool ConnectionPool::closed() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        return state_->closed;
    }

    ConnectionPool::Stats ConnectionPool::stats() const {
        std::lock_guard<std::mutex> lk(state_->mtx);
        Stats out;
        out.minConnections = static_cast<size_t>(std::max(0, state_->minConnections));
        out.maxConnections = static_cast<size_t>(std::max(0, state_->maxConnections));
        out.idle = state_->idle.size();
        out.total = static_cast<size_t>(std::max(0, state_->total));
        out.borrowed = static_cast<size_t>(std::max(0, state_->borrowed));
        out.waiting = static_cast<size_t>(std::max(0, state_->waiting));
        if (!state_->metricsEnabled) return out;
        out.connectionsCreated = state_->connectionsCreated;
        out.connectionsClosed = state_->connectionsClosed;
        out.borrowTimeouts = state_->borrowTimeouts;
        out.validationFailures = state_->validationFailures;
        out.leakWarnings = state_->leakWarnings;
        out.maxBorrowed = static_cast<size_t>(std::max(0, state_->maxBorrowed));
        out.maxWaiting = static_cast<size_t>(std::max(0, state_->maxWaiting));
        out.borrowRequests = state_->borrowRequests;
        out.borrowSuccesses = state_->borrowSuccesses;
        out.connectionCreateFailures = state_->connectionCreateFailures;
        out.invalidatedConnections = state_->invalidatedConnections;
        out.idleEvictions = state_->idleEvictions;
        out.lifetimeEvictions = state_->lifetimeEvictions;
        out.totalBorrowWait = state_->totalBorrowWait;
        out.maxBorrowWait = state_->maxBorrowWait;
        return out;
    }

    std::unique_ptr<IDatabaseConnection> ConnectionPool::createConnection(common::ErrorCode &code,
                                                                        std::string &error) const {
        code = common::ErrorCode::Ok;
        error.clear();

        auto conn = driver_->createConnection();
        if (!conn) {
            code = common::ErrorCode::ConnectionFailed;
            error = "driver '" + std::string(driver_->name()) + "' returned a null connection";
            return nullptr;
        }

        if (const auto st = conn->connect(cfg_); !st.ok()) {
            code = st.code; // 原样透出 ConnectionFailed / DriverDisabled / ConfigError
            error = st.message;
            return nullptr;
        }
        return conn;
    }
} // namespace dbmw::core
