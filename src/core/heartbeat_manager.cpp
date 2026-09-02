#include "dbmw/core/heartbeat_manager.h"
#include "dbmw/core/connection_pool.h"
#include "dbmw/common/logger.h"
#include <thread>
#include <string>
#include <utility>


namespace dbmw::core {
    HeartbeatManager::HeartbeatManager(std::chrono::milliseconds interval)
        : interval_(interval) {
    }

    HeartbeatManager::~HeartbeatManager() {
        stop();
    }

    void HeartbeatManager::addPool(const std::shared_ptr<ConnectionPool>& pool) {
        if (!pool) return;
        std::lock_guard<std::mutex> lk(poolsMtx_);
        pools_.push_back(pool);
    }

    void HeartbeatManager::start() {
        if (running_.exchange(true)) return;
        stopFlag_ = false;
        thread_ = std::thread([this]() { loop(); });
    }

    void HeartbeatManager::stop() {
        if (!running_.exchange(false)) return;
        stopFlag_ = true;
        waitCv_.notify_all();
        if (thread_.joinable()) thread_.join();
    }

    void HeartbeatManager::sweepExpiredPools() {
        // 清掉已销毁的池对应的弱引用，避免向量无限增长。
        std::lock_guard<std::mutex> lk(poolsMtx_);
        std::vector<std::weak_ptr<ConnectionPool> > alive;
        alive.reserve(pools_.size());
        for (auto &w: pools_) {
            if (!w.expired()) alive.push_back(std::move(w));
        }
        pools_.swap(alive);
    }

    void HeartbeatManager::loop() {
        DBMW_LOG_INFO("heartbeat manager started, interval="
            + std::to_string(interval_.count()) + "ms");
        while (!stopFlag_) {
            {
                std::unique_lock<std::mutex> lock(waitMtx_);
                if (waitCv_.wait_for(lock, interval_, [this] { return stopFlag_.load(); }))
                    break;
            }

            // 先快照出本轮要检查的池（提升为 shared_ptr），
            // 之后整轮循环都不持锁——healthCheck 本身耗时（ping/connect）。
            std::vector<std::shared_ptr<ConnectionPool> > snapshot;
            {
                std::lock_guard<std::mutex> lk(poolsMtx_);
                snapshot.reserve(pools_.size());
                for (const auto &w: pools_) {
                    if (auto p = w.lock()) snapshot.push_back(std::move(p));
                }
            }
            for (auto &p: snapshot) {
                if (stopFlag_) break;
                try {
                    p->healthCheck();
                } catch (const std::exception &e) {
                    DBMW_LOG_WARN("heartbeat healthCheck threw: " + std::string(e.what()));
                } catch (...) {
                    DBMW_LOG_WARN("heartbeat healthCheck threw an unknown exception");
                }
            }
            sweepExpiredPools();
        }
        DBMW_LOG_INFO("heartbeat manager stopped");
    }
} // namespace dbmw::core
