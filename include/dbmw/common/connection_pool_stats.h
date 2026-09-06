#ifndef DBMW_COMMON_CONNECTION_POOL_STATS_H
#define DBMW_COMMON_CONNECTION_POOL_STATS_H

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

// dbmw::common::ConnectionPoolStats：把池指标下沉到 common 层，解除
// common/observer.h 与 core/database_manager.h 之间的循环 include。
//
// 设计动机：
//   - M3 引入 PoolMetricsEvent（含 NamedPoolStats）必须放在 common 层（设计稿 §5.2）；
//   - observer.h 已经为 OperationType/Status 暴露给 core/database_manager.h 使用；
//   - 若 NamedPoolStats/Stats 留在 core 层，observer.h 引用它就要 include
//     database_manager.h，反向触发循环；
//   - Stats 是纯数据（+ 一个 inline utilization()），下沉到 common 不会破坏
//     任何现有调用方——core/connection_pool.h 仍暴露同名别名
//     `using Stats = common::ConnectionPoolStats;`。
namespace dbmw::common {
    struct ConnectionPoolStats {
        std::size_t minConnections = 0;
        std::size_t maxConnections = 0;
        std::size_t idle = 0;
        std::size_t total = 0;
        std::size_t borrowed = 0;
        std::size_t waiting = 0;
        std::uint64_t connectionsCreated = 0;
        std::uint64_t connectionsClosed = 0;
        std::uint64_t borrowTimeouts = 0;
        std::uint64_t validationFailures = 0;
        std::uint64_t leakWarnings = 0;
        std::size_t maxBorrowed = 0;
        std::size_t maxWaiting = 0;
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

    struct NamedPoolStats {
        std::string dataSource;
        ConnectionPoolStats stats;
    };
} // namespace dbmw::common

#endif // DBMW_COMMON_CONNECTION_POOL_STATS_H
