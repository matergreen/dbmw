#ifndef DBMW_CORE_QUERY_CACHE_H
#define DBMW_CORE_QUERY_CACHE_H

#include "dbmw/common/types.h"
#include "dbmw/config/datasource_config.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>


namespace dbmw::core {
    // 查询结果缓存：仅缓存非事务、非会话的读（DataSource::query 路径）。
    //
    // 全局单例，按 (dataSource + key) 索引；key = 结构模板 + 参数 + 数据源名。
    // 命中返回 ResultSet 深拷贝；写后 invalidate 按数据源名清除。TTL 过期自动失效。
    class QueryCache {
    public:
        QueryCache() = delete;

        static void configure(const config::QueryCacheConfig &cfg);

        // enabled()/replicaOnly() 读原子标志而不进 mtx_。
        //
        // 这两个是每次 query() 都要问一遍的开关。走 mtx_ 的话，即使缓存关着
        // （默认就是关着），所有数据源的所有读也要在同一把全局锁上排一次队——
        // 一个默认关闭的功能不该给热路径留下这种代价。
        static bool enabled();
        // 是否仅缓存打到副本的读（cache_on_replica_only）。
        static bool replicaOnly();

        // 命中则把缓存结果拷贝到 out，返回 true。
        static bool get(const std::string &dataSource, const std::string &key,
                        common::ResultSet &out);
        // 写入/更新缓存项。
        static void put(const std::string &dataSource, const std::string &key,
                        const common::ResultSet &rs);
        // 写后失效：清除该数据源全部缓存项。
        static void invalidate(const std::string &dataSource);

        // 运行时快照。
        //
        // 没有命中率的缓存是不可运维的：命中率低说明它只在白耗内存，
        // 高淘汰数说明上限设小了，两者都只能靠计数器看出来。
        struct Stats {
            std::size_t entries = 0;
            std::size_t approxBytes = 0;
            std::uint64_t hits = 0;
            std::uint64_t misses = 0;   // 含 TTL 过期
            std::uint64_t evictions = 0; // 因条目数/内存上限被淘汰
            // 写后失效清掉的条目数。它和 evictions 要分开看：
            // 前者说明"写太频繁，缓存留不住"，后者说明"上限太小"，处置方式完全不同。
            std::uint64_t invalidations = 0;
        };

        static Stats stats();

    private:
        struct Entry {
            common::ResultSet rs;
            std::chrono::steady_clock::time_point expire;
            std::list<std::string>::iterator lru;
            // 该条目的近似内存占用，缓存时算一次。
            // 不这么做就只能在淘汰时重新遍历结果集，
            // 那等于把"限内存"的代价加在每一次写缓存上。
            std::size_t bytes = 0;
        };

        // 估算一个结果集的近似内存占用（列名 + 各列值），不含容器自身开销。
        static std::size_t approxBytes(const common::ResultSet &rs);

        // 调用方必须已持有 mtx_：按 LRU 尾部淘汰直到条目数与内存都在上限内。
        // reserveSlot=true 表示随后还要插入一条新条目，需要预留一个名额；
        // false 用于"原地更新已有条目"——此时条目数没变，用 >= 判定会把
        // 刚刚更新的那一条自己淘汰掉（max_entries=1 时必然发生）。
        static void evictLocked(std::size_t incomingBytes, bool reserveSlot);

        // 调用方必须已持有 mtx_。
        static void eraseLocked(std::unordered_map<std::string, Entry>::iterator it);

        static std::mutex mtx_;
        static config::QueryCacheConfig cfg_;
        static std::unordered_map<std::string, Entry> store_; // key = ds + '\0' + key
        static std::list<std::string> lru_;                    // 最近使用在表头
        static std::size_t totalBytes_;                        // store_ 内所有条目的近似字节和
        // 计数器用原子而非 mtx_ 保护：configure() 会清空缓存，但这几个数是
        // 进程累计量，清缓存不该把历史命中率一起抹掉。
        // cfg_ 里 enabled / cache_on_replica_only 的无锁镜像，供热路径读。
        static std::atomic<bool> enabled_;
        static std::atomic<bool> replicaOnly_;
        static std::atomic<std::uint64_t> hits_;
        static std::atomic<std::uint64_t> misses_;
        static std::atomic<std::uint64_t> evictions_;
        static std::atomic<std::uint64_t> invalidations_;
    };
} // namespace dbmw::core

#endif // DBMW_CORE_QUERY_CACHE_H
