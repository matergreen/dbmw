#include "dbmw/core/query_cache.h"

#include <utility>
#include <variant>


namespace dbmw::core {
    std::mutex QueryCache::mtx_;
    config::QueryCacheConfig QueryCache::cfg_;
    std::unordered_map<std::string, QueryCache::Entry> QueryCache::store_;
    std::list<std::string> QueryCache::lru_;
    std::size_t QueryCache::totalBytes_ = 0;
    std::atomic<bool> QueryCache::enabled_{false};
    std::atomic<bool> QueryCache::replicaOnly_{false};
    std::atomic<std::uint64_t> QueryCache::hits_{0};
    std::atomic<std::uint64_t> QueryCache::misses_{0};
    std::atomic<std::uint64_t> QueryCache::evictions_{0};
    std::atomic<std::uint64_t> QueryCache::invalidations_{0};

    namespace {
        // 组合键：数据源名 + NUL + 查询键，避免不同数据源的同名 key 互相命中。
        // std::string 可以安全承载嵌入的 NUL，因此 NUL 是最省事的分隔符——
        // 数据源名里不可能出现它。
        std::string compositeKey(const std::string &dataSource, const std::string &key) {
            std::string ck;
            ck.reserve(dataSource.size() + 1 + key.size());
            ck += dataSource;
            ck.push_back('\0');
            ck += key;
            return ck;
        }

        std::size_t valueBytes(const common::Value &v) {
            if (const auto *p = std::get_if<std::string>(&v)) return p->size();
            if (const auto *p = std::get_if<common::Blob>(&v)) return p->size();
            return sizeof(common::Value);
        }
    }

    std::size_t QueryCache::approxBytes(const common::ResultSet &rs) {
        std::size_t bytes = 0;
        for (const auto &f: rs.fields()) bytes += f.size() + sizeof(std::string);
        for (const auto &row: rs.rows()) {
            for (const auto & [col, val]: row.data()) {
                // 列名在每一行都重复存一份（Row 内部是 map<string, Value>），
                // 估算时也必须逐行计入，否则宽表的实际占用会被严重低估。
                bytes += col.size() + sizeof(std::string) + valueBytes(val);
            }
        }
        return bytes;
    }

    void QueryCache::eraseLocked(std::unordered_map<std::string, Entry>::iterator it) {
        totalBytes_ -= (it->second.bytes <= totalBytes_ ? it->second.bytes : totalBytes_);
        lru_.erase(it->second.lru);
        store_.erase(it);
    }

    void QueryCache::evictLocked(const std::size_t incomingBytes, const bool reserveSlot) {
        const auto maxEntries = cfg_.max_entries > 0
            ? static_cast<std::size_t>(cfg_.max_entries) : 0;
        const auto maxBytes = cfg_.max_memory_bytes > 0
            ? static_cast<std::size_t>(cfg_.max_memory_bytes) : 0;

        while (!lru_.empty()) {
            const bool tooMany = maxEntries > 0 &&
                (reserveSlot ? store_.size() >= maxEntries : store_.size() > maxEntries);
            const bool tooBig = maxBytes > 0 && totalBytes_ + incomingBytes > maxBytes;
            if (!tooMany && !tooBig) return;
            const std::string old = lru_.back();
            auto oit = store_.find(old);
            if (oit == store_.end()) {
                // 迭代器与 store_ 不一致（理论上不会发生），清掉尾部继续。
                lru_.pop_back();
                continue;
            }
            eraseLocked(oit);
            evictions_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void QueryCache::configure(const config::QueryCacheConfig &cfg) {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            cfg_ = cfg;
            // 配置变更时清空旧数据，避免新旧 TTL/上限混用导致行为不一致。
            store_.clear();
            lru_.clear();
            totalBytes_ = 0;
        }
        // ttl<=0 等于关闭（put 会一律拒收），这里把它直接反映成 enabled=false，
        // 免得调用方看到 enabled()==true 却发现一条都缓存不进去。
        enabled_.store(cfg.enabled && cfg.ttl_ms > 0, std::memory_order_release);
        replicaOnly_.store(cfg.cache_on_replica_only, std::memory_order_release);
    }

    bool QueryCache::enabled() {
        return enabled_.load(std::memory_order_acquire);
    }

    bool QueryCache::replicaOnly() {
        return replicaOnly_.load(std::memory_order_acquire);
    }

    bool QueryCache::get(const std::string &dataSource, const std::string &key,
                         common::ResultSet &out) {
        // 无锁快速失败：关闭时不进临界区，热路径上一把全局锁都不抢。
        if (!enabled_.load(std::memory_order_acquire)) return false;
        std::lock_guard<std::mutex> lk(mtx_);
        if (!cfg_.enabled) return false;
        const std::string ck = compositeKey(dataSource, key);
        const auto it = store_.find(ck);
        if (it == store_.end()) {
            misses_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        if (it->second.expire <= std::chrono::steady_clock::now()) {
            eraseLocked(it);
            misses_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        out = it->second.rs; // ResultSet 为值类型，赋值即深拷贝
        lru_.splice(lru_.begin(), lru_, it->second.lru); // 移到表头（最近使用）
        hits_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    void QueryCache::put(const std::string &dataSource, const std::string &key,
                         const common::ResultSet &rs) {
        // 先看开关再算字节：approxBytes 要遍历整个结果集，
        // 缓存关着的时候连这一趟遍历都不该发生。
        if (!enabled_.load(std::memory_order_acquire)) return;
        const std::size_t bytes = approxBytes(rs);

        std::lock_guard<std::mutex> lk(mtx_);
        if (!cfg_.enabled) return;
        // ttl_ms <= 0 一律不缓存。
        //
        // 按字面写进去的话 expire == now，get() 立刻判过期，结果是"每次都占内存
        // 存一份、每次都读不到"——比关掉缓存还差。宁可在这里直接不收。
        if (cfg_.ttl_ms <= 0) return;
        // 单个结果集就超过内存上限时直接不缓存，否则会把整个缓存清空只为放它一个。
        if (cfg_.max_memory_bytes > 0 &&
            bytes > static_cast<std::size_t>(cfg_.max_memory_bytes))
            return;

        const std::string ck = compositeKey(dataSource, key);
        const auto now = std::chrono::steady_clock::now();
        const auto ttl = std::chrono::milliseconds(cfg_.ttl_ms);

        if (const auto it = store_.find(ck); it != store_.end()) {
            totalBytes_ -= (it->second.bytes <= totalBytes_ ? it->second.bytes : totalBytes_);
            it->second.rs = rs;
            it->second.bytes = bytes;
            it->second.expire = now + ttl;
            totalBytes_ += bytes;
            lru_.splice(lru_.begin(), lru_, it->second.lru);
            evictLocked(0, false);
            return;
        }

        evictLocked(bytes, true);
        Entry e;
        e.rs = rs;
        e.bytes = bytes;
        e.expire = now + ttl;
        lru_.push_front(ck);
        e.lru = lru_.begin();
        totalBytes_ += bytes;
        store_.emplace(ck, std::move(e));
    }

    void QueryCache::invalidate(const std::string &dataSource) {
        // markWrite() 每次写都会调好几次（组名 + 主 + 各副本 + 各候选）。
        // 缓存关着时直接返回，别让默认关闭的功能在写路径上抢全局锁。
        if (!enabled_.load(std::memory_order_acquire)) return;
        std::uint64_t removed = 0;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (store_.empty()) return;
            const std::string prefix = compositeKey(dataSource, std::string{});
            for (auto it = store_.begin(); it != store_.end();) {
                if (it->first.size() >= prefix.size() &&
                    it->first.compare(0, prefix.size(), prefix) == 0) {
                    totalBytes_ -= (it->second.bytes <= totalBytes_
                                        ? it->second.bytes : totalBytes_);
                    lru_.erase(it->second.lru);
                    it = store_.erase(it);
                    ++removed;
                } else {
                    ++it;
                }
            }
        }
        if (removed > 0) invalidations_.fetch_add(removed, std::memory_order_relaxed);
    }

    QueryCache::Stats QueryCache::stats() {
        Stats out;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            out.entries = store_.size();
            out.approxBytes = totalBytes_;
        }
        out.hits = hits_.load(std::memory_order_relaxed);
        out.misses = misses_.load(std::memory_order_relaxed);
        out.evictions = evictions_.load(std::memory_order_relaxed);
        out.invalidations = invalidations_.load(std::memory_order_relaxed);
        return out;
    }
} // namespace dbmw::core
