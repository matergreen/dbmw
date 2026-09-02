#include "dbmw/common/observer.h"
#include "dbmw/common/logger.h"

#include <algorithm>
#include <atomic>
#include <deque>
#include <functional>
#include <list>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>

namespace dbmw::common {
    namespace {
        // 观测状态按访问频率拆成两把锁，避免慢路径拖累快路径：
        //   - g_stateMutex：保护"配置 + 观察者句柄"，只在 configure/setObserver 时写；
        //   - g_statsMutex：保护慢 SQL 聚合，只在判定为慢 SQL 时写。
        // 而每条 SQL 都要走的高频读取通过线程本地快照完全绕开这两把锁。
        std::mutex g_stateMutex;
        std::mutex g_statsMutex;
        OperationObserver g_observer;
        config::ObservabilityConfig g_config;
        // 配置或观察者每次变更时递增；线程据此判断本地快照是否已过期。
        std::atomic<std::uint64_t> g_stateVersion{1};
        std::unordered_map<std::uint64_t, SlowSqlStats> g_slowStats;
        std::deque<SlowSqlRecord> g_recentSlow;
        // 慢 SQL 聚合的 LRU 顺序（最近使用的在头部）与定位表，淘汰为 O(1)。
        std::list<std::uint64_t> g_lru;
        std::unordered_map<std::uint64_t, std::list<std::uint64_t>::iterator> g_lruPos;

        // 配置与观察者句柄的只读快照。
        struct Snapshot {
            config::ObservabilityConfig config;
            OperationObserver observer;
        };

        // 每线程缓存一份快照，只在版本号变化时才回退加锁刷新。
        //
        // 这是本文件最关键的热路径优化：观测埋点每条 SQL 都会走到，若在这里抢
        // 一把进程级全局 mutex，多核下所有数据源的每次查询都会互相排队，核数
        // 越多劣化越严重。稳态下只剩一次原子读比较，无锁、也不产生写共享。
        const Snapshot &currentSnapshot() {
            static thread_local std::uint64_t tlsVersion = 0;
            static thread_local Snapshot tlsSnapshot;
            const auto version = g_stateVersion.load(std::memory_order_acquire);
            if (tlsVersion == version) return tlsSnapshot;
            std::lock_guard<std::mutex> lock(g_stateMutex);
            tlsSnapshot.config = g_config;
            tlsSnapshot.observer = g_observer;
            tlsVersion = g_stateVersion.load(std::memory_order_acquire);
            return tlsSnapshot;
        }

        // 采样序列：原来用全局原子，SQL 日志开启时每条语句递增一次，是多核
        // 写共享点。改为线程本地并按线程错开起点，既消除争用又保持采样均匀。
        std::uint64_t nextSampleSequence() {
            static thread_local std::uint64_t tlsSequence =
                std::hash<std::thread::id>{}(std::this_thread::get_id());
            return ++tlsSequence;
        }

        // 把指纹标记为最近使用（移动到 LRU 头部）；O(1)。调用方须持 g_statsMutex。
        void touchLru(std::uint64_t fp) {
            auto it = g_lruPos.find(fp);
            if (it == g_lruPos.end()) {
                g_lru.push_front(fp);
                g_lruPos.emplace(fp, g_lru.begin());
            } else {
                g_lru.splice(g_lru.begin(), g_lru, it->second);
            }
        }

        // 淘汰最近最少使用的聚合项；O(1)。调用方须持 g_statsMutex。
        void evictLru() {
            if (g_lru.empty()) return;
            const auto fp = g_lru.back();
            g_lru.pop_back();
            g_lruPos.erase(fp);
            g_slowStats.erase(fp);
        }

        std::string truncate(std::string text, const std::size_t limit) {
            if (text.size() <= limit) return text;
            const auto original = text.size();
            // 若截断点落在单引号字符串字面量内部（如 'hello wo...[truncated]'），
            // 先补一个右引号再贴标记，避免把诊断标记塞进字符串里造成畸形输出（P2）。
            bool inLiteral = false;
            bool escaped = false;
            for (std::size_t i = 0; i < limit; ++i) {
                const char c = text[i];
                if (escaped) { escaped = false; continue; }
                if (c == '\\') { escaped = true; continue; }
                if (c == '\'') {
                    if (i + 1 < limit && text[i + 1] == '\'') { ++i; continue; } // '' 转义
                    inLiteral = !inLiteral;
                }
            }
            std::string head = text.substr(0, limit);
            if (inLiteral) head += '\'';
            head += "...[truncated, original_length=" + std::to_string(original) + "]";
            return head;
        }

        std::uint64_t fingerprint(const std::string &dataSource,
                                  const OperationType type,
                                  const std::string &sql) {
            // 稳定的 64 位 FNV-1a；仅用于进程内聚合，不用于安全边界。
            std::uint64_t hash = 1469598103934665603ULL;
            auto add = [&](const unsigned char c) {
                hash ^= c;
                hash *= 1099511628211ULL;
            };
            for (const unsigned char c: dataSource) add(c);
            add(0);
            add(static_cast<unsigned char>(type));
            for (const unsigned char c: sql) add(c);
            return hash;
        }

        // 生成慢 SQL 聚合使用的结构模板：折叠字符串/数值字面量、压缩结构空白、
        // 去掉普通注释，同时保留标识符、优化器 Hint 和 PostgreSQL dollar-quoted
        // 代码块。此结果同时作为事件、日志和慢 SQL 查询 API 的唯一指纹来源。
        std::string structuralSql(const std::string &sql) {
            std::string out;
            out.reserve(sql.size());
            const std::size_t n = sql.size();
            const auto isIdent = [](unsigned char c) {
                return std::isalnum(c) || c == '_';
            };
            bool pendingSpace = false;
            const auto flushSpace = [&] {
                if (pendingSpace && !out.empty() && out.back() != ' ') out.push_back(' ');
                pendingSpace = false;
            };
            std::size_t i = 0;
            while (i < n) {
                const auto c = static_cast<unsigned char>(sql[i]);

                if (std::isspace(c)) {
                    pendingSpace = !out.empty();
                    ++i;
                    continue;
                }

                // 普通注释不影响 SQL 结构；MySQL 版本注释和优化器 Hint 会影响执行，保留。
                if (c == '-' && i + 1 < n && sql[i + 1] == '-') {
                    while (i < n && sql[i] != '\n') ++i;
                    pendingSpace = !out.empty();
                    continue;
                }
                if (c == '/' && i + 1 < n && sql[i + 1] == '*') {
                    const bool semantic = i + 2 < n &&
                        (sql[i + 2] == '!' || sql[i + 2] == '+');
                    const auto close = sql.find("*/", i + 2);
                    const auto end = close == std::string::npos ? n : close + 2;
                    if (semantic) {
                        flushSpace();
                        out.append(sql, i, end - i);
                    } else {
                        pendingSpace = !out.empty();
                    }
                    i = end;
                    continue;
                }

                // PostgreSQL $$...$$ / $tag$...$tag$：常用于函数体和 DO 块。
                // 内容影响语义，不能全部折叠成同一个占位符；但模板也不能保存原文，
                // 因此用稳定内容哈希区分不同代码块，避免泄漏块内字符串。
                if (c == '$') {
                    std::size_t tagEnd = i + 1;
                    while (tagEnd < n &&
                           (std::isalnum(static_cast<unsigned char>(sql[tagEnd])) ||
                            sql[tagEnd] == '_')) ++tagEnd;
                    const bool validTag = tagEnd < n && sql[tagEnd] == '$' &&
                        (tagEnd == i + 1 ||
                         !std::isdigit(static_cast<unsigned char>(sql[i + 1])));
                    if (validTag) {
                        const std::string delimiter = sql.substr(i, tagEnd - i + 1);
                        const auto close = sql.find(delimiter, tagEnd + 1);
                        if (close != std::string::npos) {
                            flushSpace();
                            const auto end = close + delimiter.size();
                            std::uint64_t bodyHash = 1469598103934665603ULL;
                            for (auto p = tagEnd + 1; p < close; ++p) {
                                bodyHash ^= static_cast<unsigned char>(sql[p]);
                                bodyHash *= 1099511628211ULL;
                            }
                            out += delimiter + "<body_hash:" +
                                   std::to_string(bodyHash) + ">" + delimiter;
                            i = end;
                            continue;
                        }
                    }
                }

                if (c == '\'') { // 字符串字面量 -> ?
                    flushSpace();
                    out += '?';
                    ++i;
                    while (i < n) {
                        if (sql[i] == '\\' && i + 1 < n) {
                            i += 2;
                            continue;
                        }
                        if (sql[i] == '\'') {
                            if (i + 1 < n && sql[i + 1] == '\'') { i += 2; continue; }
                            ++i;
                            break;
                        }
                        ++i;
                    }
                    continue;
                }

                // 双引号标识符和 MySQL 反引号标识符原样保留。
                if (c == '"' || c == '`') {
                    flushSpace();
                    const char quote = static_cast<char>(c);
                    out.push_back(quote);
                    ++i;
                    while (i < n) {
                        out.push_back(sql[i]);
                        if (sql[i] == '\\' && i + 1 < n) {
                            out.push_back(sql[++i]);
                        } else if (sql[i] == quote) {
                            if (i + 1 < n && sql[i + 1] == quote)
                                out.push_back(sql[++i]);
                            else {
                                ++i;
                                break;
                            }
                        }
                        ++i;
                    }
                    continue;
                }

                const bool boundary = i == 0 ||
                    !isIdent(static_cast<unsigned char>(sql[i - 1]));
                const bool signedNumber = (c == '+' || c == '-') && i + 1 < n &&
                    (std::isdigit(static_cast<unsigned char>(sql[i + 1])) ||
                     (sql[i + 1] == '.' && i + 2 < n &&
                      std::isdigit(static_cast<unsigned char>(sql[i + 2]))));
                const bool plainNumber = std::isdigit(c) ||
                    (c == '.' && i + 1 < n &&
                     std::isdigit(static_cast<unsigned char>(sql[i + 1])));
                if (boundary && (plainNumber || signedNumber)) {
                    flushSpace();
                    out += '?';
                    if (signedNumber) ++i;
                    if (i + 1 < n && sql[i] == '0' &&
                        (sql[i + 1] == 'x' || sql[i + 1] == 'X')) {
                        i += 2;
                        while (i < n && (std::isxdigit(static_cast<unsigned char>(sql[i])) ||
                                         sql[i] == '_')) ++i;
                        continue;
                    }
                    if (i + 1 < n && sql[i] == '0' &&
                        (sql[i + 1] == 'b' || sql[i + 1] == 'B')) {
                        i += 2;
                        while (i < n && (sql[i] == '0' || sql[i] == '1' || sql[i] == '_')) ++i;
                        continue;
                    }
                    while (i < n && (std::isdigit(static_cast<unsigned char>(sql[i])) ||
                                     sql[i] == '_')) ++i;
                    if (i < n && sql[i] == '.') {
                        ++i;
                        while (i < n && (std::isdigit(static_cast<unsigned char>(sql[i])) ||
                                         sql[i] == '_')) ++i;
                    }
                    if (i < n && (sql[i] == 'e' || sql[i] == 'E')) {
                        std::size_t exponent = i + 1;
                        if (exponent < n && (sql[exponent] == '+' || sql[exponent] == '-'))
                            ++exponent;
                        const auto digits = exponent;
                        while (exponent < n &&
                               (std::isdigit(static_cast<unsigned char>(sql[exponent])) ||
                                sql[exponent] == '_')) ++exponent;
                        if (exponent > digits) i = exponent;
                    }
                    continue;
                }

                flushSpace();
                out += static_cast<char>(c);
                ++i;
            }
            return out;
        }

        LogLevel parseLevel(const std::string &level) {
            if (level == "info") return LogLevel::Info;
            if (level == "warn") return LogLevel::Warn;
            if (level == "error") return LogLevel::Error;
            return LogLevel::Debug;
        }

        const char *operationName(const OperationType type) {
            switch (type) {
                case OperationType::Query: return "query";
                case OperationType::Execute: return "execute";
                case OperationType::Stream: return "stream";
                case OperationType::Batch: return "batch";
                case OperationType::Select: return "select";
                default: return "operation";
            }
        }
    }

    void Observability::setObserver(OperationObserver observer) {
        {
            std::lock_guard<std::mutex> lock(g_stateMutex);
            g_observer = std::move(observer);
        }
        // 版本号在锁外递增即可：需要刷新的线程会重新加锁读取，
        // 这里只需保证"变更后版本号一定比之前大"。
        g_stateVersion.fetch_add(1, std::memory_order_release);
    }

    void Observability::emit(const OperationEvent &event) noexcept {
        // noexcept 边界：快照里的 std::function 拷贝也可能抛，必须全部兜住。
        try {
            const Snapshot &snapshot = currentSnapshot();
            if (!snapshot.observer) return;
            try {
                snapshot.observer(event);
            } catch (...) {
                // 观测系统不得改变数据库操作的返回语义。
            }
        } catch (...) {
        }
    }

    void Observability::configure(const config::ObservabilityConfig &config) {
        config::ObservabilityConfig normalized = config;
        if (normalized.slow_sql.aggregate_capacity < 1)
            normalized.slow_sql.aggregate_capacity = 1;
        if (normalized.slow_sql.recent_capacity < 0)
            normalized.slow_sql.recent_capacity = 0;
        const auto capacity =
            static_cast<std::size_t>(normalized.slow_sql.aggregate_capacity);
        const auto recentCapacity =
            static_cast<std::size_t>(normalized.slow_sql.recent_capacity);
        const auto buckets = normalized.slow_sql.histogram_buckets_ms;

        {
            std::lock_guard<std::mutex> lock(g_stateMutex);
            g_config = std::move(normalized);
        }
        g_stateVersion.fetch_add(1, std::memory_order_release);

        // 统计用的是另一把锁，且两段加锁不嵌套：配置变更不该和埋点路径
        // 长时间互堵，也不会构成死锁条件。
        std::lock_guard<std::mutex> lock(g_statsMutex);

        // 旧样本无法无损重分桶。分桶变化时必须重置整组累计统计，否则 count、
        // totalDuration 与 histogram 会分别代表不同时间范围，百分位结果必然失真。
        const bool bucketsChanged = std::any_of(
            g_slowStats.begin(), g_slowStats.end(), [&](const auto &entry) {
                return entry.second.histogramBucketsMs != buckets;
            });
        if (bucketsChanged) {
            g_slowStats.clear();
            g_lru.clear();
            g_lruPos.clear();
        }

        // 容量缩小时按 LRU 裁剪；最近明细与聚合统计生命周期彼此独立。
        while (g_slowStats.size() > capacity) evictLru();
        while (g_recentSlow.size() > recentCapacity) g_recentSlow.pop_front();
    }

    void Observability::emitSql(OperationEvent event, const std::string &sql,
                                const SqlRenderer &renderer) noexcept {
        try {
            // 热路径：配置与观察者都取自线程本地快照，完全不碰全局锁。
            // 每条 SQL 都会经过这里，这是本文件最值得优化的地方。
            const Snapshot &snapshot = currentSnapshot();
            const config::ObservabilityConfig &config = snapshot.config;
            const OperationObserver &observer = snapshot.observer;

            const bool slowEnabled = config.slow_sql.enabled;
            const bool logEnabled = config.sql_log.enabled;
            // P1-1：观测全关（无观察者、慢 SQL 与 SQL 日志都关）时，
            // 直接返回，省掉结构化扫描和 fingerprint 的 O(n) 开销。
            // 只要注册了观察者，仍照常回调（不影响数据库语义）。
            if (!observer && !slowEnabled && !logEnabled) return;

            // 事件、日志与慢 SQL API 共用同一结构模板和同一指纹，保证日志中的
            // fingerprint 可以直接反查 slowSqlStats/recentSlowSql。
            const std::string structure = (slowEnabled || logEnabled) ? structuralSql(sql)
                                                                      : std::string();
            if (slowEnabled || logEnabled)
                event.sqlFingerprint = fingerprint(event.dataSource, event.type, structure);

            const auto durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                event.duration).count();
            event.slow = slowEnabled && durationMs >= config.slow_sql.threshold_ms;

            const bool logEligible = logEnabled &&
                (!config.sql_log.slow_only || durationMs >= config.slow_sql.threshold_ms) &&
                ((event.status.ok() && config.sql_log.log_success) ||
                 (!event.status.ok() && config.sql_log.log_errors));
            const auto sequence = logEnabled ? nextSampleSequence() : 0;
            // 采样只在 SQL 日志开启时才有意义；关闭时直接视为全采样。
            const bool sampled = !logEnabled ||
                config.sql_log.sample_rate >= 1.0 ||
                (config.sql_log.sample_rate > 0.0 &&
                 static_cast<double>((event.sqlFingerprint ^ sequence) % 1000000ULL) /
                     1000000.0 < config.sql_log.sample_rate);
            const bool needsRendered = renderer &&
                ((logEligible && sampled && config.sql_log.mode == "full") ||
                 (event.slow && config.slow_sql.retain_rendered_sql));

            if (event.slow || (logEligible && sampled)) {
                event.sqlTemplate = truncate(sql, static_cast<std::size_t>(
                    event.slow ? config.slow_sql.max_sql_length
                               : config.sql_log.max_sql_length));
            }
            if (needsRendered) {
                SqlRenderOptions options;
                options.includeStringValues = config.sql_log.include_string_values;
                options.includeBlobValues = config.sql_log.include_blob_values;
                options.maxParamLength = static_cast<std::size_t>(
                    config.sql_log.max_param_length);
                options.maxSqlLength = static_cast<std::size_t>(
                    std::max(config.sql_log.max_sql_length,
                             config.slow_sql.max_sql_length));
                std::string rendered;
                if (renderer(options, rendered).ok()) event.renderedSql = std::move(rendered);
            }

            if (event.slow) {
                const auto now = std::chrono::system_clock::now();
                // 字符串拼接型 SQL 也按结构模板聚合；淘汰走 O(1) LRU。
                const auto aggKey = event.sqlFingerprint;
                // 只有被判定为慢 SQL 才会走到这里，属低频路径，用统计专用锁，
                // 不与高频的快照读取相互阻塞。
                std::lock_guard<std::mutex> lock(g_statsMutex);
                const bool isNew = g_slowStats.find(aggKey) == g_slowStats.end();
                if (isNew) {
                    if (g_slowStats.size() >= static_cast<std::size_t>(
                            config.slow_sql.aggregate_capacity))
                        evictLru();
                    touchLru(aggKey);
                } else {
                    touchLru(aggKey);
                }
                auto &stats = g_slowStats[aggKey];
                if (isNew) {
                    stats.fingerprint = aggKey;
                    stats.dataSource = event.dataSource;
                    stats.type = event.type;
                    stats.sqlTemplate = truncate(structure, static_cast<std::size_t>(
                        config.slow_sql.max_sql_length));
                    stats.firstSeen = now;
                    stats.minDuration = event.duration;
                    stats.histogramBucketsMs = config.slow_sql.histogram_buckets_ms;
                    stats.histogram.assign(stats.histogramBucketsMs.size() + 1, 0);
                }
                ++stats.count;
                if (!event.status.ok()) ++stats.errorCount;
                if (event.status.code == ErrorCode::QueryTimeout) ++stats.timeoutCount;
                stats.totalDuration += event.duration;
                stats.minDuration = std::min(stats.minDuration, event.duration);
                stats.maxDuration = std::max(stats.maxDuration, event.duration);
                stats.lastSeen = now;
                std::size_t bucket = 0;
                while (bucket < stats.histogramBucketsMs.size() &&
                       event.duration.count() >
                           static_cast<std::int64_t>(stats.histogramBucketsMs[bucket]) * 1000)
                    ++bucket;
                ++stats.histogram[bucket];

                if (config.slow_sql.recent_capacity > 0) {
                    SlowSqlRecord record;
                    record.timestamp = now;
                    record.dataSource = event.dataSource;
                    record.type = event.type;
                    record.sqlTemplate = stats.sqlTemplate;
                    if (config.slow_sql.retain_rendered_sql)
                        record.renderedSql = truncate(event.renderedSql,
                            static_cast<std::size_t>(config.slow_sql.max_sql_length));
                    record.fingerprint = aggKey;
                    record.duration = event.duration;
                    record.errorCode = event.status.code;
                    record.sqlState = event.status.sqlState;
                    if (g_recentSlow.size() >= static_cast<std::size_t>(
                            config.slow_sql.recent_capacity))
                        g_recentSlow.pop_front();
                    g_recentSlow.push_back(std::move(record));
                }
            }

            if (logEligible && sampled) {
                const auto &displaySql = config.sql_log.mode == "full" &&
                                         !event.renderedSql.empty()
                    ? event.renderedSql : event.sqlTemplate;
                std::ostringstream message;
                message << "sql datasource=" << event.dataSource
                        << " operation=" << operationName(event.type)
                        << " duration_ms=" << durationMs
                        << " rows=" << event.rowCount
                        << " status=" << errorCodeToString(event.status.code)
                        << " fingerprint=" << event.sqlFingerprint
                        << " statement=" << displaySql;
                Logger::log(parseLevel(config.sql_log.level), message.str());
            }

            if (observer) {
                try { observer(event); } catch (...) {}
            }
        } catch (...) {
            // 诊断路径不得改变数据库操作结果。
        }
    }

    std::vector<SlowSqlStats> Observability::slowSqlStats(
        const std::size_t limit, const std::string &dataSource) {
        std::vector<SlowSqlStats> result;
        std::lock_guard<std::mutex> lock(g_statsMutex);
        result.reserve(g_slowStats.size());
        for (const auto &entry: g_slowStats) {
            if (dataSource.empty() || entry.second.dataSource == dataSource)
                result.push_back(entry.second);
        }
        // 按平均耗时（totalDuration / count）降序，更真实地暴露"慢"查询，
        // 避免被高频但单次并不慢的查询用累计耗时顶到榜首（P2-8）。
        std::sort(result.begin(), result.end(), [](const auto &a, const auto &b) {
            const std::uint64_t ac = a.count == 0 ? 1 : a.count;
            const std::uint64_t bc = b.count == 0 ? 1 : b.count;
            const auto averageA = static_cast<long double>(a.totalDuration.count()) /
                                  static_cast<long double>(ac);
            const auto averageB = static_cast<long double>(b.totalDuration.count()) /
                                  static_cast<long double>(bc);
            return averageA > averageB;
        });
        if (result.size() > limit) result.resize(limit);
        return result;
    }

    std::vector<SlowSqlRecord> Observability::recentSlowSql(
        const std::size_t limit, const std::string &dataSource) {
        std::vector<SlowSqlRecord> result;
        if (limit == 0) return result;
        std::lock_guard<std::mutex> lock(g_statsMutex);
        for (auto it = g_recentSlow.rbegin(); it != g_recentSlow.rend(); ++it) {
            if (!dataSource.empty() && it->dataSource != dataSource) continue;
            result.push_back(*it);
            if (result.size() >= limit) break;
        }
        return result;
    }

    void Observability::clearSlowSqlStats() {
        std::lock_guard<std::mutex> lock(g_statsMutex);
        g_slowStats.clear();
        g_lru.clear();
        g_lruPos.clear();
        g_recentSlow.clear();
    }
} // namespace dbmw::common
