#ifndef DBMW_CONFIG_DATASOURCE_CONFIG_H
#define DBMW_CONFIG_DATASOURCE_CONFIG_H

#include <cstdint>
#include <string>
#include <vector>
#include <map>


namespace dbmw::config {
    // 单个数据源的连接配置（对应 datasources.json 中 datasources[] 的一项）。
    struct DataSourceConfig {
        std::string name; // 数据源名称（唯一，用于分发）
        std::string type; // 数据库类型：mysql / postgres / odbc / 自定义
        std::string host; // 主机（odbc 可省略，改用 dsn）
        int port = 0; // 端口
        std::string user; // 用户名
        std::string password; // 密码
        std::string password_env; // 从环境变量读取密码，避免明文写入 JSON
        std::string database; // 默认库/模式
        std::string dsn; // ODBC 数据源名
        int connection_timeout_ms = 5000;
        int socket_timeout_ms = 0; // 0 = 不设置
        int query_timeout_ms = 0; // 0 = 使用驱动/服务端默认值
        // 单次查询允许物化的最大行数；0 表示不限制。
        //
        // query() 会把整个结果集读进内存，一条漏写 LIMIT 的查询就足以吃爆进程。
        // 超限时直接返回错误并提示改用 queryEach 流式消费。
        // 放在数据源级别，便于给报表库、分析库分别设不同的上限。
        int max_result_rows = 0;
        bool tls_enabled = false;
        bool tls_verify_peer = true;
        std::string tls_ca;
        std::string tls_cert;
        std::string tls_key;
        std::map<std::string, std::string> extra; // 驱动自定义扩展参数

        // 用于日志：隐藏密码。
        [[nodiscard]] std::string describe() const;

        // 清理驱动错误中可能回显的密码/完整连接串。
        [[nodiscard]] std::string redact(std::string text) const;
    };

    // 连接池参数。
    struct PoolConfig {
        // 是否启用池化。
        //
        // 关闭后每次操作都新建一条物理连接、用完立即关闭，不再复用、不再预热，
        // 也不再受 min/max 约束。适合低频定时任务、短命进程，或数据库侧对
        // 长连接有严格限制的场景。开启时行为与原来完全一致。
        bool enabled = true;
        int min = 1;
        int max = 8;
        // 借出连接时最多等待多久。池满且无人归还，超过该时长返回 PoolExhausted，
        // 而不是无限阻塞。0 表示不等待，立即失败。
        int borrow_timeout_ms = 30000;
        // 空闲连接超过该时长后由心跳回收；0 表示不主动回收。
        int idle_timeout_ms = 600000;
        // 物理连接的最长寿命；到期后在归还/心跳时轮换。0 表示不限。
        int max_lifetime_ms = 1800000;
        // 连接借出超过该时长时记录泄漏告警。0 表示关闭检测。
        int leak_detection_threshold_ms = 30000;
        // 借出连接时的存活校验最小间隔：距上次校验不足该时长就跳过 ping。
        //
        // 借出前 ping 一次要付一个完整网络往返，而大多数连接刚被用过，几乎
        // 不可能在毫秒级内失效。设成 0 表示每次借出都校验（更保守，原行为）。
        // 注意心跳线程仍会按 heartbeat_interval_ms 独立体检空闲连接，
        // 两者叠加能保证失效连接最终一定会被发现并剔除。
        int validation_interval_ms = 500;
    };

    struct RetryConfig {
        int max_attempts = 1;
        int initial_backoff_ms = 50;
        int max_backoff_ms = 1000;
        bool retry_writes = false;
    };

    struct CircuitBreakerConfig {
        int failure_threshold = 0;
        int open_interval_ms = 30000;
    };

    struct SqlLogConfig {
        bool enabled = false;
        // template: 仅参数化模板；full: 按驱动方言渲染绑定参数。
        std::string mode = "template";
        std::string level = "debug";
        bool log_success = true;
        bool log_errors = true;
        bool slow_only = false;
        double sample_rate = 1.0;
        int max_sql_length = 8192;
        int max_param_length = 256;
        // 字符串和二进制通常包含敏感数据，必须显式开启。
        bool include_string_values = false;
        bool include_blob_values = false;
    };

    struct SlowSqlConfig {
        bool enabled = false;
        int threshold_ms = 500;
        int aggregate_capacity = 1000;
        int recent_capacity = 200;
        bool retain_rendered_sql = false;
        int max_sql_length = 4096;
        std::vector<int> histogram_buckets_ms{10, 50, 100, 200, 500, 1000, 3000, 10000};
    };

    struct PoolMetricsConfig {
        bool enabled = true;
    };

    // 定时统计报告：按固定周期把连接池统计与慢 SQL 统计写入日志文件。
    //
    // 指标只留在进程内存里等于没有可观测性——生产上要么能被采集，要么能事后追溯。
    // 所以这里用后台线程周期采样并追加落盘，而不是等业务代码主动来调 API 取。
    // 统计口径与 slowSqlStats()/allPoolStats() 完全一致，两者可互为补充。
    struct StatsReportConfig {
        bool enabled = false;
        // 采样间隔。低于 1000ms 会被抬到 1000ms，避免高频写文件反过来拖慢业务。
        int interval_ms = 60000;
        // 日志文件路径；为空则只走 logger，不落文件。父目录不存在时自动创建。
        std::string file;
        // text：多行可读，适合人看；json：每次一行一个对象，便于采集器摄入。
        std::string format = "text";
        bool include_pool = true;
        bool include_slow_sql = true;
        // 每次报告最多列出多少条慢 SQL（按平均耗时倒序）。
        int slow_sql_limit = 10;
    };

    // 限流与背压：按数据源总 QPS 与（可选）单 SQL 指纹 QPS 做令牌桶限速。
    //
    // 限流是"快速失败"——超限返回 RateLimited（非重试），让调用方本地排队或降级，
    // 而不是把连接池打满后雪崩。所有阈值 <=0 表示该项不限制。
    struct RateLimitConfig {
        bool enabled = false;
        // 每数据源总 QPS 上限（令牌桶 refill 速率）。0 = 不限制。
        int global_qps = 0;
        // 单 SQL 指纹 QPS 上限（保护热点语句）。0 = 不限制。
        int per_fingerprint_qps = 0;
        // 突发容量；0 = 等于对应 qps（即桶容量 == 速率，瞬时仅允许 1 秒量）。
        int burst = 0;
        // 指纹模式：off（不按指纹限流）/ template（结构化模板）/ full（模板+参数）。
        std::string fingerprint_mode = "off";
    };

    // SQL 审计与拦截：在执行前对 SQL 做轻量静态分析并施加策略。
    //
    // 启发式分类（非完整解析器）可能误判动态 SQL/存储过程，因此提供
    // action="warn" 灰度：只告警不拦截。拦截返回 SqlBlocked。
    struct SqlAuditConfig {
        bool enabled = false;
        // block：直接拦截并返回 SqlBlocked；warn：仅记录告警、放行。
        std::string action = "warn";
        // UPDATE/DELETE 无 WHERE 时拦截（防止全表误改/误删）。
        bool block_no_where_dml = false;
        // SELECT 无 LIMIT 时拦截（防止一次性拉全表）。
        bool require_limit_select = false;
        // 该数据源为只读时拦截一切写（Insert/Update/Delete/DDL）。
        // 由所属 group 的 read_only 标志驱动，单数据源默认不开启。
        bool enforce_read_only = false;
        // 被拦截/告警时记录一条审计日志。
        bool log_blocked = true;
        // 指纹黑名单：命中即拦截（与 action 无关）。指纹来自 structuralTemplate。
        std::vector<std::uint64_t> blacklist_fingerprints;
        // 指纹白名单：非空时"仅放行名单内"，其余一律拦截（允许列表模式）。
        std::vector<std::uint64_t> whitelist_fingerprints;
    };

    // 查询结果缓存：仅缓存非事务、非会话的读（query 路径）。
    //
    // 默认关闭；开启即视为接受"最终一致 + TTL"。强一致读可用
    // cache_on_replica_only 让主库读不缓存。
    struct QueryCacheConfig {
        bool enabled = false;
        int ttl_ms = 60000;
        int max_entries = 1000;
        // 近似内存上限（字节），0 = 仅按条目数限制。
        int max_memory_bytes = 0;
        // true 时仅缓存打到副本的读，主库强一致读不缓存。
        bool cache_on_replica_only = false;
    };

    // 写缓冲（故障转移备援）：主与所有候选都不可用时，写请求入有界内存队列，
    // 后台 flush 线程在主恢复后补发。这是"软降级"——入队即返回 Buffered，
    // 不代表已提交；进程崩溃会丢数据，且绝不用于事务。
    struct WriteBufferConfig {
        bool enabled = false;
        int max_queue = 1000;
        int ttl_ms = 30000;          // 入队后最多保留时长，超时丢弃
        int flush_interval_ms = 1000; // 后台 flush 周期
    };

    // 主库故障转移：写路径在主不可用时切换到有序可写候选。
    struct FailoverConfig {
        // 有序可写候选（不含主；主自动置顶）。第一个健康者被选中。
        std::vector<std::string> primaries;
        // 仅按熔断状态判断"健康"（v1）。真正的复制滞后需由外部健康信号提供。
        bool require_healthy = false;
        WriteBufferConfig write_buffer;
    };

    struct ObservabilityConfig {
        SqlLogConfig sql_log;
        SlowSqlConfig slow_sql;
        PoolMetricsConfig pool_metrics;
        StatsReportConfig stats_report;
    };

    // 游标（服务端游标 / 流式结果集）能力开关与上限。
    //
    // 游标把连接从“借→用→还”变成“借→钉住→取 N 次→显式关→还”，
    // 占用的是池连接数，因此必须能限制并发上限（max_open_cursors），
    // 否则一个忘关的游标能拖垮整个池。其余三项都是“软”开关：
    //   - enabled：该功能总开关，关掉后所有 openCursor 返回 NotSupported；
    //   - default_batch_size：调用方未显式指定 batch_size 时的兜底（覆盖驱动默认）；
    //   - allow_scrollable：是否允许滚动游标（仅 ODBC 真支持，其余驱动忽略该选项）。
    struct CursorConfig {
        bool enabled = true;                 // 是否允许开游标
        int default_batch_size = 256;        // 默认每批取行数（覆盖驱动默认）
        int max_open_cursors = 0;            // 每数据源并发游标上限；0 = 不限制
        bool allow_scrollable = false;       // 是否允许滚动游标（仅 ODBC 生效）
    };

    // 预编译语句缓存：连接级句柄复用（prepare-once / execute-many）。
    //
    // 只影响"同一条物理连接上重复出现的同型 SQL"：热点语句不必每次都付一次
    // 服务端硬解析 + 参数类型推导的往返。它不改变任何查询结果，是纯性能优化，
    // 因此默认开启；关掉则退化为既有的"每次重新绑定执行"路径。
    //
    // 注意它与 query_cache 是两套**互相独立**的缓存，不要混用：
    //   - query_cache    缓存的是**结果数据**，键是 (SQL + 参数值)；
    //   - prepared_cache 缓存的是**语句句柄**，键是 (SQL + 参数类型签名)。
    struct PreparedCacheConfig {
        // 是否启用透明自动缓存。
        //
        // DataSource::query / execute 在驱动支持（supportsPrepared()）时，
        // 自动按 (归一化 SQL, 参数类型签名) 在本连接上复用句柄，调用方无感。
        bool enabled = true;
        // 每条连接保留的预编译句柄上限；0 = 不限制（随连接关闭自然回收）。
        // 超过时按 LRU 驱逐（PG DEALLOCATE / MySQL mysql_stmt_close / ODBC SQLFreeHandle）。
        int max_per_connection = 0;
    };

    struct ReplicaConfig {
        std::string name;
        int weight = 1;
    };

    // 异步执行器（v0.2.0 异步 API 的运行时底座）。
    //
    // 执行器承载所有驱动的阻塞 IO：调用线程提交异步操作后立即返回，
    // "借连接 + 执行语句"全部发生在执行器线程上。
    // 线程惰性启动——进程从未使用异步 API 就不会多出任何线程。
    struct AsyncConfig {
        bool enabled = true;
        // 执行器 worker 线程数；0 = hardware_concurrency。
        int threads = 0;
        // 立即任务队列上限；满时新操作以 Overloaded 快速失败（显式背压）。
        int queue_size = 4096;
        // 全局默认语句期限（毫秒）；0 = 不限。可被每次调用的 Options::timeout 覆盖。
        int statement_timeout_ms = 0;
    };

    struct DataSourceGroupConfig {
        std::string name;
        std::string primary;
        std::vector<ReplicaConfig> replicas;
        int read_after_write_ms = 0;
        bool fallback_to_primary = true;
        // 该 group 是否只读：开启后拦截一切写（配合 SqlAuditConfig.enforce_read_only）。
        bool read_only = false;
        FailoverConfig failover;
    };

    // 全局配置（对应 datasources.json 根）。
    struct GlobalConfig {
        std::string default_datasource;
        int heartbeat_interval_ms = 5000;
        PoolConfig pool;
        RetryConfig retry;
        CircuitBreakerConfig circuit_breaker;
        ObservabilityConfig observability;
        RateLimitConfig rate_limit;
        SqlAuditConfig sql_audit;
        QueryCacheConfig query_cache;
        CursorConfig cursor;                 // 游标能力开关与上限
        PreparedCacheConfig prepared_cache;  // 预编译语句复用（连接级句柄缓存）
        AsyncConfig async;                   // 异步执行器（v0.2.0）
        std::vector<DataSourceConfig> datasources;
        std::vector<DataSourceGroupConfig> groups;
    };
} // namespace dbmw::config


#endif // DBMW_CONFIG_DATASOURCE_CONFIG_H
