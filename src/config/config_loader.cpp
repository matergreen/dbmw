#include "dbmw/config/config_loader.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <cstdlib>
#include <utility>


namespace dbmw::config {
    using json = nlohmann::json;

    bool ConfigLoader::loadFromFile(const std::string &path, GlobalConfig &out, std::string &error) {
        // 同一个输出对象可用于配置热加载，不保留上一版的数组或默认值。
        out = GlobalConfig{};
        error.clear();
        std::ifstream f(path);
        if (!f) {
            error = "cannot open config file: " + path;
            return false;
        }

        json j;
        try {
            f >> j;
        } catch (const json::exception &e) {
            error = std::string("json parse error: ") + e.what();
            return false;
        }

        try {
        out.default_datasource = j.value("default_datasource", std::string());
        out.heartbeat_interval_ms = j.value("heartbeat_interval_ms", 5000);

        if (j.contains("pool") && j["pool"].is_object()) {
            out.pool.enabled = j["pool"].value("enabled", out.pool.enabled);
            out.pool.min = j["pool"].value("min", out.pool.min);
            out.pool.max = j["pool"].value("max", out.pool.max);
            out.pool.borrow_timeout_ms = j["pool"].value("borrow_timeout_ms", out.pool.borrow_timeout_ms);
            out.pool.idle_timeout_ms = j["pool"].value("idle_timeout_ms", out.pool.idle_timeout_ms);
            out.pool.max_lifetime_ms = j["pool"].value("max_lifetime_ms", out.pool.max_lifetime_ms);
            out.pool.leak_detection_threshold_ms = j["pool"].value("leak_detection_threshold_ms", out.pool.leak_detection_threshold_ms);
            out.pool.validation_interval_ms = j["pool"].value("validation_interval_ms", out.pool.validation_interval_ms);
        }
        if (out.pool.min < 0)
            out.pool.min = 0;
        if (out.pool.max < 1)
            out.pool.max = 1;
        if (out.pool.min > out.pool.max)
            out.pool.min = out.pool.max;
        if (out.pool.borrow_timeout_ms < 0)
            out.pool.borrow_timeout_ms = 0;
        if (out.pool.idle_timeout_ms < 0)
            out.pool.idle_timeout_ms = 0;
        if (out.pool.max_lifetime_ms < 0)
            out.pool.max_lifetime_ms = 0;
        if (out.pool.leak_detection_threshold_ms < 0)
            out.pool.leak_detection_threshold_ms = 0;
        if (out.pool.validation_interval_ms < 0)
            out.pool.validation_interval_ms = 0;

        if (j.contains("retry")) {
            if (!j["retry"].is_object()) {
                error = "retry must be an object";
                return false;
            }
            const auto &retry = j["retry"];
            out.retry.max_attempts = retry.value("max_attempts", out.retry.max_attempts);
            out.retry.initial_backoff_ms = retry.value(
                "initial_backoff_ms", out.retry.initial_backoff_ms);
            out.retry.max_backoff_ms = retry.value("max_backoff_ms", out.retry.max_backoff_ms);
            out.retry.retry_writes = retry.value("retry_writes", out.retry.retry_writes);
            if (out.retry.max_attempts < 1 || out.retry.initial_backoff_ms < 0 ||
                out.retry.max_backoff_ms < out.retry.initial_backoff_ms) {
                error = "invalid retry configuration";
                return false;
            }
        }
        if (j.contains("circuit_breaker")) {
            if (!j["circuit_breaker"].is_object()) {
                error = "circuit_breaker must be an object";
                return false;
            }
            const auto &breaker = j["circuit_breaker"];
            out.circuit_breaker.failure_threshold = breaker.value(
                "failure_threshold", out.circuit_breaker.failure_threshold);
            out.circuit_breaker.open_interval_ms = breaker.value(
                "open_interval_ms", out.circuit_breaker.open_interval_ms);
            if (out.circuit_breaker.failure_threshold < 0 ||
                out.circuit_breaker.open_interval_ms < 1) {
                error = "invalid circuit_breaker configuration";
                return false;
            }
        }

        if (j.contains("observability")) {
            if (!j["observability"].is_object()) {
                error = "observability must be an object";
                return false;
            }
            const auto &observability = j["observability"];
            if (observability.contains("sql_log")) {
                if (!observability["sql_log"].is_object()) {
                    error = "observability.sql_log must be an object";
                    return false;
                }
                const auto &sqlLog = observability["sql_log"];
                auto &cfg = out.observability.sql_log;
                cfg.enabled = sqlLog.value("enabled", cfg.enabled);
                cfg.mode = sqlLog.value("mode", cfg.mode);
                cfg.level = sqlLog.value("level", cfg.level);
                cfg.log_success = sqlLog.value("log_success", cfg.log_success);
                cfg.log_errors = sqlLog.value("log_errors", cfg.log_errors);
                cfg.slow_only = sqlLog.value("slow_only", cfg.slow_only);
                cfg.sample_rate = sqlLog.value("sample_rate", cfg.sample_rate);
                cfg.max_sql_length = sqlLog.value("max_sql_length", cfg.max_sql_length);
                cfg.max_param_length = sqlLog.value("max_param_length", cfg.max_param_length);
                cfg.include_string_values = sqlLog.value(
                    "include_string_values", cfg.include_string_values);
                cfg.include_blob_values = sqlLog.value(
                    "include_blob_values", cfg.include_blob_values);
                if ((cfg.mode != "template" && cfg.mode != "full") ||
                    (cfg.level != "debug" && cfg.level != "info" &&
                     cfg.level != "warn" && cfg.level != "error") ||
                    cfg.sample_rate < 0.0 || cfg.sample_rate > 1.0 ||
                    cfg.max_sql_length < 64 || cfg.max_param_length < 1) {
                    error = "invalid observability.sql_log configuration";
                    return false;
                }
            }
            if (observability.contains("slow_sql")) {
                if (!observability["slow_sql"].is_object()) {
                    error = "observability.slow_sql must be an object";
                    return false;
                }
                const auto &slowSql = observability["slow_sql"];
                auto &cfg = out.observability.slow_sql;
                cfg.enabled = slowSql.value("enabled", cfg.enabled);
                cfg.threshold_ms = slowSql.value("threshold_ms", cfg.threshold_ms);
                cfg.aggregate_capacity = slowSql.value(
                    "aggregate_capacity", cfg.aggregate_capacity);
                cfg.recent_capacity = slowSql.value("recent_capacity", cfg.recent_capacity);
                cfg.retain_rendered_sql = slowSql.value(
                    "retain_rendered_sql", cfg.retain_rendered_sql);
                cfg.max_sql_length = slowSql.value("max_sql_length", cfg.max_sql_length);
                if (slowSql.contains("histogram_buckets_ms")) {
                    cfg.histogram_buckets_ms = slowSql["histogram_buckets_ms"]
                        .get<std::vector<int>>();
                }
                if (cfg.threshold_ms < 0 || cfg.aggregate_capacity < 1 ||
                    cfg.recent_capacity < 0 || cfg.max_sql_length < 64 ||
                    cfg.histogram_buckets_ms.empty()) {
                    error = "invalid observability.slow_sql configuration";
                    return false;
                }
                int previous = -1;
                for (const int bucket: cfg.histogram_buckets_ms) {
                    if (bucket <= 0 || bucket <= previous) {
                        error = "slow_sql histogram buckets must be positive and increasing";
                        return false;
                    }
                    previous = bucket;
                }
            }
            if (observability.contains("pool_metrics")) {
                if (!observability["pool_metrics"].is_object()) {
                    error = "observability.pool_metrics must be an object";
                    return false;
                }
                out.observability.pool_metrics.enabled = observability["pool_metrics"].value(
                    "enabled", out.observability.pool_metrics.enabled);
            }
            if (observability.contains("stats_report")) {
                if (!observability["stats_report"].is_object()) {
                    error = "observability.stats_report must be an object";
                    return false;
                }
                const auto &report = observability["stats_report"];
                auto &cfg = out.observability.stats_report;
                cfg.enabled = report.value("enabled", cfg.enabled);
                cfg.interval_ms = report.value("interval_ms", cfg.interval_ms);
                cfg.file = report.value("file", cfg.file);
                cfg.format = report.value("format", cfg.format);
                cfg.include_pool = report.value("include_pool", cfg.include_pool);
                cfg.include_slow_sql = report.value("include_slow_sql", cfg.include_slow_sql);
                cfg.slow_sql_limit = report.value("slow_sql_limit", cfg.slow_sql_limit);
                // 间隔过小会让写文件本身变成热点，这里静默抬到下限而不是报错。
                if (cfg.interval_ms < 1000) cfg.interval_ms = 1000;
                if (cfg.format != "text" && cfg.format != "json") {
                    error = "observability.stats_report.format must be 'text' or 'json'";
                    return false;
                }
                if (cfg.slow_sql_limit < 0) {
                    error = "invalid observability.stats_report configuration";
                    return false;
                }
            }
        }

        // ---- 限流 ----
        if (j.contains("rate_limit")) {
            if (!j["rate_limit"].is_object()) {
                error = "rate_limit must be an object";
                return false;
            }
            const auto &rl = j["rate_limit"];
            out.rate_limit.enabled = rl.value("enabled", out.rate_limit.enabled);
            out.rate_limit.global_qps = rl.value("global_qps", out.rate_limit.global_qps);
            out.rate_limit.per_fingerprint_qps = rl.value(
                "per_fingerprint_qps", out.rate_limit.per_fingerprint_qps);
            out.rate_limit.burst = rl.value("burst", out.rate_limit.burst);
            out.rate_limit.fingerprint_mode = rl.value(
                "fingerprint_mode", out.rate_limit.fingerprint_mode);
            if (out.rate_limit.global_qps < 0 || out.rate_limit.per_fingerprint_qps < 0 ||
                out.rate_limit.burst < 0) {
                error = "invalid rate_limit configuration";
                return false;
            }
            const auto &fm = out.rate_limit.fingerprint_mode;
            if (fm != "off" && fm != "template" && fm != "full") {
                error = "rate_limit.fingerprint_mode must be off/template/full";
                return false;
            }
        }

        // ---- SQL 审计 ----
        if (j.contains("sql_audit")) {
            if (!j["sql_audit"].is_object()) {
                error = "sql_audit must be an object";
                return false;
            }
            const auto &audit = j["sql_audit"];
            out.sql_audit.enabled = audit.value("enabled", out.sql_audit.enabled);
            out.sql_audit.action = audit.value("action", out.sql_audit.action);
            out.sql_audit.block_no_where_dml = audit.value(
                "block_no_where_dml", out.sql_audit.block_no_where_dml);
            out.sql_audit.require_limit_select = audit.value(
                "require_limit_select", out.sql_audit.require_limit_select);
            out.sql_audit.enforce_read_only = audit.value(
                "enforce_read_only", out.sql_audit.enforce_read_only);
            out.sql_audit.log_blocked = audit.value(
                "log_blocked", out.sql_audit.log_blocked);
            if (audit.contains("blacklist_fingerprints"))
                out.sql_audit.blacklist_fingerprints =
                    audit["blacklist_fingerprints"].get<std::vector<std::uint64_t>>();
            if (audit.contains("whitelist_fingerprints"))
                out.sql_audit.whitelist_fingerprints =
                    audit["whitelist_fingerprints"].get<std::vector<std::uint64_t>>();
            if (out.sql_audit.action != "block" && out.sql_audit.action != "warn") {
                error = "sql_audit.action must be block/warn";
                return false;
            }
        }

        // ---- 查询缓存 ----
        if (j.contains("query_cache")) {
            if (!j["query_cache"].is_object()) {
                error = "query_cache must be an object";
                return false;
            }
            const auto &qc = j["query_cache"];
            out.query_cache.enabled = qc.value("enabled", out.query_cache.enabled);
            out.query_cache.ttl_ms = qc.value("ttl_ms", out.query_cache.ttl_ms);
            out.query_cache.max_entries = qc.value("max_entries", out.query_cache.max_entries);
            out.query_cache.max_memory_bytes = qc.value(
                "max_memory_bytes", out.query_cache.max_memory_bytes);
            out.query_cache.cache_on_replica_only = qc.value(
                "cache_on_replica_only", out.query_cache.cache_on_replica_only);
        if (out.query_cache.ttl_ms < 0 || out.query_cache.max_entries < 0 ||
            out.query_cache.max_memory_bytes < 0) {
            error = "invalid query_cache configuration";
            return false;
        }
    }

    // ---- 游标 ----
    if (j.contains("cursor")) {
        if (!j["cursor"].is_object()) {
            error = "cursor must be an object";
            return false;
        }
        const auto &cur = j["cursor"];
        out.cursor.enabled = cur.value("enabled", out.cursor.enabled);
        out.cursor.default_batch_size = cur.value(
            "default_batch_size", out.cursor.default_batch_size);
        out.cursor.max_open_cursors = cur.value(
            "max_open_cursors", out.cursor.max_open_cursors);
        out.cursor.allow_scrollable = cur.value(
            "allow_scrollable", out.cursor.allow_scrollable);
        if (out.cursor.default_batch_size < 0 || out.cursor.max_open_cursors < 0) {
            error = "invalid cursor configuration";
            return false;
        }
    }

    // ---- 预编译语句缓存 ----
    if (j.contains("prepared_cache")) {
        if (!j["prepared_cache"].is_object()) {
            error = "prepared_cache must be an object";
            return false;
        }
        const auto &pc = j["prepared_cache"];
        out.prepared_cache.enabled = pc.value("enabled", out.prepared_cache.enabled);
        out.prepared_cache.max_per_connection = pc.value(
            "max_per_connection", out.prepared_cache.max_per_connection);
        // 负数直接判为配置错误，而不是悄悄当 0 处理：
        // 0 是"不限制"的合法取值，静默纠偏会让调用方误以为自己设了上限。
        if (out.prepared_cache.max_per_connection < 0) {
            error = "invalid prepared_cache configuration: max_per_connection must be >= 0";
            return false;
        }
    }

        if (!j.contains("datasources") || !j["datasources"].is_array()) {
            error = "missing 'datasources' array";
            return false;
        }

        for (const auto &d: j["datasources"]) {
            if (!d.is_object()) {
                error = "datasource entry must be object";
                return false;
            }
            DataSourceConfig cfg;
            cfg.name = d.value("name", std::string());
            cfg.type = d.value("type", std::string());
            cfg.host = d.value("host", std::string());
            cfg.port = d.value("port", 0);
            cfg.user = d.value("user", std::string());
            cfg.password = d.value("password", std::string());
            cfg.password_env = d.value("password_env", std::string());
            cfg.database = d.value("database", std::string());
            cfg.dsn = d.value("dsn", std::string());
            cfg.connection_timeout_ms = d.value("connection_timeout_ms", 5000);
            cfg.socket_timeout_ms = d.value("socket_timeout_ms", 0);
            cfg.query_timeout_ms = d.value("query_timeout_ms", 0);
            cfg.max_result_rows = d.value("max_result_rows", 0);
            if (cfg.connection_timeout_ms < 0 || cfg.socket_timeout_ms < 0 ||
                cfg.query_timeout_ms < 0 || cfg.max_result_rows < 0) {
                error = "datasource '" + cfg.name
                    + "' timeout and max_result_rows values must be >= 0";
                return false;
            }
            if (!cfg.password_env.empty()) {
                const char *secret = std::getenv(cfg.password_env.c_str());
                if (!secret) {
                    error = "datasource '" + cfg.name + "' references missing password_env '"
                            + cfg.password_env + "'";
                    return false;
                }
                cfg.password = secret;
            }
            if (d.contains("tls")) {
                if (!d["tls"].is_object()) {
                    error = "datasource '" + cfg.name + "' tls must be an object";
                    return false;
                }
                const auto &tls = d["tls"];
                cfg.tls_enabled = tls.value("enabled", true);
                cfg.tls_verify_peer = tls.value("verify_peer", true);
                cfg.tls_ca = tls.value("ca", std::string());
                cfg.tls_cert = tls.value("cert", std::string());
                cfg.tls_key = tls.value("key", std::string());
                if (cfg.tls_verify_peer && cfg.tls_enabled && cfg.tls_ca.empty()) {
                    error = "datasource '" + cfg.name
                            + "' enables TLS peer verification but tls.ca is empty";
                    return false;
                }
            }
            if (d.contains("extra") && d["extra"].is_object()) {
                for (auto it = d["extra"].begin(); it != d["extra"].end(); ++it) {
                    cfg.extra[it.key()] = it.value().get<std::string>();
                }
            }
            if (cfg.name.empty()) {
                error = "datasource missing 'name'";
                return false;
            }
            if (cfg.type.empty()) {
                error = "datasource '" + cfg.name + "' missing 'type'";
                return false;
            }
            out.datasources.push_back(std::move(cfg));
        }

        if (j.contains("groups")) {
            if (!j["groups"].is_array()) {
                error = "groups must be an array";
                return false;
            }
            for (const auto &g: j["groups"]) {
                if (!g.is_object()) {
                    error = "group entry must be object";
                    return false;
                }
                DataSourceGroupConfig group;
                group.name = g.value("name", std::string());
                group.primary = g.value("primary", std::string());
                group.read_after_write_ms = g.value("read_after_write_ms", 0);
                group.fallback_to_primary = g.value("fallback_to_primary", true);
                if (group.name.empty() || group.primary.empty() ||
                    group.read_after_write_ms < 0) {
                    error = "invalid datasource group";
                    return false;
                }
                if (g.contains("replicas")) {
                    if (!g["replicas"].is_array()) {
                        error = "group '" + group.name + "' replicas must be an array";
                        return false;
                    }
                    for (const auto &r: g["replicas"]) {
                        ReplicaConfig replica;
                        if (r.is_string()) {
                            replica.name = r.get<std::string>();
                        } else if (r.is_object()) {
                            replica.name = r.value("name", std::string());
                            replica.weight = r.value("weight", 1);
                        } else {
                            error = "invalid replica in group '" + group.name + "'";
                            return false;
                        }
                        if (replica.name.empty() || replica.weight < 1 || replica.weight > 100) {
                            error = "invalid replica in group '" + group.name + "'";
                            return false;
                        }
                        group.replicas.push_back(std::move(replica));
                    }
                }
                group.read_only = g.value("read_only", false);
                if (g.contains("failover") && g["failover"].is_object()) {
                    const auto &fo = g["failover"];
                    if (fo.contains("primaries") && fo["primaries"].is_array()) {
                        for (const auto &p: fo["primaries"]) {
                            if (!p.is_string() || p.get<std::string>().empty()) {
                                error = "group '" + group.name
                                    + "' failover.primaries must be non-empty strings";
                                return false;
                            }
                            group.failover.primaries.push_back(p.get<std::string>());
                        }
                    }
                    group.failover.require_healthy = fo.value("require_healthy", false);
                    if (fo.contains("write_buffer") && fo["write_buffer"].is_object()) {
                        const auto &wb = fo["write_buffer"];
                        group.failover.write_buffer.enabled = wb.value("enabled", false);
                        group.failover.write_buffer.max_queue = wb.value("max_queue", 1000);
                        group.failover.write_buffer.ttl_ms = wb.value("ttl_ms", 30000);
                        group.failover.write_buffer.flush_interval_ms =
                            wb.value("flush_interval_ms", 1000);
                        if (group.failover.write_buffer.max_queue < 1 ||
                            group.failover.write_buffer.ttl_ms < 0 ||
                            group.failover.write_buffer.flush_interval_ms < 1) {
                            error = "invalid group '" + group.name + "' failover.write_buffer";
                            return false;
                        }
                    }
                }
                out.groups.push_back(std::move(group));
            }
        }

        if (out.default_datasource.empty() && !out.datasources.empty()) {
            out.default_datasource = out.datasources.front().name;
        }
        return true;
        } catch (const json::exception &e) {
            out = GlobalConfig{};
            error = std::string("config validation error: ") + e.what();
            return false;
        }
    }
} // namespace dbmw::config
