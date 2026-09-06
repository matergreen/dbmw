#include "dbmw/exporters/prometheus.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

namespace dbmw::exporters {
    namespace {
        // Prometheus label value 转义：
        //   - \\ -> \\\\
        //   - \" -> \\\"
        //   - \n -> \\n
        // 其它控制字符替换为 '?'，避免产生 exposition 格式不支持的转义。
        std::string escapeLabel(const std::string &s) {
            std::string out;
            out.reserve(s.size() + 8);
            for (const char c: s) {
                switch (c) {
                    case '\\': out += "\\\\"; break;
                    case '"': out += "\\\""; break;
                    case '\n': out += "\\n"; break;
                    default:
                        if (static_cast<unsigned char>(c) < 0x20) {
                            // Prometheus text exposition 只定义 \\, \" 与 \n；
                            // \uXXXX 会被解析器判为非法 escape。
                            out.push_back('?');
                        } else {
                            out += c;
                        }
                }
            }
            return out;
        }

        // Prometheus 文本格式 HELP/TYPE + 行。
        void emitHelp(std::ostringstream &os, const std::string &name,
                      const std::string &help) {
            os << "# HELP " << name << ' ' << help << '\n';
        }
        void emitType(std::ostringstream &os, const std::string &name,
                      const std::string &type) {
            os << "# TYPE " << name << ' ' << type << '\n';
        }

        // 标签字符串拼接：k1="v1",k2="v2"，全部转义。空 label map 返回空串。
        std::string renderLabels(const std::vector<std::pair<std::string, std::string>> &kvs) {
            if (kvs.empty()) return {};
            std::ostringstream os;
            bool first = true;
            for (const auto &kv: kvs) {
                if (!first) os << ',';
                first = false;
                os << kv.first << "=\"" << escapeLabel(kv.second) << '"';
            }
            return os.str();
        }

        // 计数器行：{labels} value（无 label 时省去大括号）。
        void emitMetric(std::ostringstream &os, const std::string &name,
                        const std::string &labels, std::uint64_t value) {
            if (labels.empty()) os << name << ' ' << value << '\n';
            else os << name << '{' << labels << "} " << value << '\n';
        }
        void emitMetric(std::ostringstream &os, const std::string &name,
                        const std::string &labels, double value) {
            if (labels.empty()) os << name << ' ' << value << '\n';
            else os << name << '{' << labels << "} " << value << '\n';
        }
    } // namespace

    std::string toPrometheusText(const common::PoolMetricsEvent &pools,
                                 const std::vector<common::SlowSqlStats> &slow,
                                 const std::string &prefix,
                                 std::size_t maxFingerprintLabels) {
        std::ostringstream os;

        // ===== 池指标 =====
        const std::string p_conn = prefix + "_pool_connections";
        const std::string p_max = prefix + "_pool_connections_max";
        const std::string p_min = prefix + "_pool_connections_min";
        const std::string p_idle = prefix + "_pool_connections_idle";
        const std::string p_borrowed = prefix + "_pool_connections_borrowed";
        const std::string p_util = prefix + "_pool_utilization_ratio";
        const std::string p_waiting = prefix + "_pool_waiting";
        const std::string p_req = prefix + "_pool_borrow_requests_total";
        const std::string p_succ = prefix + "_pool_borrow_successes_total";
        const std::string p_to = prefix + "_pool_borrow_timeouts_total";
        const std::string p_ccf = prefix + "_pool_connection_create_failures_total";
        const std::string p_inv = prefix + "_pool_invalidated_connections_total";
        const std::string p_vf = prefix + "_pool_validation_failures_total";
        const std::string p_leak = prefix + "_pool_leak_warnings_total";
        const std::string p_iev = prefix + "_pool_idle_evictions_total";
        const std::string p_lev = prefix + "_pool_lifetime_evictions_total";
        const std::string p_created = prefix + "_pool_connections_created_total";
        const std::string p_closed = prefix + "_pool_connections_closed_total";
        const std::string p_wait_secs = prefix + "_pool_borrow_wait_seconds_total";
        const std::string p_wait_max = prefix + "_pool_borrow_wait_seconds_max";

        // HELP/TYPE 只在第一个池出现前发一次，避免重复。
        // 但同一指标可能有多条不同 data_source 的 series，因此把所有数据源
        // 标签合并后再统一发 HELP/TYPE——这是 Prometheus 的惯例。
        // 这里采取更直观的策略：每个指标在第一行数据前发一次 HELP/TYPE，
        // 实现上是"先收集 label 集合，再输出"。
        // 为简化，每个 metric 用单独 if 分支：第一次进入时输出 HELP/TYPE。
        bool conn_emitted = false, max_emitted = false, min_emitted = false;
        bool idle_emitted = false, borrowed_emitted = false, util_emitted = false;
        bool waiting_emitted = false, req_emitted = false, succ_emitted = false;
        bool to_emitted = false, ccf_emitted = false, inv_emitted = false;
        bool vf_emitted = false, leak_emitted = false, iev_emitted = false;
        bool lev_emitted = false, created_emitted = false, closed_emitted = false;
        bool wait_secs_emitted = false, wait_max_emitted = false;

        for (const auto &np: pools.pools) {
            const std::string lbl = renderLabels(
                {{"data_source", np.dataSource}});
            const auto &s = np.stats;

            if (!conn_emitted) {
                emitHelp(os, p_conn, "Current total connections in the pool.");
                emitType(os, p_conn, "gauge");
                conn_emitted = true;
            }
            emitMetric(os, p_conn, lbl, static_cast<std::uint64_t>(s.total));

            if (!max_emitted) {
                emitHelp(os, p_max, "Configured maximum connections.");
                emitType(os, p_max, "gauge");
                max_emitted = true;
            }
            emitMetric(os, p_max, lbl, static_cast<std::uint64_t>(s.maxConnections));

            if (!min_emitted) {
                emitHelp(os, p_min, "Configured minimum connections.");
                emitType(os, p_min, "gauge");
                min_emitted = true;
            }
            emitMetric(os, p_min, lbl, static_cast<std::uint64_t>(s.minConnections));

            if (!idle_emitted) {
                emitHelp(os, p_idle, "Idle (free) connections.");
                emitType(os, p_idle, "gauge");
                idle_emitted = true;
            }
            emitMetric(os, p_idle, lbl, static_cast<std::uint64_t>(s.idle));

            if (!borrowed_emitted) {
                emitHelp(os, p_borrowed, "Currently borrowed connections.");
                emitType(os, p_borrowed, "gauge");
                borrowed_emitted = true;
            }
            emitMetric(os, p_borrowed, lbl, static_cast<std::uint64_t>(s.borrowed));

            if (!util_emitted) {
                emitHelp(os, p_util, "Pool utilization (borrowed / maxConnections).");
                emitType(os, p_util, "gauge");
                util_emitted = true;
            }
            emitMetric(os, p_util, lbl, s.utilization());

            if (!waiting_emitted) {
                emitHelp(os, p_waiting, "Threads + futures waiting for a connection.");
                emitType(os, p_waiting, "gauge");
                waiting_emitted = true;
            }
            emitMetric(os, p_waiting, lbl,
                       static_cast<std::uint64_t>(s.waiting + s.asyncWaiting));

            if (!req_emitted) {
                emitHelp(os, p_req, "Total borrow attempts.");
                emitType(os, p_req, "counter");
                req_emitted = true;
            }
            emitMetric(os, p_req, lbl, s.borrowRequests);

            if (!succ_emitted) {
                emitHelp(os, p_succ, "Successful borrows.");
                emitType(os, p_succ, "counter");
                succ_emitted = true;
            }
            emitMetric(os, p_succ, lbl, s.borrowSuccesses);

            if (!to_emitted) {
                emitHelp(os, p_to, "Borrow timeouts.");
                emitType(os, p_to, "counter");
                to_emitted = true;
            }
            emitMetric(os, p_to, lbl, s.borrowTimeouts);

            if (!ccf_emitted) {
                emitHelp(os, p_ccf, "Connection create failures.");
                emitType(os, p_ccf, "counter");
                ccf_emitted = true;
            }
            emitMetric(os, p_ccf, lbl, s.connectionCreateFailures);

            if (!inv_emitted) {
                emitHelp(os, p_inv, "Connections invalidated by health check.");
                emitType(os, p_inv, "counter");
                inv_emitted = true;
            }
            emitMetric(os, p_inv, lbl, s.invalidatedConnections);

            if (!vf_emitted) {
                emitHelp(os, p_vf, "Validation failures during borrow.");
                emitType(os, p_vf, "counter");
                vf_emitted = true;
            }
            emitMetric(os, p_vf, lbl, s.validationFailures);

            if (!leak_emitted) {
                emitHelp(os, p_leak, "Connection leak warnings emitted.");
                emitType(os, p_leak, "counter");
                leak_emitted = true;
            }
            emitMetric(os, p_leak, lbl, s.leakWarnings);

            if (!iev_emitted) {
                emitHelp(os, p_iev, "Idle-timeout evictions.");
                emitType(os, p_iev, "counter");
                iev_emitted = true;
            }
            emitMetric(os, p_iev, lbl, s.idleEvictions);

            if (!lev_emitted) {
                emitHelp(os, p_lev, "Lifetime evictions.");
                emitType(os, p_lev, "counter");
                lev_emitted = true;
            }
            emitMetric(os, p_lev, lbl, s.lifetimeEvictions);

            if (!created_emitted) {
                emitHelp(os, p_created, "Connections ever created (lifetime).");
                emitType(os, p_created, "counter");
                created_emitted = true;
            }
            emitMetric(os, p_created, lbl, s.connectionsCreated);

            if (!closed_emitted) {
                emitHelp(os, p_closed, "Connections ever closed (lifetime).");
                emitType(os, p_closed, "counter");
                closed_emitted = true;
            }
            emitMetric(os, p_closed, lbl, s.connectionsClosed);

            if (!wait_secs_emitted) {
                emitHelp(os, p_wait_secs, "Cumulative borrow wait time in seconds.");
                emitType(os, p_wait_secs, "counter");
                wait_secs_emitted = true;
            }
            emitMetric(os, p_wait_secs, lbl,
                       static_cast<double>(s.totalBorrowWait.count()) / 1e6);

            if (!wait_max_emitted) {
                emitHelp(os, p_wait_max, "Max single borrow wait in seconds.");
                emitType(os, p_wait_max, "gauge");
                wait_max_emitted = true;
            }
            emitMetric(os, p_wait_max, lbl,
                       static_cast<double>(s.maxBorrowWait.count()) / 1e6);
        }

        // ===== 慢 SQL =====
        if (!slow.empty()) {
            // 控制高基数：maxFingerprintLabels 为 0 时按"全部导出"，否则按传入顺序截断。
            const std::size_t take = maxFingerprintLabels == 0
                ? slow.size()
                : (std::min)(slow.size(), maxFingerprintLabels);

            const std::string s_count = prefix + "_slow_sql_count";
            const std::string s_err = prefix + "_slow_sql_errors";
            const std::string s_to = prefix + "_slow_sql_timeouts";
            const std::string s_duration = prefix + "_slow_sql_duration_seconds";
            const std::string s_sum = s_duration + "_sum";
            const std::string s_max = prefix + "_slow_sql_duration_seconds_max";
            const std::string s_hist = s_duration + "_bucket";
            const std::string s_hist_count = s_duration + "_count";

            bool count_emitted = false, err_emitted = false, to_emitted_s = false;
            bool duration_family_emitted = false, max_emitted_s = false;

            for (std::size_t i = 0; i < take; ++i) {
                const auto &s = slow[i];
                const std::string lbl = renderLabels({
                    {"data_source", s.dataSource},
                    {"fingerprint", std::to_string(s.fingerprint)},
                });

                if (!count_emitted) {
                    emitHelp(os, s_count, "Slow SQL occurrence count.");
                    emitType(os, s_count, "counter");
                    count_emitted = true;
                }
                emitMetric(os, s_count, lbl, s.count);

                if (!err_emitted) {
                    emitHelp(os, s_err, "Slow SQL error count.");
                    emitType(os, s_err, "counter");
                    err_emitted = true;
                }
                emitMetric(os, s_err, lbl, s.errorCount);

                if (!to_emitted_s) {
                    emitHelp(os, s_to, "Slow SQL timeout count.");
                    emitType(os, s_to, "counter");
                    to_emitted_s = true;
                }
                emitMetric(os, s_to, lbl, s.timeoutCount);

                if (!duration_family_emitted) {
                    emitHelp(os, s_duration, "Slow SQL duration histogram in seconds.");
                    emitType(os, s_duration, "histogram");
                    duration_family_emitted = true;
                }
                // totalDuration 微秒转秒
                emitMetric(os, s_sum, lbl,
                           static_cast<double>(s.totalDuration.count()) / 1e6);

                if (!max_emitted_s) {
                    emitHelp(os, s_max, "Max slow SQL duration in seconds.");
                    emitType(os, s_max, "gauge");
                    max_emitted_s = true;
                }
                emitMetric(os, s_max, lbl,
                           static_cast<double>(s.maxDuration.count()) / 1e6);

                // histogram bucket
                //   s.histogramBucketsMs 与 s.histogram 等长（同步构造），
                //   这里逐个 bucket 输出 {le="<sec>"} cumulative count；
                //   最后一个隐式 +Inf 桶用 totalDuration/count 不准确，
                //   Prometheus 惯例 +Inf = count。直接读 count。
                const std::size_t n = std::min(s.histogramBucketsMs.size(),
                                               s.histogram.size());
                std::uint64_t cumulative = 0;
                for (std::size_t b = 0; b < n; ++b) {
                    cumulative += s.histogram[b];
                    const double leSec =
                        static_cast<double>(s.histogramBucketsMs[b]) / 1000.0;
                    std::ostringstream leStream;
                    leStream << leSec;
                    const std::string bucketLbl = renderLabels({
                        {"data_source", s.dataSource},
                        {"fingerprint", std::to_string(s.fingerprint)},
                        {"le", leStream.str()},
                    });
                    emitMetric(os, s_hist, bucketLbl, cumulative);
                }
                // +Inf 桶 = 总样本数。
                {
                    const std::string bucketLbl = renderLabels({
                        {"data_source", s.dataSource},
                        {"fingerprint", std::to_string(s.fingerprint)},
                        {"le", "+Inf"},
                    });
                    emitMetric(os, s_hist, bucketLbl, s.count);
                }
                emitMetric(os, s_hist_count, lbl, s.count);
            }
        }

        return os.str();
    }
} // namespace dbmw::exporters
