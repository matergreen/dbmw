#include "dbmw/core/stats_reporter.h"
#include "dbmw/common/logger.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>


namespace dbmw::core {
    namespace {
        std::string formatTimestamp(const std::chrono::system_clock::time_point &tp) {
            const std::time_t t = std::chrono::system_clock::to_time_t(tp);
            std::tm parts{};
#if defined(_WIN32)
            localtime_s(&parts, &t);
#else
            localtime_r(&t, &parts);
#endif
            char buf[32] = {0};
            std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &parts);
            return std::string(buf);
        }

        // SQL 模板里可能有引号、换行和控制字符，手写 JSON 必须转义，
        // 否则一次含双引号的查询就会让整行记录变得无法解析。
        std::string escapeJson(const std::string &s) {
            std::string out;
            out.reserve(s.size() + 8);
            for (const char c: s) {
                switch (c) {
                    case '"': out += "\\\""; break;
                    case '\\': out += "\\\\"; break;
                    case '\n': out += "\\n"; break;
                    case '\r': out += "\\r"; break;
                    case '\t': out += "\\t"; break;
                    default:
                        if (static_cast<unsigned char>(c) < 0x20) {
                            char buf[8];
                            std::snprintf(buf, sizeof(buf), "\\u%04x",
                                          static_cast<unsigned char>(c));
                            out += buf;
                        } else {
                            out += c;
                        }
                }
            }
            return out;
        }

        double averageMs(const common::SlowSqlStats &s) {
            if (s.count == 0) return 0.0;
            return static_cast<double>(s.totalDuration.count())
                   / static_cast<double>(s.count) / 1000.0;
        }

        std::string clip(const std::string &s, const std::size_t maxLen) {
            if (s.size() <= maxLen) return s;
            return s.substr(0, maxLen) + "...";
        }
    } // namespace

    StatsReporter::~StatsReporter() {
        stop();
    }

    void StatsReporter::start(const config::StatsReportConfig &cfg, PoolStatsCollector collector) {
        stop();
        if (!cfg.enabled) return;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            cfg_ = cfg;
            collector_ = std::move(collector);
            running_ = true;
        }
        thread_ = std::thread([this] { run(); });
    }

    void StatsReporter::stop() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (!running_) return;
            running_ = false;
        }
        cv_.notify_all();
        if (thread_.joinable()) thread_.join();
    }

    bool StatsReporter::running() const {
        std::lock_guard<std::mutex> lk(mtx_);
        return running_;
    }

    void StatsReporter::run() {
        std::unique_lock<std::mutex> lk(mtx_);
        for (;;) {
            const auto interval = std::chrono::milliseconds(cfg_.interval_ms);
            // 被唤醒且 running_ 已置 false -> 退出；超时 -> 采样一轮。
            if (cv_.wait_for(lk, interval, [this] { return !running_; })) return;
            // 采集与写文件期间不持锁，避免 stop() 被长时间阻塞在 join 上。
            lk.unlock();
            try {
                writeOnce();
            } catch (...) {
                // 统计是旁路功能，任何失败都只吞掉，绝不影响业务。
            }
            lk.lock();
        }
    }

    void StatsReporter::writeOnce() {
        const auto now = std::chrono::system_clock::now();
        std::vector<NamedPoolStats> pools;
        if (cfg_.include_pool && collector_) pools = collector_();
        std::vector<common::SlowSqlStats> slowSql;
        if (cfg_.include_slow_sql) {
            slowSql = common::Observability::slowSqlStats(
                static_cast<std::size_t>(std::max(0, cfg_.slow_sql_limit)));
        }

        const std::string text = cfg_.format == "json"
                                     ? renderJson(now, pools, slowSql)
                                     : renderText(now, pools, slowSql);

        if (cfg_.file.empty()) {
            // 没配文件就只走日志，本地调试时也能看到统计内容。
            DBMW_LOG_INFO("stats report:\n" + text);
            return;
        }

        std::error_code ec;
        const auto parent = std::filesystem::path(cfg_.file).parent_path();
        if (!parent.empty()) std::filesystem::create_directories(parent, ec);
        std::ofstream out(cfg_.file, std::ios::app);
        if (!out) {
            DBMW_LOG_WARN("stats report: cannot open file '" + cfg_.file + "'");
            return;
        }
        out << text;
        if (cfg_.format == "json") out << '\n';
        out.flush();
    }

    std::string StatsReporter::renderText(
        const std::chrono::system_clock::time_point &now,
        const std::vector<NamedPoolStats> &pools,
        const std::vector<common::SlowSqlStats> &slowSql) const {
        std::ostringstream os;
        os << "[" << formatTimestamp(now) << "] dbmw stats report\n";

        if (cfg_.include_pool) {
            os << "pools:\n";
            if (pools.empty()) {
                os << "  (none)\n";
            } else {
                for (const auto &p: pools) {
                    const auto &s = p.stats;
                    os << "  " << p.dataSource
                       << "  idle=" << s.idle
                       << " borrowed=" << s.borrowed << "/" << s.maxConnections
                       << " waiting=" << s.waiting
                       << " util=" << std::fixed << std::setprecision(1)
                       << (s.utilization() * 100.0) << "%"
                       << " created=" << s.connectionsCreated
                       << " closed=" << s.connectionsClosed
                       << " timeouts=" << s.borrowTimeouts
                       << " invalidated=" << s.invalidatedConnections
                       << "\n";
                }
            }
        }

        if (cfg_.include_slow_sql) {
            os << "slow sql (top " << slowSql.size() << "):\n";
            if (slowSql.empty()) {
                os << "  (none)\n";
            } else {
                int rank = 1;
                for (const auto &s: slowSql) {
                    os << "  " << (rank++) << ". [" << s.dataSource << "]"
                       << " avg=" << std::fixed << std::setprecision(2) << averageMs(s) << "ms"
                       << " max=" << std::fixed << std::setprecision(2)
                       << (s.maxDuration.count() / 1000.0) << "ms"
                       << " count=" << s.count
                       << " errors=" << s.errorCount
                       << " timeouts=" << s.timeoutCount
                       << "  " << clip(s.sqlTemplate, 120) << "\n";
                }
            }
        }
        return os.str();
    }

    std::string StatsReporter::renderJson(
        const std::chrono::system_clock::time_point &now,
        const std::vector<NamedPoolStats> &pools,
        const std::vector<common::SlowSqlStats> &slowSql) const {
        std::ostringstream os;
        os << std::fixed;
        os << R"({"timestamp":")" << formatTimestamp(now) << "\"";

        if (cfg_.include_pool) {
            os << ",\"pools\":[";
            bool first = true;
            for (const auto &p: pools) {
                if (!first) os << ",";
                first = false;
                const auto &s = p.stats;
                os << R"({"name":")" << escapeJson(p.dataSource) << "\""
                   << ",\"idle\":" << s.idle
                   << ",\"borrowed\":" << s.borrowed
                   << ",\"max\":" << s.maxConnections
                   << ",\"waiting\":" << s.waiting
                   << ",\"utilization\":" << std::setprecision(4) << s.utilization()
                   << ",\"created\":" << s.connectionsCreated
                   << ",\"closed\":" << s.connectionsClosed
                   << ",\"timeouts\":" << s.borrowTimeouts
                   << ",\"invalidated\":" << s.invalidatedConnections
                   << "}";
            }
            os << "]";
        }

        if (cfg_.include_slow_sql) {
            os << ",\"slow_sql\":[";
            bool first = true;
            for (const auto &s: slowSql) {
                if (!first) os << ",";
                first = false;
                os << R"({"data_source":")" << escapeJson(s.dataSource) << "\""
                   << ",\"fingerprint\":" << s.fingerprint
                   << ",\"avg_ms\":" << std::setprecision(2) << averageMs(s)
                   << ",\"max_ms\":" << std::setprecision(2)
                   << (s.maxDuration.count() / 1000.0)
                   << ",\"count\":" << s.count
                   << ",\"errors\":" << s.errorCount
                   << ",\"timeouts\":" << s.timeoutCount
                   << R"(,"sql":")" << escapeJson(clip(s.sqlTemplate, 512)) << "\""
                   << "}";
            }
            os << "]";
        }

        os << "}";
        return os.str();
    }
} // namespace dbmw::core
