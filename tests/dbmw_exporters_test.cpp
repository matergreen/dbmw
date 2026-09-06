// dbmw M3 指标导出层单测：
//   - PoolMetricsEvent 透传（setPoolMetricsCollector/Observer + samplePoolMetrics）
//   - Prometheus 文本格式正确性（HELP/TYPE、标签转义、histogram +Inf 桶、截断）
//
// 与 M1/M2 测试同样不依赖真实数据库：手动构造 NamedPoolStats / SlowSqlStats。
#include "dbmw/common/observer.h"
#include "dbmw/exporters/prometheus.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace dbmw;
using common::PoolMetricsEvent;
using common::PoolMetricsObserver;
using common::PoolMetricsCollector;
using common::NamedPoolStats;
using common::SlowSqlStats;
using common::OperationType;

static int g_failed = 0;
static int g_passed = 0;

static void check(bool cond, const std::string &name) {
    if (cond) { ++g_passed; std::cout << "  [PASS] " << name << "\n"; }
    else { ++g_failed; std::cout << "  [FAIL] " << name << "\n"; }
}

// ---- 构造便利：手填 NamedPoolStats / SlowSqlStats ----
static NamedPoolStats makePool(const std::string &name,
                               size_t idle = 0, size_t borrowed = 0,
                               size_t max = 10, size_t total = 0,
                               std::uint64_t borrows = 0,
                               std::uint64_t timeouts = 0) {
    NamedPoolStats p;
    p.dataSource = name;
    p.stats.idle = idle;
    p.stats.borrowed = borrowed;
    p.stats.maxConnections = max;
    p.stats.minConnections = 0;
    p.stats.total = total ? total : (idle + borrowed);
    p.stats.waiting = 0;
    p.stats.borrowRequests = borrows;
    p.stats.borrowSuccesses = borrows;
    p.stats.borrowTimeouts = timeouts;
    p.stats.connectionsCreated = borrows;
    p.stats.connectionsClosed = 0;
    return p;
}

static SlowSqlStats makeSlow(const std::string &ds, std::uint64_t fp,
                             std::uint64_t count = 1,
                             std::chrono::microseconds max = std::chrono::milliseconds(50)) {
    SlowSqlStats s;
    s.dataSource = ds;
    s.fingerprint = fp;
    s.type = OperationType::Query;
    s.count = count;
    s.errorCount = 0;
    s.timeoutCount = 0;
    s.totalDuration = max;
    s.minDuration = max;
    s.maxDuration = max;
    s.firstSeen = std::chrono::system_clock::now();
    s.lastSeen = s.firstSeen;
    // histogram：3 个 bucket（10ms / 100ms / 1000ms）
    s.histogramBucketsMs = {10, 100, 1000};
    s.histogram = {0, count, 0}; // 全部命中 100ms 桶
    return s;
}

// ---- capture helpers ----
static std::mutex g_capMtx;
static std::vector<PoolMetricsEvent> g_captured;

static void capturingObserver(const PoolMetricsEvent &e) {
    std::lock_guard<std::mutex> lk(g_capMtx);
    g_captured.push_back(e);
}

static void clearCaptured() {
    std::lock_guard<std::mutex> lk(g_capMtx);
    g_captured.clear();
}

// ---- 正则/包含检查便利 ----
static bool contains(const std::string &haystack, const std::string &needle) {
    return haystack.find(needle) != std::string::npos;
}

static int countMatches(const std::string &haystack, const std::string &regexStr) {
    std::regex re(regexStr);
    auto begin = std::sregex_iterator(haystack.begin(), haystack.end(), re);
    auto end = std::sregex_iterator();
    return static_cast<int>(std::distance(begin, end));
}

int main() {
    std::cout << "== M3 指标导出：samplePoolMetrics 透传 ==\n";
    {
        clearCaptured();
        common::Observability::setPoolMetricsObserver(capturingObserver);
        common::Observability::setPoolMetricsCollector([] {
            std::vector<NamedPoolStats> v;
            v.push_back(makePool("ds-a", 3, 7, 10));
            v.push_back(makePool("ds-b", 0, 1, 5));
            return v;
        });

        const auto evt = common::Observability::samplePoolMetrics();

        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.size() == 1, "samplePoolMetrics 触发观察者一次");
        check(evt.pools.size() == 2, "event.pools 含两个数据源");
        check(g_captured[0].pools.size() == 2, "观察者收到的事件含两个数据源");
        check(g_captured[0].pools[0].dataSource == "ds-a",
              "数据源顺序与 collector 返回顺序一致");
        check(g_captured[0].timestamp.time_since_epoch().count() > 0,
              "event.timestamp 已被填充为系统时钟");
    }

    std::cout << "== M3 指标导出：无 collector 不报错 ==\n";
    {
        clearCaptured();
        common::Observability::setPoolMetricsCollector({});
        common::Observability::setPoolMetricsObserver(capturingObserver);
        const auto evt = common::Observability::samplePoolMetrics();
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(evt.pools.empty(), "无 collector 时 event.pools 为空");
        check(g_captured.size() == 1, "无 collector 仍触发观察者（sample 语义保留）");
        check(g_captured[0].pools.empty(), "观察者收到的事件 pools 也是空");
    }

    std::cout << "== M3 指标导出：collector 抛异常被吞掉 ==\n";
    {
        clearCaptured();
        common::Observability::setPoolMetricsObserver(capturingObserver);
        common::Observability::setPoolMetricsCollector([]() -> std::vector<NamedPoolStats> {
            throw std::runtime_error("simulated collector failure");
        });
        const auto evt = common::Observability::samplePoolMetrics();
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(evt.pools.empty(), "collector 抛异常时 event.pools 为空");
        check(g_captured.size() == 1, "collector 异常不影响观察者被调用");
    }

    std::cout << "== M3 指标导出：observer 抛异常被吞掉 ==\n";
    {
        clearCaptured();
        // 先注入一个正常 collector
        common::Observability::setPoolMetricsCollector([] {
            std::vector<NamedPoolStats> v;
            v.push_back(makePool("ds-z", 1, 0, 5));
            return v;
        });
        // 用抛异常的 observer 验证吞掉
        common::Observability::setPoolMetricsObserver(
            [](const PoolMetricsEvent &) {
                throw std::runtime_error("simulated observer failure");
            });
        // 不应抛出 → samplePoolMetrics 必须返回 event
        bool noThrow = true;
        try {
            (void)common::Observability::samplePoolMetrics();
        } catch (...) {
            noThrow = false;
        }
        check(noThrow, "observer 抛异常时 samplePoolMetrics 仍正常返回");
        // 此时没有任何 observer 捕获到事件
        std::lock_guard<std::mutex> lk(g_capMtx);
        check(g_captured.empty(), "异常 observer 不会写到 capturing 缓冲（说明它被吞了）");
    }

    std::cout << "== M3 指标导出：Prometheus 文本格式 — 池指标 ==\n";
    {
        PoolMetricsEvent evt;
        evt.timestamp = std::chrono::system_clock::now();
        evt.pools.push_back(makePool("ds-app", 3, 7, 10));
        evt.pools.push_back(makePool("ds-rep", 5, 0, 8, 5, 100, 2));

        const std::string text = exporters::toPrometheusText(evt, {});

        // HELP/TYPE 出现一次
        check(contains(text, "# HELP dbmw_pool_connections "),
              "dbmw_pool_connections 含 HELP 行");
        check(contains(text, "# TYPE dbmw_pool_connections gauge"),
              "dbmw_pool_connections 类型为 gauge");
        check(contains(text, "# HELP dbmw_pool_borrow_timeouts_total "),
              "borrow_timeouts 含 HELP 行");
        check(contains(text, "# TYPE dbmw_pool_borrow_timeouts_total counter"),
              "borrow_timeouts 类型为 counter");
        check(contains(text, "# HELP dbmw_pool_utilization_ratio "),
              "utilization 含 HELP 行");

        // data_source 标签
        check(contains(text, R"(data_source="ds-app")"),
              "输出包含 ds-app 标签");
        check(contains(text, R"(data_source="ds-rep")"),
              "输出包含 ds-rep 标签");

        // 利用率（ds-app: 7/10=0.7, ds-rep: 0/8=0.0）
        check(contains(text, R"(dbmw_pool_utilization_ratio{data_source="ds-app"} 0.7)"),
              "ds-app utilization = 0.7");
        check(contains(text, R"(dbmw_pool_utilization_ratio{data_source="ds-rep"} 0)"),
              "ds-rep utilization = 0（无借用）");

        // 计数器：ds-rep borrows=100, timeouts=2
        check(contains(text, R"(dbmw_pool_borrow_requests_total{data_source="ds-rep"} 100)"),
              "ds-rep borrow_requests_total = 100");
        check(contains(text, R"(dbmw_pool_borrow_timeouts_total{data_source="ds-rep"} 2)"),
              "ds-rep borrow_timeouts_total = 2");

        // 等待数 = waiting + asyncWaiting（默认 waiting=0, asyncWaiting=0）
        check(contains(text, R"(dbmw_pool_waiting{data_source="ds-app"} 0)"),
              "ds-app waiting = 0（waiting+asyncWaiting）");

        // HELP/TYPE 每条指标只出现一次（不应重复）
        const int connHelpCount = countMatches(text,
            std::string(R"(# HELP dbmw_pool_connections )"));
        check(connHelpCount == 1,
              "dbmw_pool_connections HELP 行只出现一次（无重复）");
    }

    std::cout << "== M3 指标导出：Prometheus 文本格式 — 慢 SQL + histogram ==\n";
    {
        PoolMetricsEvent evt;
        evt.pools.push_back(makePool("ds-app", 1, 0, 5));
        std::vector<SlowSqlStats> slow;
        slow.push_back(makeSlow("ds-app", 12345, 7, std::chrono::milliseconds(80)));
        slow.push_back(makeSlow("ds-app", 67890, 2, std::chrono::milliseconds(300)));

        const std::string text = exporters::toPrometheusText(evt, slow);

        check(contains(text, R"(# TYPE dbmw_slow_sql_count counter)"),
              "dbmw_slow_sql_count 类型为 counter");
        check(contains(text, R"(fingerprint="12345")"),
              "fingerprint label 暴露（设计稿 §5.4 警告高基数）");
        check(contains(text, R"(fingerprint="67890")"),
              "第二条慢 SQL fingerprint 暴露");
        check(contains(text, R"(dbmw_slow_sql_count{data_source="ds-app",fingerprint="12345"} 7)"),
              "slow_sql_count = 7");
        check(contains(text, R"(dbmw_slow_sql_count{data_source="ds-app",fingerprint="67890"} 2)"),
              "第二条 slow_sql_count = 2");

        // histogram bucket：第一个 fingerprint 在 10ms=0, 100ms=7（全部命中），最后 +Inf=7
        check(contains(text, R"(dbmw_slow_sql_duration_seconds_bucket{data_source="ds-app",fingerprint="12345",le="0.01"} 0)"),
              "histogram bucket le=0.01 cumulative = 0");
        check(contains(text, R"(dbmw_slow_sql_duration_seconds_bucket{data_source="ds-app",fingerprint="12345",le="0.1"} 7)"),
              "histogram bucket le=0.1 cumulative = 7");
        check(contains(text, R"(dbmw_slow_sql_duration_seconds_bucket{data_source="ds-app",fingerprint="12345",le="+Inf"} 7)"),
              "histogram +Inf bucket = count = 7");
    }

    std::cout << "== M3 指标导出：Prometheus 转义 label 值 ==\n";
    {
        PoolMetricsEvent evt;
        // 故意构造含 " \ \n 的数据源名
        NamedPoolStats p;
        p.dataSource = "ds-with-\"quote-and-\\backslash-and-\nnewline";
        p.stats.idle = 0;
        p.stats.borrowed = 1;
        p.stats.maxConnections = 2;
        p.stats.total = 1;
        evt.pools.push_back(p);

        const std::string text = exporters::toPrometheusText(evt, {});
        check(contains(text, R"(data_source="ds-with-\"quote-and-\\backslash-and-\nnewline")"),
              "label value 中的 \\\" \\\\ \\n 均被转义");
    }

    std::cout << "== M3 指标导出：maxFingerprintLabels 截断 ==\n";
    {
        PoolMetricsEvent evt;
        std::vector<SlowSqlStats> slow;
        for (std::uint64_t i = 0; i < 5; ++i)
            slow.push_back(makeSlow("ds-app", 1000 + i, 1));

        const std::string allText = exporters::toPrometheusText(evt, slow, "dbmw", 0);
        const std::string cutText = exporters::toPrometheusText(evt, slow, "dbmw", 2);

        const int allFp = countMatches(allText,
            std::string(R"(fingerprint="1\d{3}")"));
        const int cutFp = countMatches(cutText,
            std::string(R"(fingerprint="1\d{3}")"));

        check(allFp == 5, "不限时 5 条 fingerprint 全部出现");
        check(cutFp == 2, "maxFingerprintLabels=2 时仅前 2 条出现");
    }

    std::cout << "== M3 指标导出：空输入 ==\n";
    {
        const std::string empty = exporters::toPrometheusText({}, {});
        check(empty.empty(), "池空 + 慢 SQL 空 → 输出空字符串");

        // 仅池空
        PoolMetricsEvent evt;
        evt.pools.push_back(makePool("ds-app"));
        const std::string onlyPool = exporters::toPrometheusText(evt, {});
        check(!onlyPool.empty(), "仅池非空时输出非空");

        // 仅慢 SQL 空
        std::vector<SlowSqlStats> slow;
        slow.push_back(makeSlow("ds-app", 42));
        const std::string onlySlow = exporters::toPrometheusText({}, slow);
        check(!onlySlow.empty(), "仅慢 SQL 非空时输出非空");
    }

    std::cout << "== M3 指标导出：自定义 prefix ==\n";
    {
        PoolMetricsEvent evt;
        evt.pools.push_back(makePool("ds-app"));
        const std::string text = exporters::toPrometheusText(evt, {}, "myapp");
        check(contains(text, "# HELP myapp_pool_connections "),
              "prefix=myapp 时指标名前缀被替换");
        check(!contains(text, "dbmw_"),
              "prefix=myapp 时输出不应出现 dbmw_ 前缀");
    }

    // 收尾：清状态。
    common::Observability::setPoolMetricsObserver({});
    common::Observability::setPoolMetricsCollector({});

    std::cout << "\n----------------------------------------\n";
    std::cout << "通过 " << g_passed << " 项，失败 " << g_failed << " 项\n";
    return g_failed == 0 ? 0 : 1;
}
