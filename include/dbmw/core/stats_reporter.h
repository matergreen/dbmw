#ifndef DBMW_CORE_STATS_REPORTER_H
#define DBMW_CORE_STATS_REPORTER_H

#include "dbmw/common/observer.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/core/database_manager.h"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>


namespace dbmw::core {
    // 定时统计报告。
    //
    // 指标只留在进程内存里，等于没有可观测性——要么能被采集，要么能事后追溯。
    // 这里用一条后台线程按固定周期采样（连接池 + 慢 SQL），追加写入日志文件，
    // 不占用业务线程，也不要求调用方主动来取。
    //
    // 采集器由 DatabaseManager 注入（回调形式），因此本类不持有管理器引用，
    // 生命周期上只要保证 stop() 先于被采集对象销毁即可。
    class StatsReporter {
    public:
        using PoolStatsCollector = std::function<std::vector<NamedPoolStats>()>;

        StatsReporter() = default;

        // 析构时停止后台线程：线程一旦比被采集对象活得久，回调就会踩到悬垂引用。
        ~StatsReporter();

        StatsReporter(const StatsReporter &) = delete;

        StatsReporter &operator=(const StatsReporter &) = delete;

        // 按配置启动后台线程；已在运行时先停止再按新配置重启（热加载场景）。
        // cfg.enabled 为 false 时只停不启。
        void start(const config::StatsReportConfig &cfg, PoolStatsCollector collector);

        // 停止后台线程并等待其退出。可重复调用；未启动时是空操作。
        void stop();

        [[nodiscard]] bool running() const;

    private:
        void run();

        // 采集一轮并落盘。内部自行兜住所有异常：统计失败绝不能影响业务。
        void writeOnce();

        [[nodiscard]] std::string renderText(
            const std::chrono::system_clock::time_point &now,
            const std::vector<NamedPoolStats> &pools,
            const std::vector<common::SlowSqlStats> &slowSql) const;

        [[nodiscard]] std::string renderJson(
            const std::chrono::system_clock::time_point &now,
            const std::vector<NamedPoolStats> &pools,
            const std::vector<common::SlowSqlStats> &slowSql) const;

        mutable std::mutex mtx_;
        std::condition_variable cv_;
        std::thread thread_;
        bool running_ = false;
        // cfg_ / collector_ 只在 start() 中写入，且 start() 前必然已 stop()（线程已 join），
        // 因此后台线程无锁读取是安全的。
        config::StatsReportConfig cfg_;
        PoolStatsCollector collector_;
    };
} // namespace dbmw::core


#endif // DBMW_CORE_STATS_REPORTER_H
