#ifndef DBMW_EXPORTERS_PROMETHEUS_H
#define DBMW_EXPORTERS_PROMETHEUS_H

#include "dbmw/common/observer.h"

#include <cstddef>
#include <string>
#include <vector>

namespace dbmw::exporters {
    // Prometheus 文本格式（0.0.4）渲染器：纯字符串拼接，不引入第三方依赖。
    //
    // 设计取舍（M3 §5.4）：
    //   - fingerprint 作为 label 是高基数来源，时序库会被撑爆。调用方通过
    //     maxFingerprintLabels 控制最多导出几条慢 SQL（按传入顺序截断）。
    //   - 所有 label 值都按 Prometheus 转义规则处理（\n \" \\）。
    //   - histogram 的 bucket 上界来自 SlowSqlStats.histogramBucketsMs（已归一化）。
    //   - 不内置 HTTP 服务（设计稿 §5.2）：暴露 /metrics 端口是应用或 sidecar 职责。
    //
    // 参数：
    //   pools      池指标事件，可空（空数组输出空字符串仍合法）。
    //   slow       慢 SQL 聚合列表，可空。
    //   prefix     指标名前缀，默认 "dbmw"。
    //   maxFingerprintLabels 慢 SQL 的 fingerprint label 上限，0 表示全部导出（不推荐）。
    std::string toPrometheusText(const common::PoolMetricsEvent &pools,
                                 const std::vector<common::SlowSqlStats> &slow,
                                 const std::string &prefix = "dbmw",
                                 std::size_t maxFingerprintLabels = 0);
} // namespace dbmw::exporters

#endif // DBMW_EXPORTERS_PROMETHEUS_H
