#ifndef DBMW_COMMON_OBSERVER_H
#define DBMW_COMMON_OBSERVER_H

#include "dbmw/common/types.h"
#include "dbmw/config/datasource_config.h"

#include <chrono>
#include <functional>
#include <string>
#include <vector>

namespace dbmw::common {
    enum class OperationType {
        Query,
        Execute,
        Begin,
        Commit,
        Rollback,
        Cancel,
        Stream,
        Batch,
        Savepoint,
        // 游标（openCursor 打开的读）。与 Query 的区别不在"读/写"，而在消费方式：
        // 游标本就是分批、有界地消费大结果集，因此审计对它豁免 require_limit_select
        //（强制 LIMIT 会废掉"全量游标扫描"这一正当用法）。追加在末尾以保持既有枚举值不变。
        Select
    };

    // 默认不携带 SQL 文本和绑定参数，避免观测链路意外泄漏业务数据。
    //
    // 追踪字段（traceId / spanId）由 emitSql 从当前 common::ContextScope 自动填充：
    //   - traceId 来自业务入口解析的链路 ID（建议 32 hex，W3C traceparent），
    //     也可不填（中间件不强制）；
    //   - spanId 优先取调用方栈顶 spanId（沿用同一跨度）；为空时按语句自动生成
    //     16 hex 子跨度——便于在调用方不感知的情况下，按"次业务请求 = 多条 SQL"
    //     的颗粒度对齐链路。
    struct OperationEvent {
        std::string dataSource;
        OperationType type = OperationType::Query;
        std::chrono::microseconds duration{0};
        Status status;
        std::uint64_t rowCount = 0;
        std::string sqlTemplate;
        std::string renderedSql;
        std::uint64_t sqlFingerprint = 0;
        // M2 追踪上下文：见上。空串代表"未声明"。
        std::string traceId;
        std::string spanId;
        bool slow = false;
    };

    struct SlowSqlStats {
        std::uint64_t fingerprint = 0;
        std::string dataSource;
        OperationType type = OperationType::Query;
        std::string sqlTemplate;
        std::uint64_t count = 0;
        std::uint64_t errorCount = 0;
        std::uint64_t timeoutCount = 0;
        std::chrono::microseconds totalDuration{0};
        std::chrono::microseconds minDuration{0};
        std::chrono::microseconds maxDuration{0};
        std::chrono::system_clock::time_point firstSeen;
        std::chrono::system_clock::time_point lastSeen;
        std::vector<int> histogramBucketsMs;
        std::vector<std::uint64_t> histogram;
    };

    struct SlowSqlRecord {
        std::chrono::system_clock::time_point timestamp;
        std::string dataSource;
        OperationType type = OperationType::Query;
        std::string sqlTemplate;
        std::string renderedSql;
        std::uint64_t fingerprint = 0;
        std::chrono::microseconds duration{0};
        ErrorCode errorCode = ErrorCode::Ok;
        std::string sqlState;
        // M2 追踪上下文：与 OperationEvent 同源，便于按 trace 检索慢 SQL
        // 窗口里的具体样本（聚合 SlowSqlStats 不需要 trace，按 fingerprint
        // 取即可）。
        std::string traceId;
        std::string spanId;
    };

    using OperationObserver = std::function<void(const OperationEvent &)>;
    using SqlRenderer = std::function<Status(const SqlRenderOptions &, std::string &)>;

    class Observability {
    public:
        Observability() = delete;

        // 进程级观察器。传入空函数可关闭；回调抛出的异常会被中间件吞掉。
        static void setObserver(OperationObserver observer);
        static void emit(const OperationEvent &event) noexcept;

        static void configure(const config::ObservabilityConfig &config);
        static void emitSql(OperationEvent event, const std::string &sql,
                            const SqlRenderer &renderer = {}) noexcept;
        static std::vector<SlowSqlStats> slowSqlStats(
            std::size_t limit = 100, const std::string &dataSource = {});
        static std::vector<SlowSqlRecord> recentSlowSql(
            std::size_t limit = 100, const std::string &dataSource = {});
        static void clearSlowSqlStats();
    };
} // namespace dbmw::common

#endif // DBMW_COMMON_OBSERVER_H
