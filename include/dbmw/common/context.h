#ifndef DBMW_COMMON_CONTEXT_H
#define DBMW_COMMON_CONTEXT_H

#include <cstddef>
#include <string>
#include <vector>

namespace dbmw::common {

    // 幂等性声明（M5）。
    //
    // 用枚举而非 bool：布尔的 false 无法区分"未声明"与"显式声明非幂等"。
    // Unspecified 保证**不声明即保持现状**——引入该特性不改变任何既有行为。
    //
    // 这是 SPI 路线（M1）的客户字段之一，但定义位于 SqlContext 同一头里，
    // 是因为 Idempotency 与上下文共存：调用方只声明一次，在当前线程上下文里
    // 透传到所有路径（同步/异步/事务），而无需每条语句显式传参。
    enum class Idempotency {
        Unspecified,   // 未声明：走既有推断逻辑（retry_writes + Single/Multi）
        Idempotent,    // 声明幂等：允许在连接类错误上自动重试（含写）
        NonIdempotent  // 声明非幂等：任何情况下都不重试写
    };

    // 一次业务调用的可观测与路由元数据。
    //
    // 设计为**值类型 + 快照语义**：跨线程传递时按值拷贝，避免共享可变状态。
    // 既不持有任何外部指针，也不引用任何容器内元素——`ContextScope::current()`
    // 返回的引用是栈顶元素的引用，但语义上"一次业务请求"期间值不会变。
    //
    // 所有字段都允许空值（业务可能只关心 spanId，或只填 traceId）。`empty()`
    // 用于"上下文是否值得序列化进 OperationEvent"这类快速判定——空上下文不
    // 会带来额外字符串分配。
    struct SqlContext {
        // 分布式追踪 ID；建议 32 hex（W3C traceparent 的 trace-id）。
        // 业务从入口（比如 HTTP header）解析后填入，dbmw 在 emitSql 时透传到
        // OperationEvent 与 SlowSqlRecord，中间不再改动。
        std::string traceId;
        // 当前跨度 ID；建议 16 hex。中间件按语句自动生成子跨度，调用方无需关心。
        std::string spanId;
        // 多租户标识（M7 脱敏路由 + M4 动态数据源可选维度）。
        std::string tenantId;
        // 通用路由覆盖：非空时路由层按名选择数据源（已存在即用，否则回 DefaultError）。
        // 服务于影子库（M6）与业务自定义数据源选择；**不内置任何分片能力**
        // （v0.4 路线 §11 永久排除）。
        std::string targetDataSource;
        // 影子库标记（M6）；为 true 时把流量路由到同名的影子数据源。
        bool shadow = false;
        // 会话级写标记（M8）：中间件在写成功返回后置位；当前请求后续读
        // 命中 read_after_write_ms 窗口内的"应读主"判定——避免业务再传参。
        // 业务**不应**手动置位。
        bool wroteInThisRequest = false;
        // M5 幂等三态。
        Idempotency idempotency = Idempotency::Unspecified;

        [[nodiscard]] bool empty() const {
            return traceId.empty() && spanId.empty() && tenantId.empty()
                   && targetDataSource.empty() && !shadow && !wroteInThisRequest
                   && idempotency == Idempotency::Unspecified;
        }
    };

    // 上下文作用域：构造压栈，析构出栈。
    //
    // 异常安全基于 RAII——即使语句抛异常，析构仍会还原上一帧，
    // 不会出现"上下文泄漏到下一个请求"。
    //
    // 用法：业务在请求入口构造一次；中间件任意深度都能读到
    //   common::ContextScope scope({.traceId = "...", .tenantId = "..."});
    //   DBMW::query(...);   // 内部任意层都能拿 current()
    //
    // 栈深上限（kMaxDepth）防御拦截器内部再发 SQL 时的无限递归。超限后构造
    // ContextScope 仍合法，但**不再压栈**——current() 维持上一栈顶。
    // 该限频只触发一次 stderr 警告，避免被刷屏。
    class ContextScope {
    public:
        explicit ContextScope(SqlContext ctx);
        ~ContextScope();
        ContextScope(const ContextScope &) = delete;
        ContextScope &operator=(const ContextScope &) = delete;
        ContextScope(ContextScope &&) = delete;
        ContextScope &operator=(ContextScope &&) = delete;

        // 当前生效上下文（栈顶）；栈空时返回 default() 的引用（非悬垂）。
        // 永远是非空引用——只是栈空时全部字段都是默认状态。
        [[nodiscard]] static const SqlContext &current() noexcept;

        // 栈当前深度（用于调试/诊断；勿在业务热路径依赖此值）。
        [[nodiscard]] static std::size_t depth() noexcept;

        // 内部访问点：栈与默认实例。私有—不可对用户暴露。
        static std::vector<SqlContext> &stack();
        static const SqlContext &defaultInstance();

        // 进程级栈深上限。拦截器自身调 SQL 被卡在这里。
        static constexpr std::size_t kMaxDepth = 64;

    private:
        // LIFO 严格对称：构造函数 push 成功时置 true；溢出 noop 路径下
        // 仍能保证析构安全（LIFO 不被破坏）。
        bool entered_;
    };

    // 为当前帧生成一个子跨度（16 hex），用于把"一次业务请求"拆成
    // "多条 SQL 各自一个 span"。不会自动写回 current()——调用方按需赋值。
    // 无当前帧时返回空串。
    std::string nextSpanId();

    // W3C traceparent 解析与生成（M2）。
    //
    // 解析："00-<32hex traceId>-<16hex spanId>-<2hex flags>"。
    // 解析失败返回 false（不抛异常——header 格式错误不应影响业务）。
    // 解析成功填入 out 的 traceId/spanId，flags 不暴露（保留位）。
    //
    // 生成：ctx.traceId 为空时返回空串；否则按标准格式输出。
    bool parseTraceparent(const std::string &header, SqlContext &out);

    std::string formatTraceparent(const SqlContext &ctx);

} // namespace dbmw::common

#endif // DBMW_COMMON_CONTEXT_H
