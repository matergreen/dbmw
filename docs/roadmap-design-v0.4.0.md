# dbmw 能力扩展路线设计（v0.4.0）

> **状态**：设计稿（评审用）。尚未实现，任何代码改动需经本文档评审通过。
> **基线版本**：v0.3.0（HEAD）
> **前置文档**：`docs/async-design-v0.2.0.md`、`docs/prepared_generated_streaming_design.md`、`docs/guide.md`
> **读者**：dbmw 维护者。假定已熟悉闸门分离、双层缓存、游标绑定模型等既有语义。

---

## §1 定位、范围与现状核对

### §1.1 定位前提（决定所有设计的边界）

`CMakeLists.txt:114` 为 `add_library(dbmw STATIC ...)`——**dbmw 是进程内嵌入式库，不是 MySQL 协议 proxy**。

由此推出三条硬边界，后续所有设计都不得越界：

1. **不做协议伪装与多语言支持**。业务进程必须链接 C++，价值只能来自"进程内的连接治理 + SQL 治理 + 可观测"。
2. **不引入动态模块加载**。C++ 静态库里做 `dlopen` 成本与风险不成比例，扩展点一律为**编译期注册 + 运行期开关**。
3. **头文件即 ABI**。新增公开 API 要考虑既有调用点的源码兼容，能用默认参数/新重载解决的，不改既有签名。

### §1.2 目标与非目标

**目标**：以最小侵入的方式，让 dbmw 具备"能持续长出新能力"的结构，并补齐生产级可观测与若干场景刚需。

**非目标**（明确排除，避免范围蔓延）：

| 非目标 | 理由 |
|---|---|
| MySQL 协议 proxy / 多语言 | 与 §1.1 定位冲突，等于开新项目 |
| ORM / 对象映射 | 属应用层，中间件应止步于 SQL 与结果集 |
| SQL 方言自动翻译 | 需完整解析器，且与"诚实跨驱动"语义冲突 |
| **分库分表 / 分片** | **永不实现**，见 §11 |
| **分布式事务**（XA / 2PC / Saga） | **永不实现**，见 §11 |
| **跨数据源一致性保证** | **永不实现**，见 §11 |

> 后三项不是"暂缓"，而是维护者于 2026-09-06 决策的**永久排除**（§11）。建议在 `guide.md` 的"非目标"
> 章节同步声明，比"悄悄不支持"对用户负责。

### §1.3 现状核对表

本节是对前一轮能力评估的**复核结果**。前一轮基于关键词检索给出的判断有两处偏差，此处如实修正——设计必须建立在真实基线上。

| 能力 | 前轮判断 | 核对结果 | 真实缺口 |
|---|---|---|---|
| 扩展点 SPI / 拦截器 | 缺失 | ✅ **确属缺失**（全仓无 `Interceptor`/`Plugin`/`Hook`） | 从零设计 |
| 读后写一致性 | "bug 级缺口" | ❌ **判断有误**：已完整实现 | 见下条 |
| — 组级时间戳方案 | — | ✅ 已有：`readTarget()` 用 `lastWriteNs_` + `readAfterWrite_` 判定（`database_manager.cpp:640`），`markWrite()` 维护时间戳（`:664`） | 无 |
| — 配置项 | — | ✅ 已有：`DataSourceGroupConfig::read_after_write_ms`（`datasource_config.h:278`），**默认 0 = 关闭** | 默认值偏保守 + 粒度粗 |
| 分布式追踪 | 缺失 | ✅ 确属缺失：`OperationEvent` 无上下文字段 | 加字段 + 透传 |
| 指标导出接口 | 缺失 | ❌ **部分误判**：已有 `Observability::setObserver()` 进程级回调 + `slowSqlStats()` / `recentSlowSql()` / `allPoolStats()` 快照 + `StatsReportConfig` 定时落盘 | 缺池指标回调、缺 trace 关联、缺标准格式适配器 |
| 影子库路由 | 缺失 | ✅ 确属缺失 | 从零设计（可复用组路由框架） |
| 动态数据源 | 缺失 | ✅ 确属缺失（现仅有 `init()` 整体热替换） | 从零设计 |
| 结果脱敏 | 缺失 | ✅ 确属缺失（现有仅为"驱动错误脱敏"，防日志泄密） | 从零设计 |
| 幂等声明 | 缺失 | ✅ 已落地（v0.4.0 M5）：`Idempotency` 三态声明叠加到同步 `resolveWriteAttempts` + 异步 `maxAttempts` | 无 |
| 分库分表 / 分片（含轻量分片） | 缺失 | ✅ 确属缺失 | **永不实现**（§11，维护者 2026-09-06 决策） |
| 分布式事务（XA / 2PC / Saga） | 缺失 | ✅ 确属缺失 | **永不实现**（§11） |
| 跨数据源一致性保证 | 缺失 | ✅ 确属缺失 | **永不实现**（§11） |

**两处修正的具体说明**：

1. **读后写一致性不是 bug，是"默认关闭 + 粒度粗"**。功能实现完整且正确（写过之后 Δ 窗口内读固定走主库）。真正的问题是：
   - `read_after_write_ms` **默认 0**，用户配了副本却忘了配该项就会静默读到旧数据——这是**默认值与文档引导**问题，不是功能缺失；
   - 判定的 `lastWriteNs_` 是**数据源级**而非会话级：任意一次写之后，Δ 窗口内**所有**读都走主库。高写场景下副本会被架空，属**性能**问题而非正确性。
   因此该项从 P0 降为 **M8 增强**，并附带一个零成本的默认值/文档改进建议。

2. **指标导出的机制已存在**。`Observability::setObserver()` 是完整的进程级回调，`OperationEvent` 已含耗时、指纹、慢标记、状态码。缺的是**上下文字段**、**池指标导出口**和**标准格式适配器**，不是从零建机制。

### §1.4 架构不变量（改动前必读，硬约束）

以下不变量来自既有实现，任何本章设计不得违反：

| # | 不变量 | 约束含义 |
|---|---|---|
| I1 | **闸门分离** | `preGate(sql, type)` = 审计 + 限流，**只在一次业务调用最外层执行一次**；组转发叶子走 `*Ungated`，不可再调闸门（重复审计刷成倍告警、重复扣令牌让 QPS 腰斩） |
| I2 | **会话闸门只限流** | `gateSession()` 不审计（入口无 SQL）；会话内语句由 `Session::auditStatement` 逐条审 |
| I3 | **缓存 key** | `cacheKey` = 原始 SQL + `\x1e` + 参数个数 + 每参数(类型标记 + 长度前缀值)。不可用结构模板（不同取值撞 key），不可用 `valueToString`（丢类型信息） |
| I4 | **事务内无故障转移/写缓冲** | 回调未必幂等，重放 = 重复写入 |
| I5 | **`DBMW_ENABLE_*` 编译期开关** | 驱动专属符号必须包在对应宏内，禁用构建下不得引用未定义符号 |
| I6 | **驱动 `.cpp` 辅助函数顺序** | 多段匿名命名空间，辅助函数必须在**首个使用点之前** |
| I7 | **游标资源护栏** | `cursorBudgetAcquire` 原子 CAS；`OwnsHandle` / `BorrowedInSession` 两种绑定 |
| I8 | **生命周期顺序** | `shutdown` 必须先停 `writeBuffers_` 与 `statsReporter_`，再停连接池 |

**新增不变量**（本设计引入，需一并遵守）：

| # | 不变量 | 理由 |
|---|---|---|
| I9 | **SPI 埋点遵循与闸门相同的分层** | 只在最外层埋点；`*Ungated` 与叶子转发路径不重复触发。否则一次调用触发 N 次回调，指标与脱敏都会错乱 |
| I10 | **脱敏结果不进查询缓存** | 缓存存的是原始结果，脱敏是租户/角色相关的视图；把脱敏结果缓存会跨用户泄漏 |
| I11 | **SPI 回调不得抛异常** | 与 `Observability::emit` 一致：回调异常被中间件吞掉，绝不向业务传播 |
| I12 | **影子库路径不进写缓冲** | 影子流量若入写缓冲，恢复后会把压测数据补发到生产库 |

---

## §2 总体架构与落地顺序

### §2.1 依赖结构

九项能力不是并列关系。`M1（SPI）`是其余多数能力的承载层，必须先落地。

```
                    ┌─────────────────────────┐
                    │  M1 SQL 扩展点（SPI）    │  ← 地基：所有横切能力的承载层
                    │  SqlContext + Scope      │
                    └───────────┬─────────────┘
                                │ 承载
            ┌───────────┬───────┴───────┬───────────┬───────────┐
            │           │               │           │           │
       ┌────▼───┐  ┌────▼────┐    ┌─────▼────┐ ┌───▼─────┐ ┌───▼────┐
       │ M2     │  │ M6      │    │ M7       │ │ M8      │ │ M3     │
       │ 追踪   │  │ 影子库  │    │ 脱敏     │ │ 读后写  │ │ 指标   │
       │ 上下文 │  │         │    │          │ │ 增强    │ │ 导出   │
       └────┬───┘  └────┬────┘    └──────────┘ └─────────┘ └───┬────┘
            │           │ 依赖组路由框架                        │
            │ 提供 traceId / tenantId 维度                      │ 依赖 M2
            └──────────────────────────────────────────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │  M4 动态数据源            │  独立：改 DatabaseManager
                    │  M5 幂等声明              │  独立：改重试决策入参
                    └─────────────────────────┘
```

### §2.2 落地顺序与理由

| 序 | 能力 | 为什么排这个位置 |
|---|---|---|
| M1 | SPI 扩展点 | **地基**。没有它，后面每个能力都要动 `database_manager.cpp`（1300+ 行，承载 I1–I4 不变量）。有了它，后续能力降级为"挂插件" |
| M2 | 追踪上下文 | 成本最低、价值最直接。中间件是所有 SQL 的必经之路，加一个上下文槽即可让日志/慢 SQL/指标全部可串联 |
| M3 | 指标导出增强 | 依赖 M2（指标需要 trace 维度）；机制已有，只是补字段与出口 |
| M4 | 动态数据源 | ✅ **已落地**（2026-09）——`addDataSource/removeDataSource/addGroup/removeGroup` + facade 透传 + 79 项单测。独立于 SPI，改动面集中在 `DatabaseManager::init` 的替换逻辑，宜早做以暴露生命周期问题 |
| M5 | 幂等声明 | ✅ **已落地**（2026-09）——`context.h` 三态枚举 + 同步 `resolveWriteAttempts` + 异步 `maxAttempts` 接入 + 19 项单测。独立小改动，把重试语义从"引擎猜"变成"调用方声明" |
| M6 | 影子库路由 | ✅ **已落地**（2026-09）——`DataSourceGroupConfig::shadow` 字段 + 同步 `readTarget/writeTargets/dispatchWrite/cacheEligible` 影子分支 + 异步 `entryCtx` 透传 + `resolveShadows` 4 项校验 + 39 项单测。复用 M1 SPI `onRoute` 触发，硬守住 I12（影子不进写缓冲）/ I10（影子不进缓存）；同步异步决策同源 |
| M7 | 结果脱敏 | ✅ **已落地**（2026-09）——普通查询使用 `afterExecution` + `mutableRows()`，`queryEach`/游标使用逐行 `onRow`；同步缓存只保存驱动原始结果，异步 `cacheStore` 拒绝 `transformed` 结果，缓存命中仍执行改写。完全依赖 SPI，规则由业务 MaskingInterceptor 提供 |
| M8 | 读后写增强 | ✅ **已落地**（2026-09）——`SqlContext.wroteInThisRequest` 会话级粘性读 + `pinRequestWrite()` 在 leaf 写成功后置位栈顶 + `readTarget` 第 1 级判定 + `ConfigLoader` 副本+零窗口 WARN。3 级优先级：wIRT > 时间戳窗口 > 副本轮询；26 项单测覆盖同步 / 异步 / 帧隔离 / 影子 / 幂等正交 |
| M9 | 观测延展（M6/M7 收尾）| ✅ **已落地**（2026-09）——`OperationEvent` 增加 `shadow` / `transformed` 两字段；emitSql 读栈顶 `SqlContext.shadow`，observeSql 透传 `ResultSet*` 读 `result->transformed`，同步异步共用 emitSql 一份注入路径；22 项单测 + 全库 543 项 0 失败。设计 §8.5 提到的"指标可观测性"补齐 |

> 原「轻量分片」已随 §11 的永久排除决策移除；当前新增的 M9 仅做观测层延展，不引入分片。当前路线共 **9 项**（M1–M9）。

### §2.3 里程碑切分建议

| 里程碑 | 内容 | 出口标准 |
|---|---|---|
| **A** | M1 + M2 | SPI 可注册生效；`OperationEvent` 带 traceId；同步 + 异步路径均透传 |
| **B** | M3 + M4 + M5 | 池指标可导出；运行时增删数据源可用；幂等声明影响重试 |
| **C** | M6 + M7 | 影子流量隔离已通过（I12 单测覆盖）；脱敏结果确认不进缓存（I10 单测覆盖，§9.4 缓存命中修复已验证）|
| **D** | M8 | 会话级读后写已落地（26 项单测）；副本 + 零窗口 WARN 已生效（stderr 验证通过）|
| **D+** | M9 | 观测层延展已落地：`OperationEvent.shadow` / `.transformed` 由 emitSql 一份注入路径同时覆盖同步 / 异步；22 项单测 + 全库 543 项 0 失败 |

---

## §3 M1：SQL 扩展点（SPI）

### §3.1 目标与非目标

**目标**：提供一组稳定的回调埋点，让横切能力（追踪、脱敏、路由决策、审计增强、指标）以**插件**形式接入，无需修改核心执行路径。

**非目标**：
- 不做动态模块加载（`dlopen`）——静态库定位下成本收益不成比例
- 不做 SQL 改写器——改写需要解析器，且会破坏"诚实跨驱动"
- 不提供"拦截器改变 SQL 文本"的能力（v1 只给只读视图 + 结果改写 + 路由建议）

### §3.2 请求上下文 `SqlContext`

**为什么用 `thread_local` 而非函数参数**：给所有 `query`/`execute` 重载加上下文参数会让 API 面翻倍，且每个调用点都要改。改用 `thread_local` + RAII 后，业务只在请求入口包一层，中间件任意深度都能读到。这与既有的 `Session::AuditContext` 隐式传递风格一致。

**为什么用栈而非单值**：支持嵌套——拦截器内部可能再发一条 SQL（如审计写日志表），需要独立的上下文帧。

```cpp
// include/dbmw/common/context.h（新增）
namespace dbmw::common {

// 幂等性声明（M5）。
//
// 用枚举而非 bool：布尔的 false 无法区分"未声明"与"显式声明非幂等"。
// Unspecified 保证**不声明即保持现状**——引入该特性不改变任何既有行为。
enum class Idempotency {
    Unspecified,   // 未声明：走既有推断逻辑（retry_writes 配置 + Single/Multi）
    Idempotent,    // 声明幂等：允许在连接类错误上自动重试（含写）
    NonIdempotent  // 声明非幂等：任何情况下都不重试写
};

// 一次业务调用的可观测与路由元数据。
//
// 设计为**值类型 + 快照语义**：跨线程传递时按值拷贝，避免共享可变状态。
struct SqlContext {
    std::string traceId;   // 分布式追踪 ID（W3C traceparent 的 trace-id，32 hex）
    std::string spanId;    // 当前跨度 ID（16 hex）
    std::string tenantId;  // 多租户标识（M7 脱敏、M4 动态数据源可用）
    // 通用路由覆盖：非空时路由层按名选择数据源，忽略组路由与读后写判定。
    // 服务于影子库（M6）与业务自定义数据源选择；**不内置任何分片能力**（§11）。
    std::string targetDataSource;
    bool shadow = false;             // 影子库标记（M6）
    bool wroteInThisRequest = false; // 会话级写标记（M8，由中间件在写成功后置位）
    Idempotency idempotency = Idempotency::Unspecified; // M5

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
class ContextScope {
public:
    explicit ContextScope(SqlContext ctx);
    ~ContextScope();
    ContextScope(const ContextScope &) = delete;
    ContextScope &operator=(const ContextScope &) = delete;

    // 当前生效上下文（栈顶）。栈空时返回进程级默认实例的引用（非悬垂）。
    [[nodiscard]] static const SqlContext &current() noexcept;
};

// 可选：为当前帧生成一个子跨度（16 hex），用于把"一次业务请求"拆成多条 SQL 跨度。
// 不改变 traceId。无当前帧时返回空串。
std::string nextSpanId();

} // namespace dbmw::common
```

**异步路径的桥接（关键设计点）**：

worker 线程**不继承**调用线程的 `thread_local`。因此 `AsyncEngine` 在投递任务时把 `SqlContext` **按值快照**进内部 Op，执行前由引擎在 worker 线程上"安装"一个 `ContextScope`，执行结束自动弹出。

```cpp
// src/async/async_engine.cpp（示意）
// op->ctx 为提交时快照的 SqlContext
common::ContextScope scope(op->ctx);   // worker 线程上安装
// ... 执行语句 ...
```

这样同步与异步路径对 SPI 与可观测组件呈现**完全一致**的上下文语义，避免"异步调用没有 traceId"这类经典缺陷。

### §3.3 执行视图 `ExecutionView`

```cpp
// include/dbmw/core/interceptor.h（新增）
namespace dbmw::core {

// 一次语句执行的只读/可写视图。
//
// 生命周期仅限回调期间——实现**不得**保存该结构体或其指针。
struct ExecutionView {
    const std::string &dataSource;      // 逻辑数据源名（组名或叶子名）
    const std::string &sql;             // 原始 SQL（占位符形态）
    common::OperationType type;         // Query / Execute / Batch / Select ...
    const common::Params *params;       // 绑定参数；无参数时为 nullptr
    common::ResultSet *result;          // 结果集；非查询或失败时为 nullptr
    std::int64_t affected = 0;          // 影响行数
    std::chrono::microseconds duration{0}; // 仅 afterExecution / onCompletion 有效
    common::Status status;              // 仅 afterExecution / onCompletion 有效
    bool cached = false;                // 是否命中查询缓存（缓存命中不产生驱动往返）
    common::SqlContext &ctx;            // 可写：路由期可置 shadow / targetDataSource
};

class ISqlInterceptor {
public:
    virtual ~ISqlInterceptor() = default;

    // 路由决策：preGate 之后、选节点之前。
    // 可写 ctx（如置 shadow = true 把流量引到影子库，或填 targetDataSource）。
    virtual void onRoute(const std::string &dataSource, const std::string &sql,
                         common::OperationType type, common::SqlContext &ctx) = 0;

    // 执行前：SQL 与参数已定，尚未下发驱动。
    // 返回非 ok 则中止执行——该语义为"拦截"，不触发重试、不入写缓冲。
    virtual common::Status beforeExecution(const ExecutionView &view) = 0;

    // 执行后：可改写 view.result（M7 脱敏即在此实现）。
    virtual void afterExecution(const ExecutionView &view) = 0;

    // 收尾：无论成功失败恰好调用一次（由 RAII 保证）。
    virtual void onCompletion(const ExecutionView &view) = 0;
};

} // namespace dbmw::core
```

### §3.4 注册与开关

注册走 API（编译期），配置只做开关与顺序：

```cpp
// include/dbmw/dbmw.h（新增）
class DBMW {
public:
    // 注册全局拦截器。顺序即调用顺序。
    // 线程安全：内部持 mutex；建议在 init() 之前调用。
    static void addInterceptor(std::shared_ptr<core::ISqlInterceptor> interceptor);
    static void clearInterceptors();
};
```

```json
{
  "interceptors": {
    "enabled": true
  }
}
```

`enabled=false` 时热路径**不抢锁**（与 `query_cache.enabled` 一致的原子读模式）——默认关闭的功能不该给每条语句留锁代价。

### §3.5 埋点位置（与 I9 对齐，最关键的一节）

埋点**必须**遵循与闸门完全相同的分层规则，否则一次调用会触发 N 次回调。

| 路径 | 埋点位置 | 说明 |
|---|---|---|
| `DataSource::query` / `execute`（公开入口） | **在 `preGate` 之后**、路由之前调 `onRoute`；驱动调用前后调 `before/after` | 与闸门同层，只埋一次 |
| `DataSource::*Ungated` | **不埋** | 组转发给叶子时已由入口埋过（I1 / I9） |
| 叶子被直接调用（非组转发） | 由该叶子的公开入口埋 | 单数据源直连场景 |
| `Session` 内语句 | 与 `auditStatement` **同一位置**逐条埋 | 入口处无 SQL，只能在会话内埋（与 I2 同理） |
| 事务 `begin/commit/rollback` | 埋，但 `result` 为 nullptr | 让追踪能覆盖事务边界 |
| 游标 `openCursor` / `fetch` | `openCursor` 埋一次；`fetch` **不埋** | 一次打开可能取 N 次，逐次埋会淹没指标 |

**`onCompletion` 的 RAII 保证**：

```cpp
// src/core/interceptor.cpp（新增）
// RAII：析构时必然调用一次且仅一次 onCompletion，即使语句抛异常。
class InterceptorGuard {
public:
    InterceptorGuard(const ExecutionView &view) : view_(view) {}
    ~InterceptorGuard() {
        for (auto &it : interceptors()) {
            try { it->onCompletion(view_); } catch (...) { /* I11：吞掉 */ }
        }
    }
    // ...
};
```

### §3.6 落地步骤

1. 新增 `include/dbmw/common/context.h` + `src/common/context.cpp`（`SqlContext` / `ContextScope` / `nextSpanId`）。
2. 新增 `include/dbmw/core/interceptor.h` + `src/core/interceptor.cpp`（`ExecutionView` / `ISqlInterceptor` / `InterceptorGuard` / 注册表）。
3. `src/core/database_manager.cpp`：在 §3.5 表格列出的位置插入埋点调用。
4. `src/async/async_engine.cpp`：Op 增加 `ctx` 字段，worker 上安装 `ContextScope`（§3.2）。
5. `src/config/config_loader.cpp` + `datasource_config.h`：新增 `InterceptorsConfig { bool enabled = true; }`。
6. `src/dbmw.cpp`：`addInterceptor` / `clearInterceptors` 实现，并在 `shutdown` 里清理。

### §3.7 风险

| 风险 | 应对 |
|---|---|
| 回调变慢拖垮热路径 | 文档明确要求回调内不得做 IO；提供 `enabled` 一键关闭；建议 M3 统计回调自身耗时 |
| 回调抛异常 | I11：全部 `try/catch(...)` 吞掉，绝不传播到业务 |
| 嵌套调用导致上下文栈错乱 | `ContextScope` 严格 RAII；栈深度设上限（如 64），超出则忽略新帧并告警 |
| 拦截器内再发 SQL 造成无限递归 | `ExecutionView` 增加 `depth` 字段；`depth > 1` 时不再触发 SPI |

---

## §4 M2：追踪上下文（traceId）

### §4.1 缺口

`OperationEvent`（`observer.h:30`）已有 `dataSource` / `type` / `duration` / `status` / `rowCount` / `sqlTemplate` / `sqlFingerprint` / `slow`，但**没有任何字段能把一次业务请求里的多条 SQL 串起来**。

中间件是所有 SQL 的必经之路，天然是全链路最好的埋点位置。缺失关联字段意味着：跨服务出问题时，日志、慢 SQL、池指标彼此孤立，排查只能靠时间戳猜。

### §4.2 设计

**Step 1 — `OperationEvent` 增加上下文字段**（追加在末尾，保持既有字段偏移不变）：

```cpp
struct OperationEvent {
    // ... 既有字段 ...
    std::string traceId;   // 追加：可为
    std::string spanId;    // 追加：当前语句跨度
    std::string tenantId;  // 追加
};
```

> 追加而非插入，与 `ErrorCode::Overloaded`、`OperationType::Select` 的既有做法一致（保持枚举/布局向后兼容）。

**Step 2 — `emitSql` 自动填充**：从 `common::ContextScope::current()` 读取并填入事件。业务只需在请求入口包一个 `ContextScope`，无需逐条传参。

**Step 3 — W3C `traceparent` 兼容**（可选，建议支持）：

提供两个自由函数，便于与 OpenTelemetry / 现有链路系统对接：

```cpp
namespace dbmw::common {
    // 解析 "00-<32hex traceId>-<16hex spanId>-<2hex flags>"
    // 解析失败返回 false（不抛异常，格式错误不应影响业务）。
    bool parseTraceparent(const std::string &header, SqlContext &out);

    // 生成当前上下文的 traceparent 字符串，供下游 HTTP 调用透传。
    std::string formatTraceparent(const SqlContext &ctx);
}
```

**Step 4 — 慢 SQL 记录同样携带**：`SlowSqlRecord`（`observer.h:59`）增加 `traceId`，让"哪次请求触发了这条慢 SQL"可追溯。

### §4.3 落地步骤

1. `observer.h`：`OperationEvent` / `SlowSqlRecord` 追加字段。
2. `src/common/context.cpp`：实现 `parseTraceparent` / `formatTraceparent`。
3. `src/common/observer.cpp`：`emitSql` 里从 `ContextScope::current()` 填充。
4. `src/core/database_manager.cpp`：确保 `emitSql` 调用点已处于正确的 `ContextScope` 内（组转发路径尤其要检查）。

### §4.4 风险

| 风险 | 应对 |
|---|---|
| 异步路径丢失上下文 | §3.2 的 Op 快照 + worker 安装方案已覆盖；需专门加一个跨线程测试 |
| traceId 泄漏到日志造成合规问题 | traceId 本身不含业务数据；`SqlLogConfig` 的脱敏开关不受影响 |
| 事件体积增大 | 仅在上下文非空时填充；空上下文不产生额外分配 |

---

## §5 M3：指标导出增强

### §5.1 缺口（复核后）

机制已存在：`Observability::setObserver()` 进程级回调、`slowSqlStats()` / `recentSlowSql()` 快照、`allPoolStats()` 快照、`StatsReportConfig` 定时落盘。

真实缺口有三：

1. **池指标没有导出口**——`allPoolStats()` 是拉取式快照，没有推送式回调，外部采集器只能轮询。
2. **缺少标准格式适配器**——`StatsReportConfig` 只落 `text` / `json` 文件，接不进 Prometheus。
3. **指标维度缺 trace / tenant**（依赖 M2）。

### §5.2 设计

**Step 1 — 池指标推送回调**：

```cpp
// observer.h 新增
struct PoolMetricsEvent {
    std::chrono::system_clock::time_point timestamp;
    std::vector<core::NamedPoolStats> pools;  // 复用既有结构，不新造类型
};

class Observability {
public:
    // ... 既有 ...
    static void setPoolMetricsObserver(std::function<void(const PoolMetricsEvent &)> observer);
    // 立即采样一次（供采集器按需拉取，不必等周期）。
    static PoolMetricsEvent samplePoolMetrics();
};
```

由 `StatsReporter` 的既有周期驱动，复用 `StatsReportConfig::interval_ms`，不新增线程。

**Step 2 — Prometheus 文本适配器**（独立模块，可选编译）：

```cpp
namespace dbmw::exporters {
    // 把池指标 + 慢 SQL 统计渲染为 Prometheus 文本格式（0.0.4）。
    // 纯字符串拼接，不引入第三方依赖。
    std::string toPrometheusText(const common::PoolMetricsEvent &pools,
                                 const std::vector<common::SlowSqlStats> &slow,
                                 const std::string &namespacePrefix = "dbmw");
}
```

指标命名建议：`dbmw_pool_connections{data_source="x",state="in_use"}`、`dbmw_slow_sql_count{data_source="x",fingerprint="y"}`、`dbmw_slow_sql_duration_ms_bucket{...}`。

> **不内置 HTTP 服务**。暴露 `/metrics` 端口是应用或 sidecar 的职责，库只负责产出文本。这是嵌入式库的边界自觉。

### §5.3 落地步骤

1. `observer.h` / `observer.cpp`：`PoolMetricsEvent` + `setPoolMetricsObserver` + `samplePoolMetrics`。
2. `src/core/database_manager.cpp`：`StatsReporter` 采集回调里同时驱动池指标观察者。
3. 新增 `include/dbmw/exporters/prometheus.h` + `src/exporters/prometheus.cpp`。
4. `CMakeLists.txt`：纳管新文件（注意 I5，该文件无驱动依赖，无需宏包裹）。

### §5.4 风险

| 风险 | 应对 |
|---|---|
| 周期采样开销 | 复用既有线程与周期；`include_pool=false` 时不采样 |
| 高基数标签（fingerprint）撑爆时序库 | 文档明确警告：fingerprint 作为标签需谨慎，建议只导出 Top-N |

---

## §6 M4：动态数据源（运行时增删）

### §6.1 缺口

现仅有 `DatabaseManager::init()`，语义是**整体热替换**：在临时容器里建好全部池与数据源，再一次性换掉。这带来两个限制：

1. **无法增删单个数据源**——多租户 SaaS 按需建连、灰度切库等场景做不到；
2. 每次变更都要重建**全部**池，即使只改了一个数据源（`init` 的替换宽限期 `replacementGrace` 也是全局的）。

### §6.2 目标与非目标

**目标**：支持运行期注册/注销单个数据源与组，且不影响其他数据源的在途请求。

**非目标**：
- 不做"修改既有数据源参数"（改连接串等于换池，直接走 remove + add 更清晰）
- 不做跨进程的配置同步

### §6.3 设计

```cpp
class DatabaseManager {
public:
    // ... 既有 ...

    // 运行期注册单个数据源（建池 + 启动心跳 + 建 DataSource）。
    // name 已存在时返回 AlreadyExists，不做覆盖——覆盖语义会让人误以为
    // "改配置"能生效，实际旧池还在服务在途请求。要改就先 remove 再 add。
    common::Status addDataSource(const config::DataSourceConfig &cfg);

    // 运行期注销单个数据源。grace 为等待在途连接归还的宽限期，
    // 超期仍未归还的连接被强制关闭（与 shutdown 的 grace 语义一致）。
    // 返回 NotFound 表示不存在。
    common::Status removeDataSource(const std::string &name,
                                    std::chrono::milliseconds grace =
                                        std::chrono::milliseconds(5000));

    // 运行期注册/注销读写组。
    common::Status addGroup(const config::DataSourceGroupConfig &cfg);
    common::Status removeGroup(const std::string &name,
                               std::chrono::milliseconds grace =
                                   std::chrono::milliseconds(5000));
};
```

**引用完整性校验**（关键）：

`addGroup` 必须校验 `primary` 与 `replicas` 引用的数据源都已存在；`removeDataSource` 必须校验没有组在引用它。否则会出现组持有已注销数据源的悬垂 `shared_ptr`——池虽被 `shared_ptr` 保活不至于崩，但该数据源已停止心跳，连接会静默腐化。

```cpp
// 校验失败返回 InvalidArgument，并指明冲突的组名，便于运维定位。
[[nodiscard]] common::Status validateGroupRefs(const config::DataSourceGroupConfig &cfg) const;
```

**并发模型**：

`DatabaseManager` 已有 `mutable std::mutex mtx_` 保护 `pools_` / `datasources_`。增删操作在该锁下完成容器修改，但**建池与排空旧池必须在锁外**——持锁做网络 IO 会阻塞所有其他数据源的 `getDataSource`。

流程：
1. 锁内：检查重名 / 引用完整性；
2. 锁外：建新池、启动心跳；
3. 锁内：插入容器；
4. 注销时锁外：等待 grace 排空，再停心跳、关池。

### §6.4 落地步骤

1. `database_manager.h`：新增 4 个方法声明 + `validateGroupRefs`。
2. `src/core/database_manager.cpp`：把 `init()` 里"建单个池 + 建单个 DataSource"的逻辑抽成可复用私有函数，供 `init` 与 `addDataSource` 共用。
3. 注销路径复用 `init` 的旧池排空逻辑（`replacementGrace` 相关代码）。
4. `src/dbmw.cpp`：门面暴露。

### §6.5 风险

| 风险 | 应对 |
|---|---|
| 持锁做 IO 阻塞全局 | §6.3 的锁外建池/排空 |
| 组引用已注销数据源 | `validateGroupRefs` 双向校验 |
| 与 `init()` 并发调用 | 文档要求二者不可并发；实现上加断言或返回 `Busy` |
| 写缓冲持有叶子强引用 | 注销叶子前必须先停该组的 `writeBuffers_`（I8） |

### §6.5 实施状态（v0.4.0）

✅ **已落地**（2026-09）：

- `DatabaseManager::addDataSource(cfg, opts)` / `removeDataSource(name, grace)` / `addGroup(cfg, opts)` / `removeGroup(name, grace)` 全部实现；
- `DBMW` facade 透传同名 4 个静态方法；
- 引用完整性 / 重名拒绝 / ack 校验 / 并发安全均按 §6.3 落地；
- `addDataSource` 默认 pool 参数（min=1, max=32, 30s 借出超时），覆盖常见场景；如需定制在 `init()` 阶段按 `GlobalConfig::pool` 配齐，运行时路径走默认；
- `tests/dbmw_dynamic_test.cpp` 16 个场景 / 79 项断言全过。

**遗留与偏差**：

- §6.3 提到的 "与 `init()` 并发调用" 现通过 `mtx_` 串行化（不加 `Busy` 返回值，亦不抛异常）——调用方遵守文档约束即可。已在 `addDataSource` 注释里写明"reload 进行中触发增删可能让 reload 的 oldWriteBuffers 集合错过刚加进来的缓冲"。
- `GroupOptions` 仅暴露 `ack` 标志与限流器；`retry` / `circuit_breaker` / `cursor` 等仍按 `init()` 阶段全局下发；后续若需要"每组独立"语义，把字段从 `GlobalConfig` 挪到 `GroupOptions` 即可（API 设计留口）。
- `removeDataSource` 未支持的 `grace=0` 路径会"立即返回 + 池被标记 closed"，但池里在途连接的析构由借出者 RAII 兜底——已通过 M4.16 验证。

---

## §7 M5：幂等声明

### §7.1 缺口

当前重试由引擎按**语句类型**推断：异步引擎有 `Single`（`queryEach`/`executeBatch`，不重试，因副作用/流不可重放）与 `Multi` 两档（`async_engine.cpp:205`）。同步路径由 `RetryConfig::retry_writes`（默认 `false`）控制。

问题在于：**引擎只能猜，调用方才知道**。一条 `UPDATE ... SET balance = balance - 100` 从语句类型看是 `Execute`，但它是非幂等的；而 `UPDATE ... SET status = 'paid' WHERE id = ?` 是幂等的。现在要么一律不重试（浪费可用性），要么一律重试（有重复扣款风险）。

### §7.2 设计

`SqlContext` 已在 §3.2 定义 `Idempotency idempotency` 字段，语义为**三态**而非布尔（枚举定义见 §3.2）。

> 为什么用枚举而非 `bool idempotent`：布尔的 `false` 无法区分"未声明"与"显式声明非幂等"。这是关键——
> `Unspecified` 保证**不声明即保持现状**，绝不会因引入该特性而改变既有行为。

**决策优先级**（高→低）：

1. `NonIdempotent` → 绝不重试写（`Unspecified` 下的 `retry_writes` 配置被覆盖为 false）；
2. `Idempotent` → 允许重试写，即使 `retry_writes=false`；
3. `Unspecified` → 走既有逻辑（`retry_writes` 配置 + `Single`/`Multi`）。

**与 I4 的关系**：本特性只影响"是否重试"，**不影响**"事务内不做故障转移/写缓冲"这条不变量。事务内的语句一律不重试，与本声明无关。

### §7.3 落地步骤

1. `context.h`：`Idempotency` 枚举，`SqlContext` 把 `bool idempotent` 改为枚举。
2. `src/core/database_manager.cpp`：`DataSource::beforeAttempt` / `afterAttempt` 读取上下文决定重试。
3. `src/async/async_engine.cpp`：Op 的 `Single`/`Multi` 判定结合声明。
4. 文档：说明三态语义与"不声明即保持现状"。

### §7.4 风险

| 风险 | 应对 |
|---|---|
| 调用方误声明幂等导致重复写 | 文档强调"声明即承诺"；建议配合唯一键/去重表使用 |
| 与 `retry_writes` 配置冲突 | §7.2 的优先级表已明确，声明覆盖配置 |

### §7.5 实施状态（v0.4.0）

**已落地**（提交于 `dev` 分支，M5 单独 commit）：

1. `include/dbmw/common/context.h`：`Idempotency` 枚举与 `SqlContext::idempotency` 字段**此前已在 M1/M2 一并就位**（SPI 上下文槽），M5 无需改此文件。
2. `src/core/database_manager.cpp`：新增文件级助手 `resolveWriteAttempts(const config::RetryConfig&)`，4 处写重试循环（`executeUngated` 的无参 / 带参 / 生成键无参 / 生成键带参，行 1246/1302/1363/1414）由
   `retry_.retry_writes ? std::max(1, retry_.max_attempts) : 1`
   改为 `resolveWriteAttempts(retry_)`。读路径（query / cursor）的 `std::max(1, retry_.max_attempts)` **不动**——声明只影响写。
3. `src/async/async_engine.cpp`：`maxAttempts` 增加 `common::Idempotency idem` 形参，在 `WriteRetries` 分支按同一张优先级表覆盖；调用点（行 648）传入 `ctx->entryCtx.idempotency`（submit 时刻栈顶 ctx 快照）。`Single`（queryEach/executeBatch）与 `ReadRetries` 不受声明影响。

**决策优先级（与 §7.2 一致）**：

```
NonIdempotent 且 WriteRetries → attempts = 1          // 绝不重试写
Idempotent    且 WriteRetries → attempts = max(1, max_attempts)  // 覆盖 retry_writes=false
其余                        → 既有逻辑（retry_writes + Single/Multi）
```

**测试**：`tests/dbmw_idempotency_test.cpp`（19 项断言 / 9 场景全过）——覆盖三态 × 同步/异步，以及"读路径不受声明影响"与"非可重试错误照旧不重试"两个边界。

**偏差与遗留**：

- 写缓冲补发线程（后台重放）不携带调用方 `ContextScope`，重放时 `idempotency` 为 `Unspecified`，走既有 `retry_.retry_writes`——与 M4 写缓冲"不重试、重放=重复写入"的语义一致，无需为补发线程快照上下文。
- `queryEach` / `executeBatch`（`RetryMode::Single`）即便声明 `Idempotent` 也不重试——副作用/流不可重放，属设计内约束。

---

## §8 M6：影子库路由

### §8.1 场景价值

生产流量回放做全链路压测：把写流量引到影子库，验证新版本在真实负载下的表现，而不污染生产数据。

### §8.2 设计

**配置**：在组配置里声明影子目标。

```cpp
struct DataSourceGroupConfig {
    // ... 既有 ...
    std::string shadow;  // 影子数据源名；为空表示未配置影子库
};
```

**触发**：由 SPI 的 `onRoute` 置位 `ctx.shadow = true`（§3.3），路由层据此把整组流量切换到影子数据源。走 SPI 而非独立开关，好处是触发条件完全由业务定义（按 `tenantId`、按 HTTP 头、按灰度比例……）。

**路由层改动**：`DataSource::readTarget()` / `writeTargets()` 在 `ctx.shadow` 为真时返回影子数据源。

### §8.3 硬约束（I12）

**影子流量绝不进写缓冲**。写缓冲是为"主库短暂不可用"设计的补发机制；影子流量若入队，主库恢复后会把压测数据补发到生产库——这是数据污染事故。

```cpp
// DataSource::dispatchWrite 的影子分支
if (common::ContextScope::current().shadow) {
    // 影子写不进写缓冲：缓冲补发会把压测数据写回生产库（I12）。
    // 影子库不可用就直接失败，让压测停掉，而不是悄悄降级。
    return attempt(shadowTarget);
}
```

同理，**影子读不进查询缓存**（会污染真实租户的缓存）。

### §8.4 落地步骤

1. `datasource_config.h`：`DataSourceGroupConfig::shadow`。
2. `src/config/config_loader.cpp` 解析 + 校验（影子源必须存在且**不是**组内成员，否则等于自己影自己）。
3. `database_manager.h/.cpp`：`readTarget` / `writeTargets` 增加影子分支；`dispatchWrite` 影子短路。
4. 缓存路径：`cacheEligible()` 在 `ctx.shadow` 时返回 false。

### §8.5 风险

| 风险 | 应对 |
|---|---|
| **误配把生产流量引到影子库**（最严重） | 影子源不得是组内成员（配置校验）；建议影子数据源在配置里显式标记 `"shadow_only": true`，双保险 |
| 影子库写满影响压测 | 属运维职责，文档提示容量规划 |
| 忘记关影子标记导致长期写入影子库 | 建议在 `OperationEvent` 里带出 `shadow` 标记，指标可观测 |

### §8.6 实施状态（v0.4.0）

✅ **已落地**（2026-09，提交于 `dev` 分支 M6 单独 commit）：

1. **`include/dbmw/config/datasource_config.h`**：`DataSourceGroupConfig::shadow`（`std::string`）字段；
2. **`include/dbmw/core/database_manager.h`**：`DataSource::shadowName_` 声明字段 + `shadow_` 强引用（运行时解析填充）；
3. **`src/config/config_loader.cpp`：从 JSON groups[].shadow 读入配置并保留到 DataSourceGroupConfig；
4. **`src/core/database_manager.cpp`**：
   - `DataSource::readTarget()` 在 `ctx.shadow` 为真且 `shadow_` 有效时返回 `shadow_`；
   - `DataSource::writeTargets()` 在影子模式下返回 `{shadow_}`；
   - `DataSource::dispatchWrite()` 影子短路：attempt 影子，失败直接返回错误，**绝不**构造 buffered lambda；
   - `cacheEligible()` 在影子模式下返回 false（影子读不进缓存，I10）；
   - `buildSingleDataSourceGroup()`：填入 `shadowName_`；
   - 新增 `DatabaseManager::resolveShadows()`（私有）：把每组 `shadowName_` 解析为 `DataSource` 强引用；任一校验失败立刻返回 `ConfigError`，错误消息指明冲突的组 / 字段；
   - `init()` 与 `addGroup()` 在所有叶子 DataSource 建好之后、对外可见之前调用 `resolveShadows()`；
   - 校验规则：
     - 影子源必须存在（已在 `addDataSource` 注册）；
     - 影子源**不得**是本组主（自影自己）；
     - 影子源**不得**是本组副本（自影自己）；
     - 影子源**不得**与任何组名同名（避免引用歧义）；
5. **`src/async/async_engine.cpp`**：
   - `submitStatementOp()` 在调用线程跑 `core::detail::runOnRoute()` 拿到带 `shadow`/`targetDataSource` 的 `routeCtx`，围绕后续路由计算 `ContextScope scope(routeCtx)`，使影子决策对 readTarget/writeTargets 可见；
   - `routeCtx.shadow` 同时作为 `StatementOp::entryCtx.idempotency` 之外的另一个字段喂给 worker——I12（写缓冲守卫：`transferable && !ctx->entryCtx.shadow`）、I10（缓存守卫：`!ctx->entryCtx.shadow` 才 cacheStore）、afterAttempt 守卫都读到一致的 shadow 标志；
   - 影子模式下不追加主回退候选——影子不可用就让压测停掉，不悄悄降级污染生产；
6. **`tests/dbmw_shadow_test.cpp`**（新增）：39 项断言 / 10 个场景——同步读/写、影子写不进缓冲、影子读不进缓存、四个校验失败分支、异步 execute、异步 query，全部 0 失败。

**关键不变量（已守住）**：

| 不变量 | 当前实现 |
| --- | --- |
| I12 影子流量绝不进写缓冲 | `dispatchWrite` 影子短路 + 异步 `!ctx->entryCtx.shadow` 守卫双层 |
| I10 影子结果绝不进缓存 | `cacheEligible()/cacheStore` 影子守卫 |
| 校验失败时回滚 | `init()` 在 `resolveShadows()` 失败时把 `datasources_/pools_/writeBuffers_/heartbeat_` 全部还原，避免新加的数据源残留 |
| 同步与异步决策一致 | 异步显式 push `routeCtx` 入栈，与同步读写 `ContextScope::current()` 读到同一份 `shadow` |

**未做（刻意）**：

- `OperationEvent` 增加 `shadow` 字段供指标可观测（设计 §8.5 风险行提到，列入下一波 M7/M8 同步落地）。
- `dispatchWrite` 影子短路仍调用一次 `attempt(shadow_)`——按既有"重试 on shadow_" 行为复用 retry 框架，影子失败可重试由调用方决策（影子模式默认 `retry_writes=true` 不变）。

---

## §9 M7：结果脱敏

### §9.1 缺口

现有脱敏是**驱动错误脱敏**（`DataSourceConfig::redact`，防日志泄密），不是**结果集脱敏**。合规场景（手机号、身份证、银行卡）需要在结果集返回前按角色/租户做掩码。

### §9.2 设计

复用 SPI 的 `afterExecution`（§3.3）改写 `view.result`。中间件**不内置任何脱敏规则**——规则是业务/合规概念，内置等于替用户做合规决策。

```cpp
// 由业务实现的拦截器片段（示意，非 dbmw 内置）
class MaskingInterceptor : public core::ISqlInterceptor {
public:
    void afterExecution(const core::ExecutionView &view) override {
        if (!view.result) return;
        for (auto &row : view.result->rows) {
            for (auto &cell : row.values) {
                if (shouldMask(view, cell)) maskInPlace(cell);
            }
        }
        // 关键：告知中间件该结果已被改写。
        view.result->transformed = true;
    }
    // ... 其余三个回调空实现 ...
};
```

**I10 的落地机制**：`ResultSet` 增加 `bool transformed = false` 标记。

```cpp
// DataSource::cacheStore 的守卫
if (!rs.transformed) cacheStore(key, rs);   // 脱敏结果不进缓存（I10）
```

理由：缓存存的是原始结果，脱敏是角色/租户相关的视图。把脱敏结果写进缓存，下一个不同权限的用户会读到上一个用户的视图——**跨用户数据泄漏**。

### §9.3 落地步骤

1. `common/types.h`：`ResultSet` 追加 `transformed` 字段（默认 false）。
2. `src/core/database_manager.cpp`：`cacheStore` 前检查该标记。
3. 文档：给出 `MaskingInterceptor` 示例（放 `examples/`）。
4. 测试：断言脱敏后结果不入缓存、缓存命中路径不重复脱敏。

### §9.4 风险

| 风险 | 应对 |
|---|---|
| **脱敏结果进缓存导致跨用户泄漏**（最严重） | I10 + `transformed` 标记硬拦截；专项测试 |
| 逐行逐列回调的性能开销 | 文档建议按列索引而非全表扫描；提供 `enabled` 开关 |
| 缓存命中路径漏脱敏 | 缓存命中时同样要调 `afterExecution`（缓存层视为一种"执行结果"） |

### §9.5 实施状态（v0.4.0）

✅ **已落地**（2026-09，提交于 `dev` 分支 M7 单独 commit）：

1. **`include/dbmw/common/types.h`**：`ResultSet` 追加 `bool transformed = false`（默认 false，I10 标记位）。业务 `ISqlInterceptor::afterExecution` 改写 `view.result` 后置位。
2. **`src/core/database_manager.cpp`**：
   - `queryUngated` 在顶层 `afterExecution` 前保存驱动原始结果，返回副本随后按请求改写；
   - `DataSource::cacheStore` 同样守卫（与已有 shadow 守卫并列）；
   - 同步路径的 runWithInterceptors 天然包住 queryUngated → 缓存命中也会发 afterExecution（无需额外改动）。
3. **`src/async/async_engine.cpp`**：
   - 异步 `submitStatementOp` 缓存命中分支手动构造 `ExecutionView` 并调一次 `core::detail::runAfterExecution(view)`（§9.4 风险行：缓存命中漏脱敏修复）；
   - 写缓冲守卫、缓存守卫保留 `entryCtx.shadow` 守卫；新增逻辑不破坏 M6 的 I12 防御。
4. **`tests/dbmw_redaction_test.cpp`**：覆盖普通/异步/缓存命中与流式逐行改写——
   - 同步缓存保存原始结果，driver 调 1 次；每次返回都恰好执行一次 afterExecution；
   - 未脱敏读照常进缓存（I10 不误伤）；
   - `queryEach` 在业务回调前执行一次 `onRow`；
   - 异步路径 I10 同源（驱动 2 次 + afterExecution ≥2）；
   - **§9.4 修复验证**：异步缓存命中仍调 afterExecution（驱动仍 1 次 + 拦截器调 1 次 + transformed 标记位翻转）。

**关键不变量（已守住）**：

| 不变量 | 实现位置 |
| --- | --- |
| **I10 脱敏结果绝不进缓存** | 同步路径先缓存原始结果再改写返回副本；异步 `cacheStore` 拒绝 `rows.transformed` |
| 缓存命中路径仍走 afterExecution | 同步由 runWithInterceptors 包住；异步在 submit 时手动调 `detail::runAfterExecution` |
| 业务改写 cell 的方式灵活 | 普通结果用 `mutableRows()`，流式结果用 `onRow`；业务自行实现 MaskingInterceptor |

**未做（刻意）**：

- `ResultSet::transformRow(name, fn)` 可变入口（M7 不扩 API 面；当前 `rows()` / `row.data()` 返回 `const &`，业务需自行缓存原始索引或拷贝后再写）。
- `MaskingInterceptor` 示例代码放 `examples/`（设计稿 §9.3 步骤 3）——本 commit 没新增 examples 目录，建议下一波 M8 同步落地。
- `OperationEvent` 增加 `transformed` 标记供指标可观测（与 M6 §8.5 的 shadow 字段一起列入下一波统一落地）——✅ **M9 已落地**。

---

## §10 M8：读后写一致性增强

### §10.1 现状（复核后）

功能**已完整实现且正确**（§1.3）：`read_after_write_ms > 0` 时，`DataSource::readTarget()` 在 `now - lastWriteNs_ < Δ` 窗口内固定返回 `primary_`（`database_manager.cpp:640`）。

真实问题有二：

1. **默认 0 = 关闭**：用户配了副本却忘了配该项，就会静默读到旧数据。
2. **粒度是数据源级而非会话级**：任意一次写之后，Δ 窗口内**所有**读都走主库。高写场景下副本被架空。

### §10.2 设计

**改动 A（零风险，建议优先做）**：把默认值与文档引导改掉。

- `read_after_write_ms` 默认值维持 `0`（改默认值会改变既有行为，需谨慎），但：
  - `config_loader.cpp` 在检测到"组配置了副本 且 `read_after_write_ms == 0`"时，**打一条 WARN** 提示该组存在陈旧读风险；
  - `guide.md` / `config/datasources.json.example` 的副本示例**必须**带上 `read_after_write_ms`（现在示例里是有的，:459/:493，需确认默认值不为 0）。

**改动 B（会话级粒度，依赖 M1）**：

引入"会话粘性"：若调用方在一次请求内先写后读，则**该请求**内的后续读走主库，而不影响其他请求。

```cpp
struct SqlContext {
    // ... 既有字段 ...
    bool wroteInThisRequest = false;  // 由中间件在同请求内的写之后置位
};
```

实现：`DataSource::execute` 成功后，若 `ContextScope` 有活动帧，置位 `ctx.wroteInThisRequest = true`；`readTarget()` 除既有的时间戳判定外，额外检查该标志。

> 注意：`SqlContext` 是**值语义 + 线程局部**（§3.2），置位只影响当前线程的当前帧，天然做到"按请求隔离"。异步路径由 Op 快照携带，语义一致。

**判定优先级**：`wroteInThisRequest` > 时间戳窗口 > 副本轮询。

### §10.3 落地步骤

1. `context.h`：`SqlContext::wroteInThisRequest`。
2. `database_manager.cpp`：`execute` 成功后置位；`readTarget()` 增加判定分支。
3. `config_loader.cpp`：副本 + 零窗口的 WARN 提示。
4. 文档：说明两级判定的关系与调优建议。

### §10.4 风险

| 风险 | 应对 |
|---|---|
| 会话级置位过度保守（一次写 → 整请求读主库） | 这本身是正确的；如需更细，业务可自行清除标志 |
| 长请求窗口内副本闲置 | 文档建议：长任务拆分为独立上下文帧 |

---

## §11 永不实现清单（维护者决策）

以下三项由维护者于 2026-09-06 明确决策为**永不实现**，已从 v0.4.0 路线整体移除——不再排期、不再评估、不再讨论。

| 能力 | 决策 | 理由 |
|---|---|---|
| **分库分表 / 分片**（含轻量分片键路由） | 永不实现 | 需要 SQL 解析器 + 结果归并引擎 + 分布式主键 + 跨片事务，等于把 dbmw 从"访问层"重写为 ShardingSphere；与 §1.1 的嵌入式库定位冲突 |
| **分布式事务**（XA / 2PC / Saga） | 永不实现 | XA 在三驱动上语义不一致、性能代价高；Saga 需业务编写补偿逻辑，本质是业务框架而非中间件职责 |
| **跨数据源一致性保证** | 永不实现 | 同上。dbmw 不提供任何跨数据源的原子性语义 |

### §11.1 对使用者的指引

- dbmw 的定位是**进程内数据库访问层**：每个数据源与读写组彼此独立，互不承诺一致性。
- 需要分片或跨库事务的场景，应在**应用层**解决——自行按业务键选择数据源、自行编排 Saga 与幂等重试；
  或在 dbmw 之外引入专门的分库分表中间件。
- 建议把本表同步进 `docs/guide.md` 的"非目标"章节，避免用户预期错位。

### §11.2 与 SPI 的关系

M1 的 `ISqlInterceptor::onRoute` 回调**仍然保留**——它是一个**通用**路由扩展点，业务可在其上实现自己的数据源选择逻辑（灰度、多租户分库、按业务键路由等）。

但 dbmw **本身**不提供任何内置分片能力，也不为分片场景提供路由表、结果归并或事务支持。换言之：

> SPI 给的是"能自己选数据源"的能力，不是"帮你分片"的能力。

因此 `SqlContext` 中**不提供** `shardKey` 字段；保留 `targetDataSource` 作为通用路由覆盖（空 = 走默认路由），它同样服务于影子库与多租户场景。

---

## §12 里程碑、测试策略与验收

### §12.1 里程碑

见 §2.3。

### §12.2 测试策略

既有测试规模：core 147 checks / async 67 / coro 35，外加三个真实库集成测试（PG / MySQL / ODBC，靠 `DBMW_TEST_*` 环境变量）。新增能力按同样风格补测。

| 能力 | 测试要点（必须覆盖） |
|---|---|
| M1 SPI | ① 回调按 §3.5 分层**恰好触发一次**（组转发路径不得重复）；② 回调抛异常不影响业务；③ 嵌套上下文正确还原；④ 异步路径上下文正确透传 |
| M2 追踪 | ① `OperationEvent` 带 traceId；② `traceparent` 解析容错（格式错误不崩）；③ 跨线程不丢失 |
| M3 指标 | ① 池指标回调周期触发；② Prometheus 文本格式可被解析 |
| M4 动态源 | ① 增删后其他数据源在途请求不受影响；② 引用完整性校验（删被引用的源应失败）；③ 并发安全 |
| M5 幂等 | ① `Unspecified` 行为与改动前**完全一致**；② `NonIdempotent` 覆盖 `retry_writes=true`；③ `Idempotent` 覆盖 `false` |
| M6 影子库 | ① 影子写**绝不**入写缓冲（I12）；② 影子读不进缓存；③ 影子源不得是组内成员 |
| M7 脱敏 | ① 脱敏结果**不进缓存**（I10）；② 缓存命中路径仍会脱敏 |
| M8 读后写 | ① 会话级标志只影响当前请求；② 副本 + 零窗口的配置 WARN |

**跨切面回归**：每个里程碑结束必须跑全量既有测试——SPI 埋点侵入了核心执行路径，最容易破坏的是 I1（闸门分离）。

### §12.3 验收门槛

- 全量既有测试通过（core / async / coro）
- CI 7 位矩阵全绿（含 2 个 `coro=ON` 构建位）
- 新增能力在三个真实驱动下均有集成测试或明确的降级说明

---

## §13 风险登记（汇总）

| # | 风险 | 等级 | 涉及 | 应对 |
|---|---|---|---|---|
| R1 | SPI 埋点破坏 I1 闸门分离，导致审计/限流重复 | 高 | M1 | §3.5 分层表 + 专项计数测试 |
| R2 | 脱敏结果进查询缓存 → 跨用户数据泄漏 | 高 | M7 | I10 + `transformed` 硬拦截 |
| R3 | 影子流量入写缓冲 → 压测数据写回生产库 | 高 | M6 | I12 短路 + 配置校验 |
| R4 | 异步路径上下文丢失（thread_local 不跨线程） | 中 | M1/M2 | Op 快照 + worker 安装 + 跨线程测试 |
| R5 | 拦截器回调拖慢热路径 | 中 | M1 | `enabled` 原子开关 + 文档禁 IO |
| R6 | 动态数据源持锁做 IO 阻塞全局 | 中 | M4 | 锁外建池/排空 |
| R8 | 幂等误声明导致重复写 | 中 | M5 | 三态默认 `Unspecified` + 文档强调 |
| R9 | 回调异常传播到业务 | 低 | M1 | I11 全量 try/catch |

> 原 R7「轻量分片被误用为完整分片」随 M9 移除而消除（§11）。

---

## §14 待拍板决策点

以下四项需要维护者明确决策，方可进入实现阶段：

| # | 决策点 | 选项 | 建议 |
|---|---|---|---|
| D1 | SPI 是否允许改写 SQL 文本 | A 只读（v1）/ B 可改 | **A**。改写需解析器且破坏"诚实跨驱动"；如确有需求，后续按独立提案评估 |
| D2 | `read_after_write_ms` 默认值是否改为非 0 | A 维持 0 + WARN / B 改为 1000 | **A**。改默认值会改变既有行为；WARN 提示已能覆盖绝大多数误配 |
| D3 | 是否内置 Prometheus HTTP 端点 | A 只产出文本 / B 内置端点 | **A**。暴露端口是应用/ sidecar 职责，库只产文本（嵌入式库边界） |

> 原 D4（分片范围）已由维护者于 2026-09-06 决策为**永不实现**，见 §11，不再作为待拍板项。

---

## 附录 A：新增文件清单

| 文件 | 内容 | 依赖 |
|---|---|---|
| `include/dbmw/common/context.h` | `SqlContext` / `ContextScope` / `Idempotency` / traceparent | 无 |
| `src/common/context.cpp` | 实现 | 无 |
| `include/dbmw/core/interceptor.h` | `ExecutionView` / `ISqlInterceptor` | context.h |
| `src/core/interceptor.cpp` | `InterceptorGuard` / 注册表 | 无 |
| `include/dbmw/exporters/prometheus.h` | Prometheus 文本适配器 | observer.h |
| `src/exporters/prometheus.cpp` | 实现 | 无 |

## 附录 B：既有文件改动清单

| 文件 | 改动 |
|---|---|
| `include/dbmw/common/types.h` | `ResultSet::transformed` |
| `include/dbmw/common/observer.h` | `OperationEvent` / `SlowSqlRecord` 追加上下文字段；`PoolMetricsEvent` |
| `src/common/observer.cpp` | `emitSql` 填充上下文；池指标观察者 |
| `include/dbmw/config/datasource_config.h` | `InterceptorsConfig`；`DataSourceGroupConfig::shadow` |
| `src/config/config_loader.cpp` | 新配置解析 + 副本零窗口 WARN |
| `include/dbmw/core/database_manager.h` | SPI 埋点相关私有方法；`addDataSource` / `removeDataSource` / `addGroup` / `removeGroup` |
| `src/core/database_manager.cpp` | **改动最大**：埋点插入、影子路由分支、脱敏缓存守卫、会话级读后写、路由层 `targetDataSource` |
| `src/async/async_engine.cpp` | Op 携带 `SqlContext`；worker 安装 `ContextScope`；幂等声明接入重试判定 |
| `src/dbmw.cpp` | `addInterceptor`；门面暴露新方法 |
| `CMakeLists.txt` | 纳管新文件 |
