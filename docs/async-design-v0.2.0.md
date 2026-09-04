# dbmw v0.2.0 异步 API 详细设计方案

> 状态：设计稿（评审用）
> 范围：v0.2.0 全量异步能力——回调 / future / C++20 协程三形态，覆盖现有同步门面的全部操作
> 原则：**同步 API 一字不动**；治理链路（审计/限流/熔断/路由/缓存/可观测）只过一次、不绕过

---

## 0. 结论先行

1. **执行模型**：dbmw 内置 **Executor 线程池**统一承载所有驱动的阻塞 IO（MySQL/PG/ODBC 均为阻塞客户端 API）。调用线程提交异步操作后立即返回，永不被 DB 调用卡住。
2. **三层交付**，v0.2.0 一次到位：
   - **回调 API**（热路径，零 future 开销）；
   - **future API**（便利形态，`std::future<T>`）；
   - **协程 API**（C++20 opt-in 编译开关 `DBMW_ENABLE_ASYNC_CORO`，核心库保持 C++17）。
3. **连接池异步借出**：`borrowAsync()` + 等待者队列 + 归还时直接交接，worker 不再为"池空"阻塞空转。同步 `borrow()` 热路径**零改动**。
4. **治理前置**：审计 / 限流 / 熔断快检在**调用线程** fail-fast；只有"借连接 + 执行"上 worker。
5. **两个必须的重构**（不做则异步正确性不成立）：
   - `*Ungated` 拆出**单次尝试**函数——现有重试退避是 `sleep_for`，worker 睡眠会拖垮整个执行器；
   - 池 `State` 增加**异步等待者队列**，归还路径支持直接交接。
6. **取消与超时**复用既有机制：`Session::cancel()`（跨线程安全）+ 延迟任务（不为一语句开看门狗线程）。

---

## 1. 目标与非目标

### 1.1 目标

| # | 目标 | 验收标准 |
|---|---|---|
| G1 | 同步门面每个公开操作都有异步对应物 | query / execute（含 Params、GeneratedKeys）/ queryEach / executeBatch / transaction / withSession 全覆盖 |
| G2 | 调用线程永不阻塞在 DB IO 上 | 提交即返回；池耗尽/过载/熔断全部快速失败 |
| G3 | 三种编程形态 | 回调、future、协程（opt-in） |
| G4 | 治理语义与同步完全一致 | 审计、限流、熔断、读写路由、read-after-write、缓存、慢 SQL、观察者事件，逐项对齐 |
| G5 | 生命周期安全 | shutdown / reload 期间在途操作优雅收尾，不悬垂、不崩溃 |
| G6 | 跨平台 | gcc / clang / MSVC CI 全绿（沿用 v0.1.0 的 5 平台矩阵） |

### 1.2 非目标（明确不做，防止范围蔓延）

| 项 | 理由 |
|---|---|
| 驱动层原生非阻塞（libpq `PQsendQuery`、ODBC 异步属性） | v0.3+ 优化项。本设计的 `IExecutor` 抽象使将来可透明替换，见 §11 |
| 协程原生的 `AsyncSession`（挂起式事务，语句间挂起、连接保持钉住） | 协程被放弃（abandon）时的事务回滚语义非常微妙，v0.2.0 用"SessionFn 跑在 worker 上"的等价形态，见 §7 |
| 异步游标（openCursor/fetch） | 游标是长生命周期交互对象，异步化收益低、状态机复杂，v0.3 再评估 |
| 跨库分布式事务 | 与 v0.1.0 的边界声明一致，永不改变 |

---

## 2. 现状约束（设计输入，均已从代码核实）

| 事实 | 对异步设计的影响 |
|---|---|
| `DBMW` 门面全静态、同步、`Status` + 出参 | 异步门面同构：静态 + 值返回结果体 |
| `DataSource` 持 `weak_ptr<ConnectionPool>`，组路由/熔断/缓存全在 `*Ungated` 私有链 | 异步引擎必须是 `DataSource` 的 friend，复用同一链路 |
| `ConnectionPool::borrow()` 用 mutex + cv 阻塞等待 | 需要平行的非阻塞借出路径 |
| 池 `healthCheck()` 只 ping **idle** 队列里的连接 | 长期被异步会话钉住的连接不会被心跳误杀 ✅（关键前提成立） |
| `Session::cancel()` 是唯一跨线程安全入口，内部保证不抛 | 取消机制直接复用 |
| `transactionInternal` 已有看门狗 + cancel + 回滚 + "could not cancel" 语义 | 异步事务整段复用，语义零漂移 |
| `*Ungated` 重试循环内嵌 `std::this_thread::sleep_for(retryDelay(attempt))` | **必须拆出单次尝试**，否则 worker 被退避卡住 |
| `ErrorCode` 追加枚举值需放**末尾**（项目既有约定，见 observer.h） | 新增 `Overloaded` 追加在尾部 |
| 心跳线程按固定 interval 调 `pool->healthCheck()` | 等待者 deadline 过期可搭健康检查的便车，无需新线程 |
| 核心库 C++17；macOS libpqxx 8 的 PG TU 已是 C++20 | 协程层独立开关，不抬高核心库标准 |

---

## 3. 总体架构

```
┌────────────────────────────────────────────────────────────────────┐
│ 调用线程（业务线程 / asio 协程 / 任意）                                │
│  async::query(sql, params, cb)                                      │
│   ├─ fail-fast 闸门（不占 worker）：审计 preGate → 限流 → 熔断快检      │
│   │   失败 → 直接经完成调度器投递错误回调，立即返回 Handle(Done)          │
│   └─ 提交操作到 Executor（队列满 → Overloaded 快速失败）                │
├────────────────────────────────────────────────────────────────────┤
│ dbmw Executor（默认 N=hardware_concurrency worker + 1 timer 线程）    │
│  ① borrowAsync 借连接（池空 → 挂进等待者队列，worker 释放）             │
│  ② 借到后：Session 钉住连接 → queryOnceUngated 单次执行                │
│  ③ 可重试失败 → postAfter(退避) 重新提交 ①（不睡眠）                    │
│  ④ 成功/终态 → 完成调度器投递回调                                       │
├────────────────────────────────────────────────────────────────────┤
│ 连接池 State（mtx 保护）                                              │
│  idle 队列 / total 计数 / 异步等待者 deque（FIFO + deadline）           │
│  归还路径：等待者非空 → 直接交接（跳过 idle、跳过 ping）                  │
│  healthCheck 顺带 expireWaiters()（deadline 到点 → PoolExhausted）     │
├────────────────────────────────────────────────────────────────────┤
│ 驱动（IDatabaseConnection，阻塞 IO，原样复用）                          │
├────────────────────────────────────────────────────────────────────┤
│ 完成调度器（默认=Executor 本身；可注入 asio io_context 适配器）           │
│  → 回调 / set future value / 恢复协程                                 │
└────────────────────────────────────────────────────────────────────┘
```

**铁律（贯穿全文的正确性不变量）**：

- **I1 回调永不同步执行**：完成回调一律经完成调度器投递，绝不在发起调用的栈上、绝不在池锁内执行。消除重入死锁与栈溢出两类经典事故。
- **I2 结果整体 move**：`QueryResult` 等结果体按值 move 交付，不跨线程共享可变状态，无悬垂出参。
- **I3 治理只过一次**：fail-fast 闸门在调用线程，执行细节在 worker；组转发沿用 `*Ungated` 既有约定，不重复审计/扣令牌。
- **I4 同步路径零改动语义**：`borrow()`/`*Ungated` 对外行为不变，既有 125 项测试必须原样通过。

---

## 4. 核心设计决策

| # | 决策 | 理由 | 被否决的替代方案 |
|---|---|---|---|
| D1 | 内置线程池统一承载所有阻塞驱动 | 三驱动均为阻塞 API；统一模型最简、可测、语义一致 | 各驱动接原生非阻塞 API（libpq 可行但 MySQL 不可，能力分裂） |
| D2 | 治理前置到调用线程 | 限流/熔断/审计是纯内存操作，快检成本纳秒级；被拒请求不占 worker | 全部上 worker（浪费队列配额，过载时连拒绝都变慢） |
| D3 | 回调 + future + 协程三形态 | 回调零开销适合热路径；future 适合脚本/测试；协程适合新代码 | 只出协程（C++17 用户被排除；测试难写） |
| D4 | 完成调度器独立可注入 | asio 用户需要"在 io_context 线程恢复"；默认复用主 Executor 零成本 | 固定在 worker 完成（协程恢复线程不可控，破坏 strand 假设） |
| D5 | 池加异步等待者队列 + 归还直接交接 | "挂起而非阻塞"是少量 worker 扛高并发的关键；直接交接省一次 ping RTT | 把 `borrow()` 包成 future 上 worker（worker 仍被占满，只是换了地方阻塞） |
| D6 | `*Ungated` 拆单次尝试，退避用 `postAfter` | worker 睡眠 = 并发能力塌缩到线程数；定时重投递零占用 | 接受 worker 睡眠（高退避配置下吞吐不可接受） |
| D7 | 语句级超时用延迟任务，不开线程 | `transactionInternal` 每事务开看门狗线程是既有成本；单语句 QPS 高，每 op 一线程不可接受；`postAfter` 检查 + `cancel()` 等价 | 每操作一个 watcher 线程（线程爆炸） |
| D8 | 取消复用 `Session::cancel()` | 它是项目里唯一经过审计的跨线程取消原语，MSVC/Linux 均已验证 | 新造取消通道（重复造轮子，驱动侧还要改） |
| D9 | 协程层编译开关 opt-in | 核心库保持 C++17；libpqxx 8 场景已局部 C++20，但不应强制全库提标 | 全库升 C++20（破坏老用户编译环境） |
| D10 | 同步 `borrow()` 热路径零改动 | 池是全库最核心并发结构，刚经历跨平台 UB 修复，回归风险必须最小化 | 同步/异步统一为一条等待队列（更优雅但动核心，v0.3 再评估，见 §12） |

---

## 5. 公共类型与 API 全景

### 5.1 新头文件 `include/dbmw/async/async_types.h`

```cpp
namespace dbmw::async {

// ---- 结果载体（I2：值语义，整体 move）----
struct QueryResult    { common::Status status; common::ResultSet rows; };
struct ExecResult     { common::Status status; std::int64_t affected = 0; };
struct ExecKeysResult { common::Status status; std::int64_t affected = 0;
                        common::GeneratedKeys keys; };
struct EachResult     { common::Status status; std::uint64_t rows = 0; };
struct BatchResult    { common::Status status; common::BatchResult batch; };
struct OpResult       { common::Status status; };   // transaction / withSession

// ---- 回调类型 ----
using QueryCallback    = std::function<void(QueryResult &&)>;
using ExecCallback     = std::function<void(ExecResult &&)>;
using ExecKeysCallback = std::function<void(ExecKeysResult &&)>;
using EachCallback     = std::function<void(EachResult &&)>;
using BatchCallback    = std::function<void(BatchResult &&)>;
using OpCallback       = std::function<void(OpResult &&)>;

// ---- 每操作可选项 ----
struct Options {
    std::chrono::milliseconds borrowTimeout{-1};  // <0 = 池默认；0 = 不等待
    std::chrono::milliseconds timeout{0};         // 语句整体期限；0 = 不限
};

// ---- 取消句柄：轻量、可丢弃（discard 合法）----
class Handle {
public:
    Handle() = default;
    [[nodiscard]] bool valid() const;

    enum class State { Queued, Running, Done };
    [[nodiscard]] State state() const;

    // 请求取消：
    //  Queued  → 标记取消，操作启动时直接以 Cancelled 完成，不碰池
    //  Running → 尽力转发 Session::cancel()（驱动不支持时语句跑完，状态标 Cancelled）
    //  Done    → 返回错误（无事可做）
    common::Status cancel();

private:
    std::shared_ptr<detail::OpState> s_;   // 内部操作上下文，见 §8.4
};

} // namespace dbmw::async
```

`Handle` 状态机：

```
Queued ──submit 启动──▶ Running ──完成/超时/取消──▶ Done
   │                                                 ▲
   └────────── cancel()（不启动，直接 Done(Cancelled)）┘
```

### 5.2 门面 `include/dbmw/async/dbmw_async.h`（与同步门面逐项对齐）

```cpp
namespace dbmw::async {

// ============ 回调式（热路径）。末位参数是回调 → 返回 Handle ============

Handle query(const std::string &sql, QueryCallback cb, Options opts = {});
Handle query(const std::string &dataSource, const std::string &sql,
             QueryCallback cb, Options opts = {});
Handle query(const std::string &sql, const common::Params &params,
             QueryCallback cb, Options opts = {});
Handle query(const std::string &dataSource, const std::string &sql,
             const common::Params &params, QueryCallback cb, Options opts = {});

Handle execute(const std::string &sql, ExecCallback cb, Options opts = {});
Handle execute(const std::string &dataSource, const std::string &sql,
               ExecCallback cb, Options opts = {});
Handle execute(const std::string &sql, const common::Params &params,
               ExecCallback cb, Options opts = {});
Handle execute(const std::string &dataSource, const std::string &sql,
               const common::Params &params, ExecCallback cb, Options opts = {});

// 生成键形态（MySQL 自增 / PG·ODBC 的 RETURNING·OUTPUT，语义同同步版）
Handle execute(const std::string &sql, const common::Params &params,
               ExecKeysCallback cb, Options opts = {});
Handle execute(const std::string &dataSource, const std::string &sql,
               const common::Params &params, ExecKeysCallback cb, Options opts = {});

// queryEach：rowCb 在 worker 上逐行回调（须短小、线程安全）；doneCb 在完成后投递
Handle queryEach(const std::string &sql, const common::Params &params,
                 const common::RowCallback &rowCb, EachCallback done, Options opts = {});
Handle queryEach(const std::string &dataSource, const std::string &sql,
                 const common::Params &params,
                 const common::RowCallback &rowCb, EachCallback done, Options opts = {});

Handle executeBatch(const std::string &sql, const common::ParamBatch &batch,
                    BatchCallback cb, Options opts = {});
Handle executeBatch(const std::string &dataSource, const std::string &sql,
                    const common::ParamBatch &batch, BatchCallback cb, Options opts = {});

// 事务 / 会话：SessionFn 跑在 worker 上（内部整段复用 transactionInternal /
// withSessionInternal，看门狗、cancel、回滚、read-after-write 语义原样保留）
Handle transaction(const core::SessionFn &fn, OpCallback cb, Options opts = {});
Handle transaction(const std::string &dataSource, const core::SessionFn &fn,
                   OpCallback cb, Options opts = {});
Handle transaction(const common::TransactionOptions &txOpts, const core::SessionFn &fn,
                   OpCallback cb, Options opts = {});
Handle transaction(const std::string &dataSource, const common::TransactionOptions &txOpts,
                   const core::SessionFn &fn, OpCallback cb, Options opts = {});

Handle withSession(const core::SessionFn &fn, OpCallback cb, Options opts = {});
Handle withSession(const std::string &dataSource, const core::SessionFn &fn,
                   OpCallback cb, Options opts = {});

// ============ future 式（便利形态）。无回调参数 → 返回 future ============

std::future<QueryResult>    query(const std::string &sql);
std::future<QueryResult>    query(const std::string &sql, const common::Params &params);
std::future<QueryResult>    query(const std::string &dataSource, const std::string &sql,
                                  const common::Params &params);
std::future<ExecResult>     execute(const std::string &sql);
std::future<ExecResult>     execute(const std::string &sql, const common::Params &params);
std::future<ExecKeysResult> execute(const std::string &dataSource, const std::string &sql,
                                    const common::Params &params);
std::future<EachResult>     queryEach(const std::string &sql, const common::Params &params,
                                      const common::RowCallback &rowCb);
std::future<BatchResult>    executeBatch(const std::string &sql,
                                         const common::ParamBatch &batch);
std::future<OpResult>       transaction(const core::SessionFn &fn);
std::future<OpResult>       transaction(const std::string &dataSource,
                                        const common::TransactionOptions &txOpts,
                                        const core::SessionFn &fn);
// ...（future 式无 Handle、无取消；需要取消请用回调式）

// ============ 执行器管理 ============

// 注入自定义执行器（如 asio 适配器）。必须在任何异步调用之前设置；
// 传入后 dbmw 不再自建线程池，且执行器必须活到 DBMW::shutdown 完成。
void setExecutor(std::shared_ptr<IExecutor> ex);

// 完成回调/协程恢复的调度器；默认复用主执行器。
void setCompletionExecutor(std::shared_ptr<IExecutor> ex);

// 执行器运行时统计（队列深度/活跃线程/拒绝数/在途操作数）。
ExecutorStats stats();

} // namespace dbmw::async
```

**重载消歧说明**：回调式以**末位的 `*Callback` 参数**区分（`std::function` 无法从 `Params`/`Options` 隐式转换），future 式不带回调——同名重载无歧义，且 API 面与同步门面保持镜像对称。

### 5.3 协程层 `include/dbmw/async/task.h`（`DBMW_ENABLE_ASYNC_CORO`，C++20）

```cpp
namespace dbmw::async {

// 惰性 Task：仅在 co_await 时启动；未被 co_await 就析构则什么都不执行（安全放弃）。
template <class T>
class Task { /* promise_type: 初始挂起、最终挂起，结果存帧内 */ };

// 启动器：把惰性 Task 接到完成调度器上跑起来（fire-and-forget 的受控形态）
void run(Task<void> t);

Task<QueryResult>    queryAsync(std::string sql, common::Params params = {},
                                Options opts = {});
Task<ExecResult>     executeAsync(std::string sql, common::Params params = {},
                                  Options opts = {});
Task<ExecKeysResult> executeAsync(std::string dataSource, std::string sql,
                                  common::Params params, Options opts = {});
Task<BatchResult>    executeBatchAsync(std::string sql, common::ParamBatch batch,
                                       Options opts = {});
Task<OpResult>       transactionAsync(common::TransactionOptions txOpts,
                                      core::SessionFn fn);

} // namespace dbmw::async
```

`Task<T>` 的 awaiter 内部就是**回调 API 的薄封装**：`co_await queryAsync(sql)` 挂起协程 → 发起回调式 `query` → 完成调度器投递 → 把结果写进协程帧 → `handle.resume()`。因此：

- 协程恢复线程 = **完成调度器线程**（asio 用户注入 io_context 适配器即可在自己线程恢复）；
- 取消/超时/治理与回调形态完全同源，零额外语义。

---

## 6. Executor 设计

### 6.1 抽象接口 `include/dbmw/async/executor.h`

```cpp
namespace dbmw::async {

struct ExecutorStats {
    size_t threads = 0;
    size_t queueDepth = 0;        // 立即任务队列当前长度
    size_t active = 0;            // 正在执行任务的线程数
    uint64_t submitted = 0;
    uint64_t completed = 0;
    uint64_t rejected = 0;        // tryPost 因队列满被拒
    uint64_t delayedPending = 0;  // 定时任务挂起数
};

class IExecutor {
public:
    virtual ~IExecutor() = default;
    using Task = std::function<void()>;

    // 非阻塞提交：队列满立即返回 false（调用方据此快速失败 Overloaded）。
    virtual bool tryPost(Task task) = 0;

    // 延迟提交（重试退避 / 语句超时检查）。不做容量拒绝：
    // 定时任务数量天然受在途操作数约束，到期后进入工作队列，
    // 队列满时 timer 线程阻塞等待空位（背压自然传导）。
    virtual void postAfter(Task task, std::chrono::milliseconds delay) = 0;

    virtual void shutdown(std::chrono::milliseconds grace
                          = std::chrono::milliseconds(5000)) = 0;
    [[nodiscard]] virtual ExecutorStats stats() const = 0;
};

// 内置实现：N worker（0 = hardware_concurrency）+ 1 timer 线程 + 有界工作队列。
std::shared_ptr<IExecutor> makeThreadPoolExecutor(int threads, size_t queueSize);

} // namespace dbmw::async
```

### 6.2 内置 ThreadPoolExecutor 结构

```
工作队列（有界，默认 4096）◀── tryPost（满则 false = Overloaded）
        ▲
timer 线程：优先队列（deadline, task）；cv 等最近 deadline；
        │  到期 → 移入工作队列（满则阻塞等待，含 shutdown 检查）
        ▼
N 个 worker：循环取任务执行；异常一律 catch + 记日志（worker 死亡 = 服务死亡）
```

- **线程数**：默认 `hardware_concurrency`。异步模式下"并发能力 = worker 数 × 单 worker 周转率"，与连接池 `max` 解耦——池满时操作挂在**池等待者队列**而不是占 worker（§7）。
- **定时精度**：毫秒级（`steady_clock` + cv wait_until），满足退避与超时检查需求，不追求微秒。
- **关闭顺序**：`shutdown(grace)` = 停止接受新任务 → 上层在 grace 内等待在途操作 → 丢弃未到期的定时任务 → worker 协作式排空并 join。`grace` 不是线程强杀硬上限；驱动不支持 cancel 时必须等阻塞调用返回，否则分离线程会访问已释放的连接/执行器状态。

### 6.3 过载保护

- 队列满 → `tryPost` 返回 false → 操作立即以 `ErrorCode::Overloaded`（**新增，追加在 ErrorCode 末尾**）完成，`retryable = true`，调用方自行决定重投递或降级。
- **最大在途操作数 ≈ queueSize + workers + 池等待者 + 定时任务**，全部有界 → 资源不会无限增长。
- 配置见 §10。

---

## 7. 连接池改造（`connection_pool.h/.cpp`）

### 7.1 新增：异步等待者队列与 `borrowAsync`

```cpp
// ConnectionPool 公开新增：
// 异步借出：在任何线程调用都立即返回，结果（含失败）经回调交付。
// 回调在完成调度器线程执行（I1：不 inline、不在锁内）。
void borrowAsync(std::chrono::milliseconds timeout,
                 std::function<void(std::unique_ptr<Handle>, common::Status)> complete) const;

// State 私有新增（受 mtx 保护）：
struct AsyncWaiter {
    std::chrono::steady_clock::time_point enqueuedAt;
    std::chrono::steady_clock::time_point deadline;
    std::function<void(std::unique_ptr<Handle>, common::Status)> complete;
};
std::deque<AsyncWaiter> asyncWaiters;
```

`borrowAsync` 决策表（全部在调用线程，仅路径 A 涉及投递 worker 任务）：

| 池状态 | 动作 |
|---|---|
| closed | 投递 `complete(nullptr, PoolClosed)` |
| idle 队列有**新鲜**连接（validationInterval 未过期） | 锁内取出，锁外投递 `complete(handle, OK)` |
| idle 有**过期**连接 | 投递 worker 任务：ping → 成功交付 / 失败丢弃并走"建连" |
| idle 空、`total < max` | 锁内 `total++` 占位，投递 worker 任务：建连 → 成功交付 / 失败回滚计数并交付错误 |
| idle 空、`total == max` | 入 `asyncWaiters` 尾部，等归还直接交接 / deadline 到点交付 `PoolExhausted` |

### 7.2 归还路径：直接交接

`State::returnConn()` 修改点（reusable 且未超 lifetime 时）：

```
lock(mtx)
if (!asyncWaiters.empty() && 队首未过期) {
    waiter = move(asyncWaiters.front()); asyncWaiters.pop_front();
    // 直接把连接交给等待者：跳过 idle 队列、跳过借出 ping——
    // 这条连接刚被正常使用过，比闲置连接更可信
    unlock(mtx);
    完成调度器投递 waiter.complete(move(conn), OK);
    return;
}
// —— 以下与现状完全一致 ——
unlock 前：lifetime/复用判定 → idle.push / 关闭
unlock 后：cv.notify（同步等待者按既有语义被唤醒）
```

**公平性策略**：归还时**异步队列优先**，其次同步 cv 等待者，最后回 idle。理由：异步等待者有精确 FIFO 与 deadline；同步等待者被唤醒后仍要抢锁竞争，本就是近似公平。持续混用时同步侧可能略吃亏——在文档中明示，v0.3 若实测有饥饿再做交替出队（见 §12）。

### 7.3 deadline 过期：搭健康检查的便车

- **主动检查**：`healthCheck()` 开头调用 `expireWaiters()`（心跳线程已按 interval 跑，零新增线程）；`borrowAsync`/`returnConn` 触碰队列时顺带检查队首。
- 过期等待者弹出 → 投递 `complete(nullptr, PoolExhausted)`，message 携带实际等待毫秒数。
- 粒度 = 心跳 interval（默认秒级），对"池耗尽超时"语义足够（同步 `borrow()` 的 cv 超时同为毫秒~秒级近似）。

### 7.4 shutdown 与统计

- `shutdown(grace)`：锁内清空 `asyncWaiters` → 锁外逐个投递 `PoolClosed`，其余流程不变。
- `Stats` **末尾**追加 `size_t asyncWaiting`（结构体追加字段，向后兼容默认初始化）。

### 7.5 同步路径零改动声明

`borrow()`、cv 等待、ping 校验、leak 检测——**一行不动**。等待者队列是纯增量：同步代码只在 `returnConn` 多了一个"先看异步队列"的分支（一次 `deque::empty()` 判断）。既有全部池测试必须原样通过（G6 回归门禁）。

---

## 8. 执行管线（AsyncEngine）

### 8.1 所需的 `DataSource` 重构：拆出"单次尝试"

现状（叶子 `queryUngated`）：

```cpp
// 缓存查询 → 命中直接返回
for (attempt = 1..max_attempts) {
    borrowSession(h, ...);          // 阻塞借出
    status = s.query(sql, out);     // 执行
    if (!status.retryable || 最后一次) return status;
    std::this_thread::sleep_for(retryDelay(attempt));   // ← worker 杀手
}
```

重构为**三层**（私有方法，公开行为不变）：

```cpp
// ① 前奏（无 IO，纯内存）：组路由/readTarget/fallback 判定、缓存查询
common::Status queryPrelude(const std::string &sql, const common::Params &params,
                            CachedOutcome &cache, TargetInfo &target) const;

// ② 单次尝试（IO）：借出 + 执行 + 成功后写缓存 + markWrite（execute 路径）
common::Status queryAttempt(const TargetInfo &target, const std::string &sql,
                            const common::Params &params, common::ResultSet &out) const;

// ③ 重试策略外置：同步版保留原 sleep 循环（行为字节级等价）；
//    异步版用 postAfter 重投递。
```

> 实施注记：execute / queryEach / executeBatch / 生成键 / 流式各拆一套同构的 `*Prelude`/`*Attempt`。fallback-to-primary 与组转发的**逐尝试语义**在拆分中保持等价，由"异步 vs 同步结果一致性"测试锁定（§14 T12）。`runGuarded`、看门狗、审计下沉等逻辑不参与拆分、原样复用。

### 8.2 单语句操作完整生命周期（以带参 query 为例）

```
【调用线程】async::query(ds, sql, params, cb, opts)
  1. ds 不存在                     → 投递 cb({ConfigError})，返回 Handle(Done)
  2. preGate(sql, Query) 失败       → 投递 cb({SqlBlocked|RateLimited})，返回 Handle(Done)
  3. beforeAttempt() 熔断开放       → 投递 cb({CircuitOpen})，返回 Handle(Done)
  4. op = OpState{cb, deadline, ...}；登记进 OpRegistry（在途计数/关机排水用）
  5. opts.timeout > 0 → postAfter(超时检查, opts.timeout)
  6. engine.submit(op) → executor.tryPost(step1) 失败 → 注销 + 投递 cb({Overloaded})
     返回 Handle(Queued)

【worker·step1 借连接】
  op 已被用户取消？ → 注销 + 投递 cb({Cancelled})
  ds->pool()->borrowAsync(borrowTimeout,
      [op](handle, st) { …step2… })          // 池空 → op 挂队列，本 worker 立即释放

【worker·step2 执行】（借到连接后经执行器投递）
  Session s(move(handle), ds->name(), AuditContext{false, false});  // 审计已在 preGate 过
  QueryResult r;
  r.status = ds->queryPrelude(sql, params, cache, target);   // 缓存命中则跳过执行
  r.status.ok() && !cache.hit
      → r.status = ds->queryAttempt(target, sql, params, r.rows);
  ds->afterAttempt(r.status);                                // 熔断计数（与同步同源）
  r.status.retryable && attempt < max && !op.cancelled
      → postAfter(retryDelay(attempt)) 重投递 step1          // 不睡眠（D6）
  否则 → finish(op, r)

【完成】finish(op, r)
  取消挂起的超时检查任务；op.state = Done；从 OpRegistry 注销；
  完成调度器投递 [cb = op.cb, r = move(r)] { cb(move(r)); }   // I1 + I2
```

### 8.3 语句级超时（D7）

`postAfter` 的超时检查任务到点后：

```
if (op.state == Done) return;            // 已完成，无事可做（常态）
op.timedOut = true;
best-effort: 若 op 已钉住 Session → s.cancel()（try/catch 兜底，同看门狗约定）
// 不改写结果：真正的结果随 finish 走正常完成路径，由 finish 统一判定：
//   op.timedOut → status 改写为 QueryTimeout（retryable=true），
//   message 附 "could not cancel" 提示当 cancel 未送达——与 transactionInternal
//   的既有语义逐字对齐（含"超时是尽力而为"的文档说明）。
```

### 8.4 OpState（Handle 的实现载体）

```cpp
namespace dbmw::async::detail {
struct OpState {
    // 状态机（原子）
    std::atomic<Handle::State> state{Handle::State::Queued};
    std::atomic<bool>  userCancelled{false};
    std::atomic<bool>  timedOut{false};

    // 完成回调（一次性；经完成调度器投递）
    std::function<void()> deliver;

    // 取消路由：worker 借到连接后把 Session 装进来，finish 前清空。
    // mutex 保护指针的装载/读取（cancel 可来自任意线程）。
    std::mutex sessionMtx;
    Session  *pinnedSession = nullptr;    // 非拥有，Session 生命期在本操作内

    // 重试簿记
    int attempt = 0;
};
} // namespace dbmw::async::detail
```

`Handle::cancel()` 与超时检查共用 `OpState::requestCancel()`，差别只在置 `userCancelled` 还是 `timedOut`。

### 8.5 事务 / 会话异步化（最薄的一层）

```
【调用线程】gateSession() 失败 → 投递 cb({RateLimited})，返回
【worker】r.status = ds->transactionInternal(txOpts, fn, borrowTimeout, enforceReadOnly);
          // —— 整段复用：熔断闸门、begin、看门狗线程、cancel、回滚、
          //    commit、markWrite、read-after-write 全部原样 ——
【完成】finish(op, r)
```

约束（写入文档）：`SessionFn` 内部**禁止**调用 `dbmw::async::*`——回调在 worker 上执行，嵌套异步会在池偏小时互相等连接造成活锁；SessionFn 内用同步 `Session` 方法即可（它本来就在 worker 上，阻塞即职责）。

---

## 9. 生命周期：init / reload / shutdown

### 9.1 init

`DatabaseManager::init` 末尾：

```
if (cfg.async.enabled) {
    gExecutor = cfg.async.executor            // 测试可注入
        ? cfg.async.executor
        : makeThreadPoolExecutor(cfg.async.threads, cfg.async.queue_size);
    gCompletion = gExecutor;                  // 默认复用；setCompletionExecutor 可覆盖
}
```

执行器 worker **惰性启动**（首个 `tryPost` 时拉起），未使用异步的进程零额外线程。

### 9.2 reload（热加载）

- 在途操作持有 `shared_ptr<DataSource>`（经 `DBMW::dataSource()` 提升），数据源被替换后旧池进入 `reload` 既有的 5s 排空流程；在途 Handle 正常归还，未开始借出的操作遇 `PoolClosed` 经正常完成路径报错——**与同步语义一致，无需新机制**。
- 新池生效后新操作自然路由到新池。

### 9.3 shutdown（顺序是正确性问题，不是细节）

```
DBMW::shutdown(grace) 扩展为：
  1. asyncStopping = true                    // 新异步操作立即以 PoolClosed 拒绝
  2. OpRegistry::drain(grace)                // 等在途操作归零（cv + 计数）
  3. gCompletion->shutdown(0)                // 完成调度器排空（回调都投出去了）
  4. gExecutor->shutdown(grace)              // worker 排空任务
  5. 既有流程：statsReporter 停 → writeBuffers 停 → 池 shutdown(grace) → 心跳停
     // 池排在执行器之后：在途操作归还连接前，池必须活着
```

> 若用户注入了自定义执行器（`setExecutor`）：第 3/4 步改为"通知 + 等待在途归零"，不 join 用户线程；文档约定用户执行器必须活到 `shutdown` 返回。

---

## 10. 配置

`config::AsyncConfig`（挂 `GlobalConfig`，解析进 `datasource_config` 既有管线）：

```json
{
  "async": {
    "enabled": true,
    "threads": 0,
    "queue_size": 4096,
    "statement_timeout_ms": 0
  }
}
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | `true` | 惰性启动，不用异步则零成本 |
| `threads` | `0` | 0 = `hardware_concurrency` |
| `queue_size` | `4096` | 立即任务队列上限，满则 `Overloaded` |
| `statement_timeout_ms` | `0` | 全局默认语句期限（可被 `Options::timeout` 覆盖） |

旧配置文件（无 `async` 节）解析行为不变。

---

## 11. 为 v0.3+ 原生异步预留的接缝（本期只留口，不实现）

- 驱动侧：`IDatabaseConnection` 不动。将来 PG 接 `PQsendQuery`/`PQsocket` 时，新增**平行**的可选接口（如 `tryQuery`）+ 池等待者换成 socket 事件注册；执行管线对"单次尝试"的调用点不变。
- 池侧：等待者 `complete` 回调已经是唯一交付口，将来换成 io 事件唤醒不改对外形状。
- 协程侧：`AsyncSession`（语句间挂起、连接保持钉住）依赖协程放弃时的回滚语义设计，v0.3 专项。

---

## 12. 风险与开放问题（主动暴露）

| # | 风险 | 缓解 |
|---|---|---|
| R1 | `*Ungated` 拆分动到核心执行链，拆错会改变重试/fallback/缓存语义 | T12"异步 vs 同步结果一致性"矩阵测试；同步路径保留原循环结构、diff 审查；既有 125 测试作回归门禁 |
| R2 | 同步/异步双等待通道的公平性（异步优先可能饿死同步等待者） | 文档明示策略；压测 T14 观测同步 P99；v0.3 备选方案：统一单队列 |
| R3 | worker 上跑用户回调（queryEach 的 rowCb、事务 fn）拖慢执行器 | 文档约定回调须短小；`Overloaded` 是显式背压信号而非隐式劣化 |
| R4 | `std::future` 形态无取消 | 文档明示；需要取消用回调式拿 `Handle` |
| R5 | 协程被 co_await 前析构 / fire-and-forget 悬垂 | Task 惰性启动保证前者安全；后者只提供 `run()` 受控启动，不提供 detach |
| R6 | `ErrorCode::Overloaded` 追加引发下游 switch 告警 | 项目 v0.x 阶段可接受；README 变更日志标注 |
| R7 | 每操作一次 `shared_ptr<OpState>` + 队列入队的分配开销 | 热路径可接受（对比一次网络 RTT 可忽略）；future 形态另加 packaged_task，文档建议热路径用回调 |
| R8 | MSVC 对协程头文件的需求（`<coroutine>` 需 `/std:c++20`） | CI 矩阵加 `DBMW_ENABLE_ASYNC_CORO=ON` 构建位；关闭时零影响 |

**开放问题**（实现前需拍板，均可后补不改架构）：
1. `queryEach` 的 rowCb 是否提供"分批投递到完成调度器"选项（当前设计：worker 上同步逐行）。
2. `ExecutorStats` 是否并入 `StatsReporter` 周期日志（倾向是，M3 顺手）。
3. 队列满时 `Overloaded` 与"调用线程降级执行"（caller-runs）二选一还是可配（倾向：只 `Overloaded`，语义纯净）。

---

## 13. 文件改动清单

| 文件 | 动作 | 内容 |
|---|---|---|
| `include/dbmw/async/async_types.h` | 新增 | 结果体 / 回调类型 / Options / Handle |
| `include/dbmw/async/executor.h` | 新增 | IExecutor / ExecutorStats / 工厂 |
| `include/dbmw/async/dbmw_async.h` | 新增 | 异步门面（§5.2 全量） |
| `include/dbmw/async/task.h` | 新增 | Task\<T\> / run / queryAsync…（coro 开关守卫） |
| `src/async/executor.cpp` | 新增 | ThreadPoolExecutor 实现 |
| `src/async/async_engine.cpp` | 新增 | AsyncEngine / OpState / OpRegistry / 门面实现 |
| `src/async/task.cpp` | 新增 | 协程层 awaiter 桥接 |
| `include/dbmw/core/connection_pool.h` | 修改 | `borrowAsync`、`AsyncWaiter`、Stats 末尾加 `asyncWaiting` |
| `src/core/connection_pool.cpp` | 修改 | 等待者队列 / 归还直接交接 / expireWaiters / shutdown 清队 |
| `include/dbmw/core/database_manager.h` | 修改 | `friend class dbmw::async::detail::AsyncEngine;`（前向声明即可）+ `*Prelude`/`*Attempt` 私有声明 |
| `src/core/database_manager.cpp` | 修改 | Ungated 拆分（同步循环保持原样） |
| `include/dbmw/common/types.h` | 修改 | `ErrorCode::Overloaded` 追加在**末尾** |
| `include/dbmw/config/datasource_config.h` + 解析 | 修改 | `AsyncConfig` |
| `src/dbmw.cpp` | 修改 | init 建执行器；shutdown 排水顺序（§9.3） |
| `CMakeLists.txt` | 修改 | 新源文件；`DBMW_ENABLE_ASYNC_CORO` 选项（对应 TU 提标 C++20，仅该选项开启时） |
| `tests/dbmw_async_test.cpp` | 新增 | §14 全部 |
| `examples/async_example.cpp` | 新增 | 回调 / future / 协程 / 取消 / asio 接入五段示例 |
| `.github/workflows/build.yml` | 修改 | 矩阵加 coro=ON 构建位（只编译，不重复全测） |

---

## 14. 测试计划

| # | 测试 | 覆盖点 |
|---|---|---|
| T1 | Executor：post / postAfter / 满队列拒绝 / shutdown 排水 | 基础调度 |
| T2 | borrowAsync：立即借出 / 占位建连 / 池满入队 / deadline 过期 / 池关闭清队 | §7 全决策表 |
| T3 | 直接交接：归还时等待者即时获得连接，跳过 ping（用 mock 驱动计数 ping 次数验证） | 交接路径 |
| T4 | 混合公平：同步 borrow 与异步 waiter 并存，同步 P99 不劣化超阈值 | R2 |
| T5 | 管线 fail-fast：审计拦截 / 限流 / 熔断开放 → 回调在调用线程**之外**快速收到对应错误码 | D2 + I1 |
| T6 | 缓存命中：结果从缓存返回，mock 驱动零调用 | prelude |
| T7 | 重试：可重试错误 × postAfter 退避（验证 worker 数不变、无睡眠阻塞：重试期间提交无关任务能立即执行） | D6 |
| T8 | 取消：Queued 取消（不碰池）/ Running 取消（cancel 转发到 mock）/ Done 取消（报错） | Handle 状态机 |
| T9 | 语句超时：超时后 status=QueryTimeout + "could not cancel" 提示路径 | D7 |
| T10 | 异步事务：成功提交 / fn 失败回滚 / 超时看门狗 / 只读拦截——对齐同步事务测试断言 | §8.5 |
| T11 | 生命周期：shutdown 排水（在途 op 全部完成后再停池）/ reload 换池在途报 PoolClosed / 未用异步时零线程 | §9 |
| T12 | **一致性矩阵**：mock 驱动注入各种结果（成功/可重试/熔断计数/缓存交互），同一场景下同步与异步的最终 Status、熔断状态、缓存内容、markWrite 断言逐项相等 | R1 |
| T13 | 回调线程断言：完成回调线程 id ∈ 完成调度器线程集合（含"非调用线程"负断言） | I1 |
| T14 | 压测：32 线程 × 1k 操作并发的正确性 + TSAN 干跑 | 并发 |
| T15 | 协程（coro=ON 才编译）：co_await 基本链 / run 启动 / 未 await 析构安全 / 恢复线程断言 | §5.3 |
| T16 | 回归：既有 `dbmw_core_test`（125 项）原样通过 | G6 |

---

## 15. 里程碑（v0.2.0 内部推进，每段可独立合入）

| 里程碑 | 内容 | 出口标准 |
|---|---|---|
| **M1 地基** | ErrorCode / AsyncConfig / IExecutor + ThreadPoolExecutor / 池 borrowAsync + 等待者队列 + 直接交接 | T1–T4 绿；同步回归绿 |
| **M2 管线** | Ungated 拆分 / AsyncEngine / 回调 + future 门面 / Handle 取消 / 语句超时 / shutdown 排水 | T5–T13, T16 绿 |
| **M3 协程与交付** | Task\<T\> / run / queryAsync 族 / 示例 / README / CI coro 构建位 / TSAN | T14, T15 绿；全平台 CI 绿 |

---

## 16. 附录：使用示例

### A. 回调（热路径）

```cpp
dbmw::DBMW::init("config/datasources.json");

auto h = dbmw::async::query(
    "SELECT id, name FROM users WHERE age > ?",
    dbmw::common::Params{std::int64_t(18)},
    [](dbmw::async::QueryResult &&r) {
        if (!r.status.ok()) { /* 处理错误 */ return; }
        for (const auto &row : r.rows.rows()) { /* 消费 */ }
    });
// h 可丢弃；需要取消时保存并在另一处 h.cancel()
```

### B. future

```cpp
auto f1 = dbmw::async::query("SELECT count(*) FROM orders");
auto f2 = dbmw::async::execute("UPDATE cfg SET v = v + 1");
// ……做别的事……
auto r1 = f1.get();
```

### C. C++20 协程

```cpp
dbmw::async::run([]() -> dbmw::async::Task<void> {
    auto r = co_await dbmw::async::queryAsync(
        "SELECT id FROM orders WHERE status = ?",
        dbmw::common::Params{std::string("paid")});
    if (!r.status.ok()) co_return;
    dbmw::async::ExecResult e;
    for (const auto &row : r.rows.rows())
        e = co_await dbmw::async::executeAsync(
            "UPDATE orders SET status = 'shipped' WHERE id = ?",
            dbmw::common::Params{row.at("id")});
});
```

### D. 事务（SessionFn 跑在 worker 上）

```cpp
dbmw::async::transaction(
    dbmw::common::TransactionOptions{.isolation = dbmw::common::IsolationLevel::Serializable,
                                     .readOnly = false,
                                     .timeout = std::chrono::milliseconds(3000)},
    [](dbmw::core::Session &s) {
        std::int64_t n = 0;
        if (auto r = s.execute("UPDATE a SET v = v - 1 WHERE id = 1", n); !r.ok()) return r;
        return s.execute("UPDATE b SET v = v + 1 WHERE id = 2", n);
    },
    [](dbmw::async::OpResult &&r) { /* r.status */ });
```

### E. 接入 asio（协程恢复到 io_context 线程）

```cpp
boost::asio::io_context io;
dbmw::async::setExecutor(dbmw::async::makeThreadPoolExecutor(0, 4096));
dbmw::async::setCompletionExecutor(
    std::make_shared<AsioExecutor>(io));   // 用户实现 IExecutor：tryPost = io.post
// 此后所有完成回调 / 协程恢复都发生在 io 的运行线程上，
// 可安全地在协程里操作 strand 保护的对象。
```
