# dbmw v0.5.0 结果集实体映射层详细设计方案

> 状态：待评审（未动代码）
> 基线：`v0.4.0`（tag `v0.4.0`，CMake `project(dbmw VERSION 0.4.0)`）
> 目标版本：`v0.5.0`
> 关联文档：`docs/async-design-v0.2.0.md`（异步三层形态）、`docs/roadmap-design-v0.4.0.md`（非目标与永不实现清单）

---

## 0. 结论先行

| 项 | 结论 |
|---|---|
| 做什么 | **结果集 ↔ 业务实体的适配层**（行→对象 / 对象→绑定参数），**不是 ORM** |
| 形态 | 单头文件 `include/dbmw/mapping.h`，header-only、全模板、手写特化、零引擎改动 |
| 严格性 | **类型不符、NULL 落非 `optional` 即报错**；**缺列默认跳过**（字段保持默认值），可 `.missingColumns(MissingColumns::Error)` 收紧；多余列默认忽略，可 `.extraColumns(ExtraColumns::Error)` 收紧 |
| 方向 | **读写双向**：读 = `ResultSet/Row → T`；写 = `T → Params`（含批量与生成键回填） |
| 范围 | 同步 + 异步（回调 / future）+ 协程**一次做完**；游标与事务内 `Session` 一并覆盖 |
| 归属 | 独立头文件，不修改任何既有签名（遵守 §1.1「头文件即 ABI」） |
| 需要改动核心的地方 | 仅一处可选：`ErrorCode` 尾部追加 `MappingError`（见 §9 与决策 D1） |

**与 v0.4.0 非目标的关系（必须先说清）**：`roadmap-design-v0.4.0.md` §1.2 把「ORM / 对象映射」列为非目标，理由是"属应用层，中间件应止步于 SQL 与结果集"。本方案**不推翻该原则**，而是把它的措辞精确化：

- 仍然**不做**：关系映射、关联（1:1/1:N）、懒加载、脏跟踪、UnitOfWork、自动 DDL、自动生成业务 SQL、查询 DSL、实体级缓存。这些会改变中间件性质，且泛型 `T` 无法在缓存/治理层安放。
- **要做**：一次**显式的**行↔对象适配。SQL 仍由调用方书写，映射声明仍由调用方手写，中间件只提供"按声明搬运"的能力，不持有任何元数据驱动的魔法。

因此 §1.2 的非目标行需改写为「完整 ORM（关系映射 / 懒加载 / 脏跟踪 / 自动生成 SQL）」，并在 `guide.md` / `guide_en.md` 的"非目标"章节同步（见 §13 文档改动清单）。

---

## 1. 目标与非目标

### 1.1 目标

| # | 目标 | 验收口径 |
|---|---|---|
| G1 | 消除业务侧"for 循环搬字段"的样板 | 一次声明，全场景复用（同步/异步/流式/游标/事务内） |
| G2 | 映射错误**可定位** | 失败信息含实体名、列名、源类型、目标类型、行号 |
| G3 | 读写双向 | 读：`queryAs<T>` 等；写：`T → Params` + 批量 + 生成键回填 |
| G4 | 与治理/缓存/脱敏语义正交 | 缓存仍只存原始 `ResultSet`；映射在脱敏之后 |
| G5 | 零引擎侵入 | `database_manager.cpp` / `async_engine.cpp` / `dbmw.h` 既有签名不动 |
| G6 | 可扩展到业务自定义类型 | 提供 `ValueConverter<T>` 特化点（枚举、强类型 ID、protobuf 等） |

### 1.2 非目标（明确不做）

| 非目标 | 理由 |
|---|---|
| 关系映射与关联加载 | 需要元数据模型 + 惰性执行，等于 ORM |
| 脏跟踪 / UnitOfWork / 自动 UPDATE | 需长期持有实体状态，与嵌入式库定位冲突 |
| 自动生成业务 SQL（DDL、联表、分页） | 需 SQL 解析器/构造器；本版只拼**列名与占位符**这类结构确定的片段 |
| 实体级缓存 | 泛型 `T` 在缓存层无处安放，且脱敏语义要求缓存只存原始行（I1） |
| 隐式类型转换与"尽力而为"填充 | 与 G2 冲突，且静默丢精度是最难查的一类线上问题 |
| 运行期反射 / 宏侵入实体定义 | 用户已决策：纯手写特化 |

---

## 2. 现状约束（设计输入，均已从代码核实）

| # | 事实 | 出处 | 对设计的影响 |
|---|---|---|---|
| C1 | `Value = variant<nullptr_t, bool, int64, uint64, double, Decimal, string, Date, Time, Timestamp, Uuid, Json, Blob>` | `common/types.h:45` | 转换矩阵共 13 个源类型；`Decimal` 等强类型刻意防丢精度，映射必须尊重 |
| C2 | `Row` 底层是 `std::map<string, Value>`；`at(col)` 缺失返回静态 NULL | `common/types.h:52-71` | **缺列与 NULL 无法靠 `at()` 区分**，映射必须先用 `data().find()` 判存在性，否则缺列会被当成 NULL 静默通过 |
| C3 | `ResultSet::transformed` + 查询缓存只存驱动原始结果 | `common/types.h:96-99`、`core/query_cache.h:38-42` | 映射必须在缓存命中之后执行（I1） |
| C4 | SPI `afterExecution` 改写结果（脱敏）在引擎内完成，结果不进缓存 | `roadmap` §9（M7，I10） | 映射层在门面最外层，天然位于脱敏之后，顺序正确 |
| C5 | 同步门面 `DBMW::query/execute/queryEach/executeBatch/transaction/withSession`；`core::Session` 提供同名方法（含生成键重载） | `dbmw.h:45-113`、`core/database_manager.h:45+` | 映射层需为「门面」与「Session」各提供一套重载 |
| C6 | 异步三层：回调（返回 `Handle`）/ future / 协程（`DBMW_ENABLE_ASYNC_CORO`，C++20） | `async/dbmw_async.h`、`async/task.h` | 映射层需提供三套同形 API |
| C7 | 异步完成投递走**完成调度器**（默认复用主执行器，可注入 asio） | `async/dbmw_async.h:113` | 映射发生在投递线程，必须保持轻量（见 D3/R3） |
| C8 | MySQL 生成键固定合成列名 `insert_id`；PG/ODBC 靠 SQL 自带 `RETURNING/OUTPUT`；**dbmw 不自动追加** | `driver/mysql_driver.cpp:741`、设计既有约定 | 生成键回填需同时支持"列名匹配"与"`lastInsertId()` 兜底"两条路 |
| C9 | 游标接口 `ICursor::fetch(n, ResultSet&)` / `fetchRow(Row&, bool&)` | `core/cursor.h:25,28` | 映射层提供 `fetchAs<T>` 即为薄封装 |
| C10 | `ErrorCode` 追加值只能在枚举尾部（不得中间插入） | `common/types.h:243-247` | `MappingError` 只能加在 `Overloaded` 之后 |
| C11 | 既有测试规模 core 147 / async 67 / coro 35；测试为自研 `check(cond, name)` 宏 + mock 驱动 | `roadmap` §12.2、`tests/` | 新增测试沿用同一风格，不引入第三方框架 |
| C12 | `Task<T>` 惰性、结果存帧；GCC 13 在 `co_await` 实参含非平凡花括号临时时 ICE | `async/task.h:24` | 协程示例与测试一律用具名局部变量传参 |

---

## 3. 总体架构

### 3.1 分层与插入点

```
  业务代码
     │  queryAs<User>(sql, params)                ← 新增：映射层门面（namespace dbmw）
     ▼
┌──────────────────────────────────────────────────────────────┐
│  映射层 mapping.h（header-only，本期唯一新增运行时代码）        │
│  · 声明：RowMapper<T>::describe() → 字段表                     │
│  · 读：Row → T    写：T → Params                              │
│  · 位置：调用方与门面之间，引擎之外                             │
└──────────────────────────────────────────────────────────────┘
     │  内部调用既有 DBMW::query / async::query / Session::query
     ▼
  门面 DBMW / async 引擎 ── 治理闸门（审计·限流·熔断·影子）
     │
     ▼  查询缓存（命中即返回原始 ResultSet 深拷贝）
     │
     ▼  驱动执行 → SPI afterExecution（脱敏，置 transformed）
     │
     ▼  交付 ResultSet ──────────────► 映射层在此之后取走并映射
```

**关键：映射层不进入虚线框内的任何一层。** 它只是一层"调用约定"——接收引擎已经治理完毕的 `ResultSet`，按声明转成 `T`。

### 3.2 三条执行链上映射发生的位置

| 链 | 映射发生点 | 线程 | 说明 |
|---|---|---|---|
| 同步 | `DBMW::query` 返回之后，映射层函数内 | 调用线程 | 最简单 |
| 异步（回调/future/协程） | 引擎把 `QueryResult` 投递给完成回调**之后**、交给用户回调之前 | 完成调度器线程（默认 = 主执行器 worker；注入 asio 时 = `io_context` 线程） | 见 D3 与 R3：映射必须轻量 |
| 流式 `queryEachAs` | `RowCallback` 内逐行映射 | worker（流式行回调本就在 worker 上） | 不物化全量，内存友好 |
| 游标 `fetchAs` | `fetch()` 拿到 `ResultSet` 之后 | 调用线程 | 薄封装 |
| 事务/会话内 | `Session::query` 返回之后 | 会话所在线程 | 复用同步实现，不新增异步嵌套（沿用 async 设计对 `SessionFn` 的既有约束） |

---

## 4. 核心 API 设计

### 4.1 声明层：`RowMapper<T>` 与字段表

```cpp
namespace dbmw::mapping {

// 字段标志（位掩码，可组合）
enum class FieldFlags : unsigned {
    None       = 0,
    PrimaryKey = 1u << 0,  // 参与 UPDATE ... WHERE（可多个 = 复合主键）
    Generated  = 1u << 1,  // 由数据库生成：INSERT 参数跳过；回填时接收
    ReadOnly   = 1u << 2,  // 视图/计算列：写方向跳过
    Lossy      = 1u << 3,  // 允许有损数值转换（Decimal / string → 浮点或整型）
    Textual    = 1u << 4,  // 允许与文本表示互转（Date/Time/Uuid/Json/Decimal ↔ string）
};
constexpr FieldFlags operator|(FieldFlags a, FieldFlags b) noexcept;
constexpr bool       has(FieldFlags v, FieldFlags bit) noexcept;

// 多余列策略：默认忽略（兼容 SELECT *），可切到报错
enum class ExtraColumns { Ignore, Error };

// 字段表。由 describe() 一次性构建，之后被复用（见 §4.5 缓存）
template <class T>
class Mapping {
public:
    template <class M>
    Mapping &field(M T::*ptr, std::string column,
                   FieldFlags flags = FieldFlags::None);
    Mapping &extraColumns(ExtraColumns policy);

    std::size_t size() const noexcept;
    const std::vector<std::string> &columns() const noexcept;
};

// 用户特化点：必须提供 describe()
template <class T>
struct RowMapper {
    // static Mapping<T> describe();
};

} // namespace dbmw::mapping
```

**业务侧声明示例**（纯手写，无宏）：

```cpp
struct User {
    std::int64_t              id;
    std::string               name;
    std::optional<std::string> email;   // 可为 NULL
    common::Decimal           balance;  // 绝不自动转 double
    common::Timestamp         createdAt;
};

namespace dbmw::mapping {
template <> struct RowMapper<User> {
    static Mapping<User> describe() {
        return Mapping<User>()
            .field(&User::id,        "id",         FieldFlags::PrimaryKey | FieldFlags::Generated)
            .field(&User::name,      "name")
            .field(&User::email,     "email")
            .field(&User::balance,   "balance")
            .field(&User::createdAt, "created_at");
    }
};
}
```

缺失特化时给出可读诊断（不是一屏模板报错）：

```cpp
static_assert(detail::hasDescribe<T>(),
              "dbmw::mapping: 请为实体特化 RowMapper<T> 并提供 static Mapping<T> describe()");
```

### 4.2 转换层：`ValueConverter<T>`（也是用户扩展点）

```cpp
template <class U> struct ValueConverter {
    // 读：Value → U。flags 携带该字段的 Lossy/Textual 声明
    static common::Status fromValue(const common::Value &v, U &out, FieldFlags flags);
    // 写：U → Value
    static common::Value  toValue(const U &in);
};
```

- 内置实现对 §5 的矩阵负责；
- 业务自定义类型（强类型 ID、枚举之外的领域类型、第三方时间库）通过**特化**接入，不改库代码；
- `std::optional<U>`、`enum class` 有内置偏特化（见 §5.3 / §5.4）。

### 4.3 读路径 API（`namespace dbmw` 自由函数，不动 `dbmw.h`）

```cpp
namespace dbmw {

template <class T> struct EntityResult {
    common::Status   status;
    std::vector<T>   items;     // status 非 Ok 时保证为空（不返回半成品）
};

template <class T> struct EntityOne {
    common::Status   status;
    std::optional<T> value;     // 零行 → nullopt（不是错误）；多于一行 → 报错
};

// ---- 门面级（默认 / 指定数据源，形态与 DBMW::query 一一对应）----
template <class T> EntityResult<T> queryAs(const std::string &sql);
template <class T> EntityResult<T> queryAs(const std::string &sql, const common::Params &params);
template <class T> EntityResult<T> queryAs(const std::string &dataSource,
                                           const std::string &sql, const common::Params &params);

template <class T> EntityOne<T> queryOneAs(...);          // 同上三种重载

// 流式：不物化全量，逐行映射后回调；返回 false 提前终止（语义同 queryEach）
template <class T>
common::Status queryEachAs(const std::string &sql, const common::Params &params,
                           const std::function<bool(T &&)> &cb, std::uint64_t &rows);

// ---- 事务 / 会话内（core::Session 版）----
template <class T> EntityResult<T> queryAs(core::Session &s, const std::string &sql,
                                           const common::Params &params);
template <class T> EntityOne<T>    queryOneAs(core::Session &s, ...);
template <class T> common::Status  queryEachAs(core::Session &s, ...);

// ---- 游标 ----
template <class T> EntityResult<T> fetchAs(core::ICursor &c, std::size_t n);

// ---- 生成键结果集 → 实体（PG/ODBC 的 RETURNING/OUTPUT 直出多行多列）----
template <class T> EntityResult<T> keysAs(const common::GeneratedKeys &keys);

} // namespace dbmw
```

### 4.4 写路径 API

```cpp
namespace dbmw::mapping {

// 参数：按声明顺序生成 Params（跳过 Generated / ReadOnly，除非显式包含）
enum class WriteCols { Writable, All, PrimaryKey };
template <class T> common::Params paramsOf(const T &entity, WriteCols which = WriteCols::Writable);
template <class T> common::ParamBatch batchOf(const std::vector<T> &entities,
                                              WriteCols which = WriteCols::Writable);

// 结构确定的 SQL 片段（用 quoteIdentifier 转义标识符；不碰 WHERE 业务条件）
template <class T> std::string insertSql(std::string table);             // INSERT INTO t (c1,c2) VALUES (?,?)
template <class T> std::string updateSql(std::string table);             // UPDATE t SET c1=?,c2=? WHERE pk=?
template <class T> std::string updateSql(std::string table,
                                         std::vector<std::string> setCols,
                                         std::vector<std::string> whereCols);

// 生成键回填：优先按 Generated 列的列名从 keys 取；MySQL 只有 insert_id 列时，
// 回退到 lastInsertId() 并赋给第一个整型 Generated 列（见 C8）。
template <class T> common::Status applyGeneratedKeys(const common::GeneratedKeys &keys, T &entity);

} // namespace dbmw::mapping

namespace dbmw {

// 便捷形态：拼 SQL + 绑参数 + 执行 + 回填
template <class T> InsertResult  insertAs(std::string table, T &entity);              // 回填生成键
template <class T> ExecResult    updateAs(std::string table, const T &entity);        // WHERE = PrimaryKey
template <class T> BatchResult   insertBatchAs(std::string table, const std::vector<T> &entities);
// 事务内版本：多一个 core::Session& 首参

} // namespace dbmw
```

**约定（沿用既有设计，不新增魔法）**：

- `insertSql` **不追加 `RETURNING` / `OUTPUT`**（与 C8 一致）；需要生成键时业务自己在 SQL 里写，或用 `insertAs` 走 MySQL 的 `insert_id` 路径。
- `updateSql` 的 `WHERE` 只由 `PrimaryKey` 声明推导；无主键声明时 `updateAs` 直接返回 `MappingError`（不给全表更新的机会）。
- 写方向的 `NULL`：`std::optional<U>` 空值 → `nullptr` 参数；非空 optional 与非 optional 走同一矩阵。

### 4.5 字段表的构建与复用

`describe()` 返回 `Mapping<T>`（含 `std::function` 成员），**每次调用都重建会有可观开销**。因此：

```cpp
template <class T> const Mapping<T> &mapping() {
    static const Mapping<T> m = RowMapper<T>::describe();   // C++11 magic static，线程安全
    return m;
}
```

- 一次构建，进程内复用；不引入全局注册表，也不需要初始化顺序保证；
- 代价：`describe()` 里的 `std::function` 捕获在进程生命周期内常驻（几十字节 × 字段数，可忽略）。

### 4.6 异步 API（`namespace dbmw::async`，形态与既有三层镜像）

```cpp
namespace dbmw::async {

template <class T> using EntityQueryCallback = std::function<void(EntityResult<T> &&)>;

// 回调式 → Handle（取消语义与既有 query 完全一致：Queued/Running/Done 三态不变）
template <class T> Handle queryAs(const std::string &sql, const common::Params &params,
                                  EntityQueryCallback<T> cb, Options opts = {});
template <class T> Handle queryAs(const std::string &dataSource, const std::string &sql,
                                  const common::Params &params,
                                  EntityQueryCallback<T> cb, Options opts = {});

// 流式：rowCb 在 worker 上逐行执行（映射在其中完成）；done 经完成调度器投递
template <class T> Handle queryEachAs(const std::string &sql, const common::Params &params,
                                      const std::function<bool(T &&)> &rowCb,
                                      EachCallback done, Options opts = {});

// future 式（无 Handle、无取消）
template <class T> std::future<EntityResult<T>> queryAs(const std::string &sql,
                                                        const common::Params &params);

#ifdef DBMW_ENABLE_ASYNC_CORO
template <class T> Task<EntityResult<T>> queryAsAsync(std::string sql,
                                                      common::Params params = {},
                                                      Options opts = {});
#endif
} // namespace dbmw::async
```

实现上全部是**既有异步 API 的薄封装**（与 `task.cpp` 里协程工厂同思路）：拿到 `QueryResult` → 调 `mapping::fromRows<T>` → 交给用户回调/写入协程帧。引擎零改动。

> ⚠️ 协程形态沿用 C12 的 GCC 13 约束：`co_await queryAsAsync<User>(sql, params)` 的实参必须是**具名局部变量**，不得在 `co_await` 实参里写非平凡花括号临时。文档与示例会遵守，测试亦同。

---

## 5. 类型转换规则

### 5.1 默认策略

1. **精确匹配优先**：源 `Value` 的 alternative 与目标类型语义一致才通过；
2. **无隐式放宽**：不做数值提升（除整型间带范围检查）、不做文本↔数值猜测、不做 `Decimal → double`；
3. **类型不符即 `MappingError`**：不填默认值、不吞掉、不跳字段；
4. 唯一例外由字段显式声明：`Lossy` / `Textual`（§4.1）。

> **v0.5.0 修订（用户决策）**：§5.5 的「缺列」行为从「报错」放宽为「**默认跳过**」。类型不符与
> NULL 落非 `optional` 仍保持报错（这两类静默填值最难排查）。

### 5.2 转换矩阵（读方向：`Value → U`）

| 目标 `U` | 接受 | 规则 |
|---|---|---|
| `bool` | `bool` | 其它一律报错（含 `int64 0/1`——驱动返回整型说明列类型与声明不符，应显式修声明） |
| `int8/16/32/64_t`、`uint8/16/32/64_t` | `int64_t`、`uint64_t` | 范围检查，溢出报错（消息含实际值）；`double`/`Decimal`/`string` 报错，除非 `Lossy` |
| `float` / `double` | `double` | `int64/uint64` 报错（严格）；`Decimal` 报错（防丢精度，与 C1 同源）；`Lossy` 下接受 `int64/uint64/Decimal/string(可解析)` |
| `std::string` | `string`、`Decimal`、`Date`、`Time`、`Uuid`、`Json` | 取文本表示；`Blob` 报错（二进制不是文本）；数值/时间报错 |
| `common::Decimal` | `Decimal` | `string` 需 `Textual`；`double` 报错（**反向也不允许**，防二次丢精度） |
| `common::Date` / `Time` / `Uuid` / `Json` | 同名强类型 | `string` 需 `Textual`（`Date/Time` 走 `tryParseTimestamp` 语义校验） |
| `common::Timestamp` | `Timestamp` | `Date/Time/string` 需 `Textual` + `tryParseTimestamp`；解析失败报错（不回退字符串） |
| `common::Blob` | `Blob` | 其余全部报错 |
| `std::optional<U>` | `nullptr` → `nullopt`；否则按 `U` 规则 | 见 §5.3 |
| `enum class E` | `int64_t` / `uint64_t` | 转 `underlying_type` 并做范围检查（不校验枚举值合法性——那是业务语义） |
| 其它类型 | — | 需用户特化 `ValueConverter<U>`，否则编译期报错并提示 |

**非 `optional` 目标遇 `NULL`** → `MappingError`（"column x is NULL but target is not std::optional"）。

### 5.3 `std::optional<U>` 规则

- `nullptr` → `nullopt`；
- 非 NULL → 按 `U` 的矩阵转换；
- `std::optional<std::optional<U>>` 无意义，`static_assert` 拒绝。

### 5.4 写方向（`U → Value`）

- 默认取读方向的**逆映射**，同样严格（不允许 `double → Decimal` 这类"看起来能转"的路径，除非 `Textual`）；
- `optional` 空 → `nullptr`；
- `Generated` / `ReadOnly` 列在 `paramsOf(Writable)` 中跳过；
- 未声明进 `describe()` 的字段不参与写（映射层不猜测结构体布局）。

### 5.5 列匹配规则

| 情形 | 默认行为 | 可配 |
|---|---|---|
| 声明的列在结果集中缺失 | **跳过**该字段（保持默认构造值），正常返回 | `Mapping<T>::missingColumns(MissingColumns::Error)` 切为报错 |
| 结果集有声明外的列 | 忽略（兼容 `SELECT *`、联表） | `Mapping<T>::extraColumns(ExtraColumns::Error)` 切为报错 |
| 重名列 | 沿用 `Row` 既有语义（后者覆盖前者），文档提示用别名 | — |
| `SELECT` 顺序 | 不依赖：`Row` 是 map，按列名取 | — |

> **实现要点（来自 C2）**：判断缺列必须用 `row.data().find(name)`，**不能**用 `at()`——后者对缺失列
> 返回静态 NULL，会把"SQL 少查了一列"伪装成"这列是 NULL"；缺列与 NULL 是两回事（前者跳过、后者仍报错）。

---

## 6. 错误模型

### 6.1 失败语义

- **整批失败**（Abort）：任一行、任一字段转换失败 → 返回 `MappingError`，`items` 保证为空，不返回半成品；
- 失败不影响连接与事务：映射是纯 CPU 操作，无 IO、不改连接状态（I3）；
- 错误信息格式（便于 grep 与告警）：

```
mapping: User.email: NULL -> std::optional<std::string> ok;  User.balance: Decimal -> double refused [row=3]
mapping: User.created_at: column not found in result set [row=0]
mapping: Order.qty: value 4294967296 out of range for int32_t [row=7]
```

### 6.2 错误码

新增 `common::ErrorCode::MappingError`，**追加在枚举尾部**（遵守 C10），并在 `src/common/types.cpp:16` 的 `errorCodeToString` 补字符串 `"MappingError"`。

- 理由：映射失败与"数据库报错"（`QueryError`）性质不同——前者是**调用方声明与 SQL 结果不匹配**，属于可修复的程序错误，运维侧需要能单独分类与告警；
- 替代方案：复用 `QueryError`。代价是灰度期无法区分"数据库出问题"与"我们字段写错了"（见决策 D1）。

---

## 7. 不变量（实现与评审的验收清单）

| # | 不变量 | 为什么 |
|---|---|---|
| I1 | 查询缓存只存原始 `ResultSet`；**映射结果绝不入缓存**，缓存命中后照常映射 | 泛型 `T` 在缓存层无处安放；与 I10（脱敏结果不进缓存）同源 |
| I2 | 映射发生在 SPI `afterExecution`（脱敏）之后 | 顺序错会绕过脱敏，直接泄漏字段 |
| I3 | 映射不触碰连接、事务、池状态 | 纯 CPU；失败也不得影响连接可用性 |
| I4 | 不修改任何既有公开签名；`dbmw.h` / `dbmw_async.h` / `task.h` 零改动 | §1.1 头文件即 ABI |
| I5 | 严格：无静默转换、无默认值填充、无"尽力而为" | 用户已决策；静默丢精度最难查 |
| I6 | 不自动追加 `RETURNING` / `OUTPUT`；不生成业务 `WHERE` | 沿用既有约定（C8），避免改写 SQL 语义与方言耦合 |
| I7 | 异步映射在**完成投递线程**执行，且必须轻量 | C7：asio 注入时该线程就是 `io_context` 线程 |
| I8 | 失败时 `items` 为空；绝不返回部分映射结果 | 避免调用方拿到半截数据还以为成功 |
| I9 | 映射层不引入新的全局状态（字段表用函数内静态） | 免初始化顺序问题，可多 translation unit 安全复用 |

---

## 8. 性能与成本

| 维度 | 评估 |
|---|---|
| 单行映射 | 每字段一次 `std::map::find`（O(log m)）+ 一次 variant 访问。m 通常 < 30，万行级无感 |
| 分配 | `vector<T>` 按 `rows.size()` 一次性 `reserve`；字符串按列 move，不额外拷贝 |
| 字段表 | 进程内构建一次（§4.5）；每字段两个 `std::function`，映射时有一次间接调用 |
| 异步 | 映射占完成投递线程时间；大结果集 + asio 单线程场景需评估（R3） |
| 编译期 | 全模板，每个 `queryAs<T>` 实例化一份；实体数量多时编译时间上升，建议声明集中在一个头文件 |
| 二进制 | header-only，未使用不产生代码 |

**可选优化（本期不做，留接口）**：`Row` 未来若改为「列名数组 + 值数组 + 索引」，映射可降为 O(1) 定位；该改动属核心，不在 v0.5.0 范围。

---

## 9. 开放决策（需拍板后方可实现）

| # | 决策点 | 选项 | 建议 |
|---|---|---|---|
| D1 | 是否新增 `ErrorCode::MappingError` | A 新增（追加枚举尾部） / B 复用 `QueryError` | **A**。映射失败是程序错误，需与数据库故障区分；代价是一行枚举 + 一行字符串 |
| D2 | `insertSql` / `updateSql` 这类"拼列名与占位符"是否提供 | A 提供最小版 / B 只提供 `paramsOf`，SQL 全手写 | **A**。结构确定、无方言差异，能消掉最啰嗦的一段样板；不越界到查询构造器 |
| D3 | 异步映射线程 | A 完成投递线程（零引擎改动） / B 引擎内 worker 上映射（需改 `async_engine.cpp`） | **A**。B 会让映射挤占 worker 且需引擎改动；A 的代价是 §7 I7 约束——映射必须轻量 |
| D4 | 容错模式 | A 只有 Abort / B 同时提供 Skip（跳过坏行） | **A**。用户已决策"直接报错"；Skip 会掩盖数据问题，留到有真实需求再评估 |
| D5 | `enum class` 支持范围 | A 整型→枚举（带范围检查） / B 不支持 | **A**。成本极低，状态字段是最常见的枚举列 |

---

## 10. 测试计划

新增 `tests/dbmw_mapping_test.cpp`，沿用既有 `check(cond, name)` + mock 驱动风格（C11），在 **sync 与 coro 两个构建位都构建**（协程段用 `DBMW_ENABLE_ASYNC_CORO` 门控）。

| # | 用例 | 期望 |
|---|---|---|
| M1 | 基本映射：5 列全类型 | `items` 与原始行逐字段相等 |
| M2 | `optional` 接收 NULL / 非 NULL | `nullopt` / 有值 |
| M3 | NULL 落非 optional | `MappingError`，消息含列名 |
| M4 | 缺列（SQL 少查一列） | 默认跳过、字段保持默认值；`MissingColumns::Error` 下 `MappingError`（**关键**：验证用 `find` 而非 `at`，见 §5.5） |
| M5 | 多余列 | 默认忽略；`ExtraColumns::Error` 下报错 |
| M6 | 类型不符矩阵 | 逐组合断言报错：`bool←int64`、`int32←overflow`、`double←Decimal`、`string←Blob`、`Timestamp←string`（无 Textual）等 |
| M7 | `Lossy` / `Textual` 字段声明 | 声明后放行，未声明报错 |
| M8 | `enum class` 映射 | 值正确；越界报错 |
| M9 | 自定义 `ValueConverter<T>` | 业务特化生效（强类型 ID 例子） |
| M10 | 写方向 `paramsOf` | 顺序与声明一致；`Generated`/`ReadOnly` 被跳过 |
| M11 | `insertSql` / `updateSql` | 文本正确；表名/列名走 `quoteIdentifier` 转义（注入用例：列名带引号） |
| M12 | `insertAs` 生成键回填 | PG `RETURNING` 多行多列路径；MySQL `insert_id` 兜底路径（C8） |
| M13 | 无主键声明时 `updateAs` | `MappingError`，不生成无条件 UPDATE |
| M14 | 批量 `batchOf` / `insertBatchAs` | 参数行数与顺序正确 |
| M15 | 流式 `queryEachAs` | 逐行映射；回调返回 false 提前终止；行数计数正确 |
| M16 | 游标 `fetchAs` | 与 `fetch` + 映射等价 |
| M17 | 事务内 `queryAs(Session&, ...)` | 与门面版结果一致（一致性矩阵） |
| M18 | **缓存**：映射结果不入缓存；缓存命中仍映射 | 第二次查询驱动零调用且映射结果正确（I1） |
| M19 | **脱敏顺序**：`afterExecution` 脱敏后映射 | 映射拿到的是脱敏后的值（I2） |
| M20 | 异步回调 / future / 协程三形态 | 与同步形态结果**完全一致**（同源语义） |
| M21 | 异步映射失败 | 回调收到 `MappingError`，`items` 为空（I8） |
| M22 | 缓存/治理/统计不受影响 | 慢 SQL 计数、审计计数与纯 ResultSet 查询一致 |
| M23 | 一致性矩阵 | 同一 SQL 的 `query` + 手工映射 vs `queryAs` 结果相等 |

**回归门槛**：`core 147 / async 67 / coro 35` 全绿；CI 7 位矩阵（含 2 个 `coro=ON` 位）全绿。

---

## 11. 风险登记

| # | 风险 | 等级 | 应对 |
|---|---|---|---|
| R1 | 被误用为 ORM（业务开始期待关联加载/自动 SQL） | 中 | 文档首屏与 `guide.md` 非目标章节显式划界；不提供任何元数据驱动能力 |
| R2 | 缺列被 `at()` 的静态 NULL 掩盖 → 缺列与 NULL 混淆 | 高 | 强制 `data().find()`（§5.5）；M4 专项用例 |
| R3 | 异步大结果集映射阻塞完成投递线程（asio 单线程场景） | 中 | I7 文档约束 + 流式 `queryEachAs` 分流；必要时后续评估 D3-B |
| R4 | 模板错误信息难读 | 低 | `static_assert` 友好提示（§4.1）；`ValueConverter` 未特化时给明确提示 |
| R5 | 严格模式抬高存量代码迁移成本（老代码有大量隐式转换假设） | 中 | 迁移期可用 `Lossy` / `Textual` 逐字段放开；错误信息精确给出列名与目标类型 |
| R6 | 跨驱动类型差异（同列在 MySQL 返 `int64`、PG 返 `Decimal`） | 中 | 严格模式会立刻暴露；文档列出常见差异与声明建议；必要时按数据源提供第二套 `RowMapper` 特化的用法示例 |
| R7 | 编译时间膨胀 | 低 | 声明集中；`describe()` 复用（§4.5） |

---

## 12. 文件改动清单

### 新增

| 文件 | 内容 | 依赖 |
|---|---|---|
| `include/dbmw/mapping.h` | `mapping::RowMapper` / `Mapping<T>` / `ValueConverter<T>` / `FieldFlags`；`dbmw::queryAs<T>` 等自由函数；`dbmw::async::queryAs<T>` 三形态（协程段 `DBMW_ENABLE_ASYNC_CORO` 门控） | `common/types.h`、`dbmw.h`、`async/dbmw_async.h`（coro 段含 `async/task.h`） |
| `tests/dbmw_mapping_test.cpp` | §10 的 M1–M23 | mock 驱动（复用 core 测试的风格） |

### 改动

| 文件 | 改动 |
|---|---|
| `include/dbmw/common/types.h` | `ErrorCode` 尾部追加 `MappingError`（C10） |
| `src/common/types.cpp` | `errorCodeToString` 补一行（`:16` 起） |
| `tests/CMakeLists.txt` | 新增 `dbmw_mapping_test` 目标与 `add_test` |
| `CMakeLists.txt` | `project(dbmw VERSION 0.4.0)` → `0.5.0` |
| `docs/roadmap-design-v0.4.0.md` | §1.2 非目标行改写为「完整 ORM（关系映射 / 懒加载 / 脏跟踪 / 自动生成 SQL）」并注明 v0.5.0 引入的适配层边界 |
| `docs/guide.md` / `docs/guide_en.md` | 非目标章节同步；新增"实体映射"使用章节 |
| `README.md` / `README_en.md` | 目录结构补 `mapping.h`；新增实体映射示例小节 |

### 不动（声明）

`include/dbmw/dbmw.h`、`src/dbmw.cpp`、`src/core/database_manager.cpp`、`src/async/async_engine.cpp`、`include/dbmw/async/*.h` —— **零改动**。CI 矩阵不新增构建位（现有 7 位已覆盖 sync/coro 两种配置）。

---

## 13. 里程碑（一次性交付，此处为实现顺序而非可选交付）

| 段 | 内容 | 完成口径 |
|---|---|---|
| M1 | `mapping.h` 声明层 + `ValueConverter` 读方向 + 同步 `queryAs/queryOneAs/queryEachAs` + `MappingError` | M1–M9、M15、M17、M18、M19、M23 绿 |
| M2 | 写方向：`paramsOf/batchOf/insertSql/updateSql/insertAs/updateAs/insertBatchAs` + 生成键回填 | M10–M14 绿 |
| M3 | 异步三形态 + 协程 + 游标 | M20–M22 绿；coro 构建位通过 |
| M4 | 文档（roadmap 非目标修订、guide×2、README×2）+ 版本号 + 全量回归 | core/async/coro 全绿，CI 7 位全绿 |

---

## 附录 A：使用示例

### A. 同步读

```cpp
auto r = dbmw::queryAs<User>("SELECT id,name,email,balance,created_at FROM users WHERE age > ?",
                             {dbmw::common::Value(std::int64_t(18))});
if (!r.status.ok()) { log(r.status.message); return; }
for (auto &u : r.items) use(u);
```

### B. 事务内读 + 写

```cpp
auto st = dbmw::DBMW::transaction([](dbmw::core::Session &s) {
    auto r = dbmw::queryAs<User>(s, "SELECT id,name,email,balance,created_at FROM users WHERE id = ?",
                                 {dbmw::common::Value(std::int64_t(1))});
    if (!r.status.ok()) return r.status;
    if (!r.items.empty()) {
        r.items[0].balance.value = "0.00";
        std::int64_t n = 0;
        return s.execute(dbmw::mapping::updateSql<User>("users"),
                         dbmw::mapping::paramsOf(r.items[0]), n);
    }
    return dbmw::common::Status::OK();
});
```

### C. 异步（回调 / future / 协程）

```cpp
// 回调
dbmw::async::queryAs<User>("SELECT id,name,email,balance,created_at FROM users", {},
                           [](dbmw::EntityResult<User> &&r) { /* ... */ });

// future
auto f = dbmw::async::queryAs<User>("SELECT id,name,email,balance,created_at FROM users", {});

// 协程（C12：实参用具名局部变量，避免 GCC 13 ICE）
dbmw::async::Task<void> demo() {
    dbmw::common::Params p;
    auto r = co_await dbmw::async::queryAsAsync<User>(
        "SELECT id,name,email,balance,created_at FROM users", p);
}
```

### D. 自定义类型接入

```cpp
struct UserId { std::int64_t v; };

namespace dbmw::mapping {
template <> struct ValueConverter<UserId> {
    static common::Status fromValue(const common::Value &val, UserId &out, FieldFlags) {
        if (auto p = std::get_if<std::int64_t>(&val)) { out.v = *p; return common::Status::OK(); }
        return common::Status::error(common::ErrorCode::MappingError, "UserId expects int64");
    }
    static common::Value toValue(const UserId &in) { return common::Value(in.v); }
};
}
```

---

## 附录 B：与既有设计的关系速查

| 既有约定 | 本方案如何遵守 |
|---|---|
| 头文件即 ABI（§1.1） | 只新增头文件与自由函数，不改既有签名（I4） |
| 缓存只存原始 ResultSet（C3 / I10） | 映射在缓存命中之后（I1） |
| 脱敏在 `afterExecution` | 映射在其之后（I2） |
| 不自动追加 `RETURNING`（C8） | `insertSql` 不追加；回填走列名匹配 + `lastInsertId()` 兜底 |
| `ErrorCode` 只能尾部追加（C10） | `MappingError` 追加在 `Overloaded` 之后 |
| 异步三层同源语义 | 映射层三形态均为既有 API 薄封装，治理/重试/取消不变 |
| GCC 13 协程 ICE（C12） | 协程示例与测试一律用具名局部变量传参 |

---

## 14. 实施状态（v0.5.0 已落地）

| 里程碑 | 内容 | 状态 |
|---|---|---|
| M1 | `mapping.h` 声明层 + `ValueConverter` 读方向 + 同步 `queryAs/queryOneAs/queryEachAs` + `MappingError` | ✅ |
| M2 | 写方向 `paramsOf/batchOf/insertSql/updateSql/insertAs/updateAs/insertBatchAs` + 生成键回填 | ✅ |
| M3 | 异步三形态（回调 / future / 协程）+ 游标 `fetchAs<T>` + 事务内 `queryAs<T>(Session&)` | ✅ |
| M4 | 文档（roadmap §1.2 修订、guide×2、README×2）+ 版本号 + 全量回归 | ✅ |

**回归结果**（本地 MinGW GCC 15）：

| 构建形态 | 结果 |
|---|---|
| `DBMW_ENABLE_ASYNC_CORO=ON` | ctest 14/14 通过（`dbmw_mapping_test` 83/83，含 M20–M22 协程段） |
| `DBMW_ENABLE_ASYNC_CORO=OFF` | ctest 13/13 通过（映射层同步部分全绿，协程段按宏跳过） |

**实施期修正**（与本文正文的偏差，以后者为准）：

1. `queryOneAs` 的多行判定放在 `detail::queryOneAsImpl` 内，五个重载共用（正文 §4.3 只给了概念描述）。
2. `Mapping<T>` 的 `writable()` 与 `isDeclared()` 均改为 **public**——写方向 SQL 生成与缺列校验需要跨类调用它们。
3. `updateSql` 的 SET 子句由 `joinIdentifiers(setCols)` 与 `placeholders(setCols.size())` 分别生成后拼接，形如
   ``UPDATE `t` SET `a` = ?, `b` = ? WHERE `id` = ?``（正文起草时的拼接表达式有误，实现以本条为准）。
4. **缺列策略放宽（用户决策，覆盖 §0/§5.5）**：新增 `MissingColumns { Ignore, Error }`，默认 `Ignore`——
   缺列跳过该字段（保持默认构造值）正常返回；`.missingColumns(MissingColumns::Error)` 可拿回严格行为。
   类型不符与 NULL 落非 `optional` 仍保持报错。`M4` 用例随之更新为「默认跳过 + 显式 Error 报错」两条路径。
