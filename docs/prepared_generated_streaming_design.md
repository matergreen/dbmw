# 高优先级官方 API 覆盖设计：预编译语句 / 生成键 / 大参数流式

> 目标：把三个官方驱动都具备、dbmw 抽象层 `IDatabaseConnection` 尚未封装的高频能力接进来：
> 1. **预编译语句复用**（prepare-once / execute-many，连接级句柄缓存）
> 2. **生成键 / 自增 ID**（`execute` 能回吐刚插入的自增主键）
> 3. **大参数流式**（data-at-execution，超大 BLOB 不必整体物化进内存）
>
> 设计约束：**不破坏现有架构不变量**——闸门分离（preGate 只在 DataSource 入口过一次）、
> 结果缓存只在 `DataSource::query` 路径且按 `(sql+类型标记参数)` 键、游标不进结果缓存、
> 故障转移/写缓冲不用于事务、`DBMW_ENABLE_*` 编译期驱动开关、驱动 `.cpp` 辅助函数顺序即作用域。

---

## 0. 现状对照（已覆盖 vs 本次新增）

`IDatabaseConnection` 已声明：`connect/ping/close/isOpen`、`query/execute`（含 `params` 重载）、
`queryEach`、`openCursor`、`executeBatch`、`begin/commit/rollback` + `savepoint` 三件套 +
`TransactionOptions`、`cancel`、`supportsParams`/`escapeLiteral`/`renderSqlForLogging`。

本次在抽象层**新增**（基类默认实现返回 `NotSupported` 或安全委托，老驱动零改动）：

| 能力 | 新增虚函数（IDatabaseConnection） | 新增类型 |
| --- | --- | --- |
| 预编译缓存 | `prepare` / `executePrepared(query)` / `executePrepared(exec)` / `closeAllPrepared` | `PreparedStatementHandle` |
| 生成键 | `execute(..., GeneratedKeys&)` 重载 | `GeneratedKeys` |
| 大参数流式 | `query(..., const StreamParams&)` / `execute(..., const StreamParams&, ...)` / 批量流式重载 | `StreamSource` / `StreamParams` |

---

## 1. 能力一：预编译语句复用（连接级句柄缓存）

### 1.1 抽象层接口

```cpp
namespace dbmw::core {

// 连接级预编译语句句柄。由 prepare() 产出，绑定到"当前这条物理连接"，
// 仅在连接/session 存活期内有效；连接归还/关闭后句柄失效（驱动内部随之释放原生句柄）。
class PreparedStatementHandle {
public:
    // 内部持有驱动原生句柄（MYSQL_STMT* / PG 预备名 / ODBC SQLHSTMT）。
    // 仅 IDatabaseConnection 实现与 Session 可见；上层只当不透明令牌。
};

// 显式预编译：在本连接上 prepare 一条语句，返回可复用句柄。
// typesSample 仅用于推导参数类型签名（占位值即可，不需要真实数据）。
// 不支持服务端绑定的驱动返回 NotSupported。
virtual Status prepare(const std::string &sql,
                       const common::Params &typesSample,
                       PreparedStatementHandle &out);

// 用已编译句柄执行（查询 / 写）。
virtual Status executePrepared(const PreparedStatementHandle &h,
                               const common::Params &params,
                               common::ResultSet &out);
virtual Status executePrepared(const PreparedStatementHandle &h,
                               const common::Params &params,
                               std::int64_t &affected);

// 连接关闭/归还前释放本连接上所有预编译句柄（由 close() 调用）。
virtual void closeAllPrepared();
}
```

**透明自动缓存（核心收益，调用方无感）**：在 `DataSource::query(sql, params)` /
`execute(sql, params)` 内部，当驱动 `supportsParams()` 且配置 `prepared_cache.enabled` 时，
走一条 `getOrPrepare(conn, sql, typesSignature)` 辅助——按 key 在本连接的缓存里查句柄，
没有就 `prepare` 并存入；随后用 `executePrepared` 执行。**对 `DBMW::query` 调用方完全透明，
签名不变**，直接拿到"热点 SQL 只 prepare 一次"的收益。

### 1.2 缓存 key 与存储

- **Key** = 归一化 SQL + 参数类型签名（按位置的有序类型标记序列，从 `Params` 各元素推断）。
  PG / MySQL 在 prepare 阶段就需要类型，必须纳入 key；与结果缓存的 `cacheKey` 思路一致但**相互独立**（一个是语句预备、一个是结果数据）。
- **存储位置**：挂在驱动连接对象上（`MySQLConnection` / `PgConnection` / `OdbcConnection` 各自持有
  `std::unordered_map<Key, NativeStmt>` 或等价的 `preparedName`/`SQLHSTMT` 映射）。
- **生命周期**：
  - 连接借出期间（含 `withSession`/`transaction` 钉住期间）缓存有效。
  - 连接归还连接池后，缓存随连接保留；下次借到同一连接直接复用。
  - 连接被心跳判定失效 / 池驱逐 / `reload` 旧池排空 → 连接对象析构，`closeAllPrepared` 释放全部原生句柄。
  - 池是**每数据源**独立的，prepared 语句不会跨数据源串。
- **并发**：连接在池中一次只服务一个借出者，缓存只在借出期间被本调用者访问，**无需加锁**；
  `withSession`/`transaction` 连同一连接被钉住，整段 session 内缓存稳定。

### 1.3 三驱动实现要点

- **PostgreSQL（libpqxx 7.x）**：`conn.prepare(uniqueName, sql)` 注册命名预备语句（持久到连接关闭），
  执行用 `tx.exec_prepared(uniqueName, params)`。名称用每连接原子计数器生成 `dbmw_ps_<n>`，
  **绝不重名**（重名 PG 报错）。超出 `max_per_connection` 时按 LRU `DEALLOCATE <name>` 释放。
  注意：命名预编译语句与现有 `execParams`（匿名参数执行）是两套机制，互不冲突。
- **MySQL（libmysqlclient）**：`mysql_stmt_prepare` 得到 `MYSQL_STMT*`，存入 map；
  执行前 `mysql_stmt_bind_param` + `mysql_stmt_execute`；超限时 `mysql_stmt_close` 驱逐。
  复用现有 `MysqlBoolArray` / 移动语义等既有写法（避开 `std::vector<bool>` 陷阱）。
- **ODBC（unixODBC）**：`SQLPrepare` 到一条 `SQLHSTMT`（存 map），`SQLExecute` 执行；
  超限 `SQLFreeHandle(SQL_HANDLE_STMT, ...)` 释放。沿用现有 `StmtGuard` 移动构造规范。

### 1.4 门面与 Session API

- 无状态门面 `DBMW::query/execute(params)`：走 1.1 的透明自动缓存，调用方零改动。
- 显式句柄 API放在 **`Session`** 上（句柄绑定具体连接，无状态门面持有不了连接）：
  `Session::prepare` / `Session::executePrepared(...)`，供需要在稳定连接上精细控制的高级用户。
- **闸门/缓存/审计不受影响**：预编译执行仍经 `DataSource::query` 入口的 `preGate`（一次）；
  结果缓存键逻辑不变（缓存命中则连预备执行都跳过）；审计仍按 SQL 文本分类。

### 1.5 配置

```json
{
  "prepared_cache": {
    "enabled": true,
    "max_per_connection": 0
  }
}
```
- `enabled` 默认 `true`（纯性能优化、透明、无副作用）；关掉则退化为现有每次重绑路径。
- `max_per_connection`：`0` = 不限制（靠连接关闭自然回收）；`>0` 触发 LRU 驱逐（PG `DEALLOCATE` / MySQL `mysql_stmt_close` / ODBC `SQLFreeHandle`）。

---

## 2. 能力二：生成键 / 自增 ID

### 2.1 抽象层接口与类型

```cpp
namespace dbmw::common {
// 生成键结果：本质是"被插入行生成的列"组成的结果集。
// 便捷 lastInsertId() 服务于 MySQL 风格单列自增（多行插入取首行 id）。
struct GeneratedKeys {
    ResultSet rows;                       // 每行 = 一条被插记录的生成列
    [[nodiscard]] bool empty() const { return rows.empty(); }
    // MySQL 单列自增便捷取法；多行/多列时取首行首列。
    [[nodict]] std::int64_t lastInsertId() const;
};
}

namespace dbmw::core {
// execute 新增带生成键的重载（含 params 版与非 params 版）。
virtual Status execute(const std::string &sql, std::int64_t &affected,
                       common::GeneratedKeys &out);
virtual Status execute(const std::string &sql, const common::Params &params,
                       std::int64_t &affected, common::GeneratedKeys &out);
}
```

### 2.2 三驱动语义（**诚实跨驱动**，不偷偷改写 SQL）

- **MySQL**：执行后直接 `mysql_insert_id(conn)` → 合成 1 行 1 列 `ResultSet` 填入 `out`。
  多行 `INSERT` 取首行 id（自增连续假设，与官方语义一致）。**无需改 SQL，开箱即得。**
- **PostgreSQL**：PG **没有** `mysql_insert_id` 等价物。规则：**SQL 含 `RETURNING` 时，
  返回的结果集即生成键**；不含 `RETURNING` 则 `out` 为空。dbmw **不**自动给 SQL 追加 `RETURNING`
  （那会破坏语义、且与方言耦合）。上层文档明确：PG 想拿自增 id 就写 `INSERT ... RETURNING id`。
- **ODBC**：插入后若列是自动生成，`SQLGetDescField(IRD, col, SQL_DESC_AUTO_UNIQUE_VALUE)` 标记，
  值已在绑定的结果缓冲里 → 作为生成键行返回。需语句本身返回该列（SQL Server `OUTPUT INSERTED.id`、
  PG/ODBC 走 `RETURNING`）；不可检测时 `out` 为空。

> 统一模型：`GeneratedKeys` 始终是"生成列的结果集"，MySQL 用 `mysql_insert_id` 合成、PG/ODBC 用 RETURNING 直出。
> 这样 `keys.lastInsertId()` 在 MySQL 上顺手，PG/ODBC 上也对" RETURNING 出的行"同样适用。

### 2.3 门面 API

- `DBMW::execute(sql, params, affected, keys)` 与 `Session::execute(... keys)`。
- 闸门/审计/限流：走现有 `execute` 入口，无新增路径。

---

## 3. 能力三：大参数流式（data-at-execution）

### 3.1 抽象层类型与接口

**关键决定**：不扩展 `common::Value`（避免牵连 `cacheKey`、结果映射、游标等既有逻辑），
新增一套**并列参数载体** `StreamParams`。

```cpp
namespace dbmw::common {
// 流式数据源：同步执行期间由驱动按块拉取字节；调用结束后即失效（不跨调用存活）。
class StreamSource {
public:
    using ReadFn = std::function<std::size_t(void *buf, std::size_t n)>; // 返回本块字节数，0=EOF
    StreamSource(ReadFn read, std::optional<std::uint64_t> totalSize = {},
                 bool isBinary = true);
    explicit StreamSource(std::istream &in, bool isBinary = true);     // 便捷构造
    std::size_t read(void *buf, std::size_t n);
    std::optional<std::uint64_t> totalSize() const;   // PG 用：为 NULL 时按 bytea 流式
    bool isBinary() const;                             // true=bytea/blob, false=text/clob
};

// 位置参数，每个要么是普通 Value，要么是流式源。
using StreamParam  = std::variant<Value, StreamSource>;
using StreamParams = std::vector<StreamParam>;
}

namespace dbmw::core {
virtual Status query(const std::string &sql, const common::StreamParams &params,
                     common::ResultSet &out);
virtual Status execute(const std::string &sql, const common::StreamParams &params,
                       std::int64_t &affected, common::GeneratedKeys &out);
virtual Status executeBatch(const std::string &sql, const StreamParamBatch &batch,
                            common::BatchResult &out);
}
```

### 3.2 三驱动实现要点

- **MySQL**：参数按 `mysql_stmt_bind_param` 正常绑定；遇到 `StreamSource` 参数，在
  `mysql_stmt_execute` **之前**循环 `mysql_stmt_send_long_data(stmt, i, chunk, len)` 把流吃完。
  （这要求该语句已 prepare——天然复用能力一的预编译句柄。）
- **ODBC**：把流式参数绑定为 `SQL_LEN_DATA_AT_EXEC(len)`（未知长度用 `SQL_DATA_AT_EXEC`）；
  `SQLExecute` 返回 `SQL_NEED_DATA` 后，循环 `SQLParamData` 取参数序号、`SQLPutData` 按块推送，直到 `SQL_SUCCESS`。
- **PostgreSQL（libpqxx）**：**libpq 协议不支持参数 data-at-execution**——参数必须以完整缓冲随
  `execParams`/`exec_prepared` 一次性发送。故 PG 驱动实现 = 把 `StreamSource` 整段读入 `Blob` 再按普通参数绑定
  （**缓冲降级**，在文档中明确标注为 PG 限制）。上层调用代码保持统一，不受驱动差异影响。
  （真正零拷贝的 PG 大对象走 `blob`/`lo_import`，属另一能力，不在本次范围。）

### 3.3 与审计 / 缓存 / 游标的交互

- **闸门**：流式查询带参数仍经 `DataSource` 入口 `preGate`（一次），不变。
- **结果缓存**：含 `StreamSource` 的查询**不可缓存**（流式内容不是定值，无法参与 `cacheKey`），
  与游标一样按"非缓存"处理；普通 `Value` 参数查询不受影响。
- **审计**：SQL 文本照常审计；流内容**绝不**进日志（与 `include_blob_values=false` 默认一致）。
- **游标**：`openCursor` 未来可接收 `StreamParams`，但非本次必做；先做 `query/execute/executeBatch` 三处。

---

## 4. 配置与错误码变更

- 新增配置：`prepared_cache { enabled, max_per_connection }`（见 1.5）。生成键 / 流式为 API 驱动，无需配置开关。
- 错误码：**尽量复用现有码**（`QueryError` / `NotSupported` / `TxError`）。
  可选新增 `ErrorCode::PreparedStatementError`（覆盖 PG `DEALLOCATE` 失败、预备名冲突等），
  默认实现/老驱动不触发。其余沿用既有。

---

## 5. 兼容性（老驱动不动）

- 所有新增虚函数基类提供默认实现：`prepare`/`executePrepared`/`closeAllPrepared` 返回 `NotSupported`；
  `execute(... GeneratedKeys&)` 默认调用既有的无键 `execute` 并把 `out` 留空；`query/execute/executeBatch(StreamParams)` 默认
  把流读入 `Blob` 后委托给既有 `Params` 路径（即"缓冲降级"成为基类默认行为，连 PG 都可继承该默认）。
- `Params` / `Value` / `cacheKey` / 结果映射 **完全不变** → 现有 125 项 mock 测试与新接口零耦合，应原样通过。
- 内置三驱动实现三能力；自定义驱动（`IDriver`）若不实现，调用方拿到 `NotSupported` 或基类降级行为，编译不破。

---

## 6. 测试

- **单元（mock 驱动）**：确认新虚函数默认 `NotSupported`/安全委托，且不破坏现有 125 项测试。
- **集成（docker-compose 起真实 MySQL/PG/ODBC，补进 CI）**：
  - 预编译缓存：同 SQL 执行 N 次，断言 PG `pg_prepared_statements` 仅 1 条、MySQL `mysql_stmt_prepare` 调用次数为 1（或比对性能）。
  - 生成键：MySQL `execute` 后 `keys.lastInsertId()` 正确；PG `INSERT ... RETURNING id` 后 `keys.rows` 含该行。
  - 大参数流式：用 `std::istringstream` 灌 10MB 数据 `execute` 插入，回读比对字节一致；验证 MySQL `send_long_data` 分块、ODBC `SQLPutData` 分块路径被走到。

---

## 7. 待确认决策点

1. **自动缓存 vs 仅显式句柄**：建议两者都要（透明缓存覆盖常见路径 + `Session::prepare` 给控制权）。是否同意？
2. **PG 生成键不自动补 RETURNING**：需上层写 `RETURNING`（诚实跨驱动）。接受还是希望门面特殊处理 PG？
3. **PG 大参数流式缓冲降级**：接受 PG 缓冲（协议限制），还是本期 PG 直接返回 `NotSupported`、只做 MySQL/ODBC 真流式？
4. **新增 `StreamParams` 类型**（不动 `Value`）：确认，还是你更想扩展 `Value` variant？
5. **是否新增 `ErrorCode::PreparedStatementError`**，还是全复用 `QueryError`？

---

## 8. 落地顺序建议

1. 抽象层（`IDatabaseConnection`）加虚函数 + 默认实现 + 新类型（`PreparedStatementHandle` / `GeneratedKeys` / `StreamSource` / `StreamParams`）。
2. MySQL 驱动：预编译缓存（`MYSQL_STMT*` map）+ 生成键（`mysql_insert_id`）+ 流式（`send_long_data`）。
3. PostgreSQL 驱动：预编译（`conn.prepare`/`exec_prepared` + LRU `DEALLOCATE`）+ 生成键（RETURNING 透传）+ 流式（缓冲降级）。
4. ODBC 驱动：预编译（`SQLPrepare`/`SQLExecute`）+ 生成键（`SQLGetDescField`）+ 流式（`SQLPutData`）。
5. 门面 `DBMW`/`Session` 接入 + `DataSource` 透明缓存路径 + `prepared_cache` 配置。
6. 单元测试（默认行为）+ 集成测试（docker-compose）。
