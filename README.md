# dbmw — C++ 数据库连接中间件

> 📘 English version: [README_en.md](README_en.md)

一个使用 C++17 开发的数据库连接中间件，支持：

- **多数据源**：一份 JSON 配置描述任意多个数据源，按名字分发。
- **连接池**：每个数据源独立的线程安全连接池，支持借出校验、失效重建、最小/最大连接数。
- **池化可开关**：`pool.enabled` 决定是否复用连接；关闭后每次操作新建并关闭一条物理连接，
  适合低频定时任务、短命进程，或数据库侧对长连接有严格限制的场景。上层 API 用法不变。
- **心跳保活**：后台线程周期 `ping` 空闲连接，失效自动回收，并补足到最小连接数。
- **生产连接生命周期**：空闲回收、最大寿命轮换、借出泄漏告警、池运行指标。
- **事务与会话**：`transaction()` 在一条独占连接上执行多条语句，成功提交、失败或抛异常自动回滚。
- **事务增强**：隔离级别、只读事务、整体超时取消和保存点。
- **参数化查询**：`?` 占位符 + 绑定参数，杜绝 SQL 字符串拼接带来的注入风险。
- **预编译语句 / 生成键 / 大参数流式**：连接级预编译句柄缓存（热点 SQL 透明只 prepare 一次）、`execute` 回吐自增主键（`GeneratedKeys`）、超大 BLOB 按块流式写入（`StreamSource`）。详见下文「预编译语句 / 生成键 / 大参数流式」一节。
- **韧性与路由**：只读查询安全重试、指数退避、熔断/半开、主从读写路由及写后读窗口。
- **主库故障转移**：写路径在主不可用时按序切换到候选备用库；全部不可用时可选写缓冲软降级（后台补发，返回 `Buffered`），但绝不用于事务。
- **限流与背压**：按数据源总 QPS 与（可选）单 SQL 指纹 QPS 做令牌桶限速，超限快速失败返回 `RateLimited`，不重试、不打满连接池。
- **SQL 审计与拦截**：执行前对 SQL 做轻量静态分析，可拦截无 WHERE 的 UPDATE/DELETE、无 LIMIT 的 SELECT、只读数据源上的写，以及按指纹黑名单/白名单拦截；灰度期 `action=warn` 仅告警。
- **查询结果缓存**：按 `(数据源 + 原始SQL + 类型标记参数)` 缓存非事务读，LRU + TTL + 内存上限，写后按数据源失效；默认关闭。
- **大数据处理**：逐行回调、批量执行；MySQL/ODBC 按行消费，PostgreSQL 使用服务端游标分块。
- **可观测性**：可控的完整 SQL 日志、慢 SQL 聚合/最近记录、连接池明细快照与操作事件回调。
- **定时统计落日志**：后台线程按 `observability.stats_report` 周期采样连接池与慢 SQL 统计，
  追加写入日志文件（text 可读 / json 一行一对象，便于采集器摄入），不占用业务线程。
- **结果集行数护栏**：每个数据源可设 `max_result_rows`，`query()` 物化超过上限直接报错
  并提示改用 `queryEach()` 流式消费，防止一条漏 LIMIT 的查询吃爆进程内存。
- **安全配置**：环境变量密码、TLS、驱动错误脱敏与结构化 SQLSTATE。
- **热加载**：新配置完整创建后原子切换，并在宽限期内排空旧连接池。
- **多数据库类型**：内置 **MySQL / PostgreSQL / ODBC（SQL Server·Oracle）** 驱动，
  并预留**驱动扩展接口**，新增数据库只需实现 `IDriver` 并注册。

> 状态：核心层（配置/连接池/心跳/事务/参数绑定/门面）已完整实现，
> 并通过 `tests/dbmw_core_test.cpp` 的 125 项行为验证（mock 驱动，无需真实数据库）。
>
> 驱动实现进度：
> - **MySQL 已完整实现**（libmysqlclient）：连接超时/字符集、ping、按列类型映射结果集、
>   显式事务、`mysql_real_escape_string` 转义；并支持 **`mysql_stmt_prepare` 服务端预编译**
>   （连接级句柄缓存）、`mysql_insert_id` **生成键**、超大 BLOB **流式写入**。
> - **PostgreSQL 已完整实现**（libpqxx）：connstring 拼接/超时/字符集、ping、
>   结果集按 OID 映射、显式事务；通过 `pqxx::params` 支持**服务端参数绑定**（内部经 `execParams()` 封装），
>   并支持命名预备语句 **预编译缓存**（LRU `DEALLOCATE`）、`RETURNING` **生成键**、超大 BLOB **流式写入**。
> - **ODBC 已完整实现**（unixODBC）：DSN/连接串、诊断记录、类型映射、原生参数绑定、
>   查询超时/取消、事务与 SQL Server/标准保存点方言；并支持 **`SQLPrepare`/`SQLExecute` 预编译缓存**、
>   `OUTPUT INSERTED`/`RETURNING` **生成键**、超大 BLOB **流式写入**。
>
> 预编译与生成键为三个驱动各自实现；大参数流式（`StreamSource`）当前三驱动统一以**缓冲降级**实现
> （一次性读成 `Blob` 再按普通参数绑定），调用代码保持一致，MySQL `send_long_data` / ODBC `SQLPutData` 真分块为后续增强。
> 三个驱动均由 `DBMW_ENABLE_*` 编译期开关控制。

---

## 目录结构

```
include/dbmw/
  common/    types.h(值/行/结果集/状态/错误码)  observer.h(观测事件)  logger.h(轻量日志)
  config/    datasource_config.h  config_loader.h(解析 JSON)
  core/      idatabase_connection.h(连接抽象 + 流式/批量默认能力)
             connection_pool.h     heartbeat_manager.h  database_manager.h
  driver/    idriver.h  driver_registry.h  driver_factory.h
             mysql_driver.h  postgres_driver.h  odbc_driver.h
  async/     async_types.h(结果体/Handle)  executor.h(IExecutor/线程池)
             dbmw_async.h(异步门面)  task.h(协程层，可选 C++20)
  dbmw.h     (对外门面)
src/         对应实现
tests/       dbmw_core_test.cpp  dbmw_async_test.cpp  dbmw_coro_test.cpp(coro=ON)
examples/    basic_usage.cpp  async_example.cpp
config/      datasources.json.example
third_party/nlohmann/json.hpp  (vendored 单头，离线可用)
scripts/     setup-wsl.sh
```

## 构建（WSL / Linux）

```bash
# 1) 安装工具链（按需开启的驱动选择性安装）
sudo apt update
sudo apt install -y build-essential cmake
# 开启 MySQL:  sudo apt install -y default-libmysqlclient-dev
# 开启 PG:     sudo apt install -y libpqxx-dev libpq-dev
# 开启 ODBC:   sudo apt install -y unixodbc-dev

# 2) 配置 + 构建
mkdir -p build && cd build
cmake ..                                   # 仅核心层
# 启用驱动示例：
# cmake .. -DDBMW_ENABLE_MYSQL=ON -DDBMW_ENABLE_POSTGRES=ON -DDBMW_ENABLE_ODBC=ON
# 启用协程层（可选，仅 task.cpp 提标 C++20）：
# cmake .. -DDBMW_ENABLE_ASYNC_CORO=ON
cmake --build .

# 3) 运行示例（演示加载配置与查询；默认驱动未启用会得到 DriverDisabled 提示）
./examples/dbmw_example_basic ../config/datasources.json.example
```

运行测试（可选，不需要真实数据库，用 mock 驱动验证核心语义）：

```bash
cmake .. -DDBMW_BUILD_TESTS=ON && cmake --build . && ctest --output-on-failure
# 或直接执行： ./tests/dbmw_core_test
```

也可一键执行 `scripts/setup-wsl.sh`（按参数安装依赖并构建）。

## 构建（macOS）

macOS 用 [Homebrew](https://brew.sh) 管理依赖，编译器走系统 **clang++**（需先装 Xcode Command Line Tools）。Homebrew 的包装在 `/opt/homebrew`（Apple Silicon）或 `/usr/local`（Intel），CMake 默认搜索路径未必覆盖，建议显式用 `CMAKE_PREFIX_PATH` 指明客户端库位置。

> **注意**：`DBMW_ENABLE_ODBC` 在本项目的 `CMakeLists.txt` 里**默认是 `ON`**。macOS 上若没装 unixODBC，直接 `cmake ..` 会触发 `FATAL_ERROR`——要么先 `brew install unixodbc`，要么显式 `-DDBMW_ENABLE_ODBC=OFF`。

```bash
# 1) 命令行工具（提供 clang++ / make）
xcode-select --install

# 2) 安装依赖（与 Linux apt 对应的三个客户端库）
brew install mysql-client libpqxx libpq unixodbc
#    - mysql-client 是 keg-only，不会自动软链到 /usr/local，必须显式加入 CMAKE_PREFIX_PATH
#    - libpqxx 依赖 libpq；unixodbc 提供 ODBC 头与 libodbc

# 3) 配置 + 构建（用 brew --prefix 定位头文件与库；分号分隔多个路径）
mkdir -p build && cd build
cmake .. \
  -DCMAKE_PREFIX_PATH="$(brew --prefix);$(brew --prefix mysql-client)" \
  -DDBMW_ENABLE_MYSQL=ON -DDBMW_ENABLE_POSTGRES=ON -DDBMW_ENABLE_ODBC=ON
cmake --build . -j"$(sysctl -n hw.ncpu)"

# 4) 运行示例
./examples/dbmw_example_basic ../config/datasources.json.example
```

> 只启用部分驱动时，删掉对应 `-DDBMW_ENABLE_*` 并去掉 `CMAKE_PREFIX_PATH` 里未安装的包（未安装的 `brew --prefix <pkg>` 会报错）；核心层不需要任何客户端库，可直接 `cmake ..` 构建。

运行测试（可选，mock 驱动、无需真实数据库）：

```bash
cmake .. -DDBMW_BUILD_TESTS=ON && cmake --build . -j"$(sysctl -n hw.ncpu)" && ctest --output-on-failure
```

## 快速使用

```cpp
#include "dbmw/dbmw.h"

dbmw::DBMW::init("config/datasources.json");   // 加载多数据源 + 启动心跳

dbmw::common::ResultSet rs;
auto st = dbmw::DBMW::query("SELECT 1", rs);    // 默认数据源
if (st.ok()) { /* 处理 rs */ }

int64_t n = 0;
dbmw::DBMW::execute("UPDATE t SET c = 1 WHERE id = 2", n); // 默认数据源

dbmw::DBMW::shutdown();
```

指定数据源：`dbmw::DBMW::query("pg", "SELECT now()", rs);`

## 事务与会话

`query()` / `execute()` 每次都会**重新借一条连接**，因此跨多条语句的事务必须先把连接固定下来：

```cpp
auto st = dbmw::DBMW::transaction([](dbmw::core::Session& s) {
    int64_t n = 0;
    if (auto r = s.execute("UPDATE accounts SET bal = bal - 100 WHERE id = 1", n); !r.ok())
        return r;                       // 返回失败 -> 自动 rollback
    return s.execute("UPDATE accounts SET bal = bal + 100 WHERE id = 2", n);
});                                      // 返回成功 -> 自动 commit
```

- 回调**抛异常**同样会触发回滚（异常被捕获后转为 `TxError`，不会逃逸出 `transaction`）。
- 回调内部可以自行 `commit()` / `rollback()`，外层检测到事务已结束就不会重复提交。
- 不需要事务、只想在一条连接上连做几件事（临时表、会话变量等）时用 `withSession()`。

## 参数化查询

SQL 中用 `?` 作占位符，参数值通过 `common::Params` 传入，**不参与 SQL 字符串拼接**：

```cpp
dbmw::common::ResultSet rs;
dbmw::common::Params p{ std::string("O'Brien"), std::int64_t(42) };
auto st = dbmw::DBMW::query("SELECT * FROM t WHERE name = ? AND age > ?", p, rs);
```

- PostgreSQL / MySQL / ODBC 均走**原生参数绑定**。
- 自定义驱动未实现原生绑定时默认返回 `NotSupported`，不会静默退化为 SQL 拼接。
- 仅明确覆盖 `allowsLiteralInterpolation()` 的兼容驱动才会启用字面量插值；扫描器会跳过
  字符串、标识符与注释里的 `?`。
- 占位符数量与参数数量不一致时返回 `QueryError`，不会静默产生错误 SQL。

## 预编译语句 / 生成键 / 大参数流式

三个官方驱动都具备、此前 `IDatabaseConnection` 尚未封装的高频能力，现已统一接入。三者都**不破坏现有架构不变量**
（闸门只在 `DataSource` 入口过一次、结果缓存键不变、故障转移/写缓冲不用于事务）。

> 注意：`prepare` / `executePrepared` 显式句柄 API 只存在于 `Session`（句柄绑定具体连接，无状态门面持有不了跨调用的句柄）；
> 生成键与大参数流式在 `DataSource`（`DBMW::dataSource()` 取得）与 `Session` 上都有。

### 预编译语句复用（连接级句柄缓存）

`DBMW::query(sql, params)` / `execute(sql, params)` 在驱动支持且 `prepared_cache.enabled` 开启时，
内部按 `(归一化 SQL + 参数类型签名)` 在本连接的缓存里查已编译句柄，没有就 `prepare` 并存入，再用
`executePrepared` 执行。**对调用方完全透明、签名不变**——热点 SQL 自动只 prepare 一次。

```cpp
// 透明自动缓存：用法与原来完全一致，无需任何改动
dbmw::common::ResultSet rs;
dbmw::common::Params p{ std::int64_t(1) };
auto st = dbmw::DBMW::query("SELECT * FROM t WHERE id = ?", p, rs);
```

需要在稳定连接上精细控制、或批量复用同一句柄时，用 `Session` 的显式句柄 API：

```cpp
auto st = dbmw::DBMW::transaction([](dbmw::core::Session& s) {
    dbmw::core::PreparedStatementHandle h;
    // typesSample 仅用于推导参数类型签名（占位值即可，不需要真实数据）
    if (auto r = s.prepare("INSERT INTO t(a,b) VALUES(?,?)",
                           dbmw::common::Params{std::int64_t(0), std::string("")}, h); !r.ok())
        return r;
    int64_t n = 0;
    for (const auto& row : rowsToInsert)
        if (auto r = s.executePrepared(h, dbmw::common::Params{row.a, row.b}, n); !r.ok())
            return r;
    return dbmw::common::Status::OK();
});
```

- 句柄生命周期绑定到"当前这条物理连接"，仅在连接/`Session` 存活期内有效；连接归还/关闭后句柄失效，
  驱动随 `close()` 调 `closeAllPrepared()` 释放原生句柄（MySQL `mysql_stmt_close` / PG `DEALLOCATE` / ODBC `SQLFreeHandle`）。
- 缓存挂在驱动连接对象上，连接归还池后保留、下次借到同一连接直接复用；池是每数据源独立的，不会跨数据源串。
- `max_per_connection > 0` 时按 LRU 驱逐最久未用句柄；`0` = 不限制（靠连接关闭自然回收）。
- 预编译执行仍经过 `DataSource` 入口的 `preGate`（一次），结果缓存键逻辑不变，审计仍按 SQL 文本分类。

### 生成键 / 自增 ID（GeneratedKeys）

`execute` 新增带生成键的重载，回吐刚插入生成的列：

```cpp
auto ds = dbmw::DBMW::dataSource();          // 默认数据源（也可传名字取指定源）
int64_t n = 0;
dbmw::common::GeneratedKeys keys;

// MySQL：开箱即得，无需改 SQL
ds->execute("INSERT INTO t(name) VALUES('x')", n, keys);
int64_t id = keys.lastInsertId();            // MySQL 自增主键

// PostgreSQL / ODBC：靠 SQL 自带 RETURNING / OUTPUT 直出，dbmw 不自动补
ds->execute("INSERT INTO t(name) VALUES('x') RETURNING id", n, keys);
if (!keys.empty()) id = keys.rows[0].asInt64(0);  // 取 RETURNING 出来的第一列
```

统一模型：`GeneratedKeys` 始终是"生成列的结果集"——MySQL 用 `mysql_insert_id` 合成一行一列，
PG/ODBC 用 `RETURNING`/`OUTPUT` 直出。**dbmw 不会给 SQL 自动追加 `RETURNING`**（那会改写语义并耦合方言），
因此 PG/ODBC 想拿自增 id 就在 SQL 里自己写 `RETURNING id`。无 `RETURNING` 且非 MySQL 自增时 `keys.empty()` 为真（不报错）。
复用同一 `GeneratedKeys` 对象前调用 `keys.clear()`，避免重试着法残留旧行被当成这次生成的键。

### 大参数流式（StreamSource）

超大 BLOB/CLOB 不必整体物化进内存：用 `StreamSource` 包裹一个同步读取回调或 `std::istream`，
执行期间由驱动按块拉取。这是**输入方向**的流式，与结果集的流式消费（`queryEach`/游标）方向相反，不要混用。

```cpp
auto ds = dbmw::DBMW::dataSource();
std::ifstream f("big.bin", std::ios::binary);
dbmw::common::StreamParams sp{ std::int64_t(1), dbmw::common::StreamSource(f) };
int64_t n = 0;
ds->execute("INSERT INTO t(id, blob) VALUES(?, ?)", sp, n);   // 或 query / executeBatch
```

- `StreamSource(read, totalSize, isBinary)`：自定义 `read(buf, n)` 回调返回本块字节数（0=EOF）；
  也可直接 `StreamSource(std::istream&)` 便捷构造。用 istream 构造时，流必须在本次执行期间保持存活。
- `isBinary=true` 表二进制（bytea/blob），`false` 表文本（clob）。
- **当前三驱动统一以缓冲降级实现**：`StreamSource` 一次性读成 `Blob` 再按普通参数绑定
  （libpq 协议不支持参数 data-at-execution，PG 天然如此；MySQL `send_long_data` / ODBC `SQLPutData` 真分块为后续增强）。
  调用代码保持一致、不受驱动差异影响。
- 含 `StreamSource` 的查询**不进结果缓存**（流式内容不是定值，无法参与 `cacheKey`），审计照常按 SQL 文本分类、流内容绝不进日志。

### 配置

```json
{
  "prepared_cache": {
    "enabled": true,
    "max_per_connection": 0
  }
}
```

- `enabled` 默认 `true`（纯性能优化、透明、无副作用）；关掉则退化为每次重绑路径。
- `max_per_connection`：`0` = 不限制；`>0` 触发 LRU 驱逐。
- 生成键 / 大参数流式为 API 驱动，无需配置开关。

## 超时、取消与事务选项

数据源的 `query_timeout_ms` 会映射到 PostgreSQL `statement_timeout`、ODBC
`SQL_ATTR_QUERY_TIMEOUT` 和 MySQL 客户端读写期限。事务还可设置隔离级别、只读和整体期限：

```cpp
dbmw::common::TransactionOptions options;
options.isolation = dbmw::common::IsolationLevel::Serializable;
options.readOnly = false;
options.timeout = std::chrono::seconds(5);

auto st = dbmw::DBMW::transaction(options, [](dbmw::core::Session& s) {
    s.savepoint("before_optional_step");
    // ...
    return dbmw::common::Status::OK();
});
```

期限到达时中间件会从监控线程请求驱动取消当前语句，并回滚事务。

> **`options.timeout` 是尽力而为的上限，不是硬中断。**
> 驱动实现了 `cancel()`（三个内置驱动都实现了）时语句会被真正打断；驱动未实现时
> C++ 无法安全地强杀正在执行的用户回调，只能等它自然结束后把结果改写成
> `QueryTimeout`。后一种情况下返回的 `message` 会带有 `could not cancel` 提示，
> 便于区分"被及时取消"和"其实没打断，只是事后判了超时"。

取消路径本身是异常安全的：看门狗线程会吞掉驱动 `cancel()` 抛出的任何异常。
线程里逃逸异常会导致 `std::terminate`，这类"为了健壮性而加的机制反而成为崩溃点"
的问题必须在框架侧挡住。

## 流式读取与批量执行

```cpp
std::uint64_t rows = 0;
dbmw::DBMW::queryEach("SELECT * FROM large_table", {},
    [](const dbmw::common::Row& row) {
        // 返回 false 可提前停止。
        return consume(row);
    }, rows);

dbmw::common::ParamBatch batch{
    {std::int64_t(1), std::string("a")},
    {std::int64_t(2), std::string("b")}
};
dbmw::common::BatchResult result;
dbmw::DBMW::executeBatch("INSERT INTO t(id, name) VALUES(?, ?)", batch, result);
```

**批量执行是原子的**，三个驱动行为一致：调用方未开事务时中间件自动包一层事务，
中途任何一组失败都整批回滚，且 `BatchResult` 不会留下部分影响行数（避免调用方
误以为前几组已落库）。调用方已在事务中时则沿用外层事务，回滚范围由调用方决定。

> 实现注意：驱动若覆盖 `executeBatch` 追求更高性能（数组绑定 / COPY），
> 必须同时覆盖 `inTransaction()` 返回真实事务状态，并保持同样的原子性保证。

## 游标（Cursor）

`query()` 一次借连接、物化全部结果、归还；`queryEach()` 流式但每条回调内仍是一次性消费。
**游标**则把连接生命周期从"借→用→还"变成"借→钉住→取 N 次→显式关→还"：
一条物理连接（PostgreSQL 上连带其事务）被游标持有，直到 `close()` 或析构才归还。
适合"结果集大、想按批可控消费、且不想一次物化进内存"的场景。

```cpp
dbmw::core::CursorOptions opts;
opts.batch_size = 1000;          // 每次 fetch 预取行数（也可用配置 default_batch_size 兜底）
opts.auto_transaction = true;    // PG 未开事务时由游标自建事务兜底

std::unique_ptr<dbmw::core::Cursor> cur;
auto st = dbmw::DBMW::openCursor("SELECT * FROM large_table WHERE k > ?",
                                 dbmw::common::Params{std::int64_t(0)}, opts, cur);
if (!st.ok()) { /* 处理错误 */ }

dbmw::common::ResultSet batch;
while (cur->fetch(0, batch).ok() && cur->hasNext()) {  // fetch(0) = 按 batch_size 取
    consume(batch);
    batch.clear();
}
cur->close();   // 显式归还连接；不调也会在析构时关 + 还
```

门面 `DBMW::openCursor` 有两个重载：默认数据源，或指定数据源名。事务/会话内另可用
`Session::openCursor(...)`（连接不额外占用，随会话结束归还）。`fetch(n, out)` 把至多 n 行**追加**
写入 `out`（不清空，多次 fetch 可累积同一结果集）；`n == 0` 由驱动按 batch_size 决定。`fetchRow`
取单行、`close` 显式关闭（幂等）、`isOpen` / `hasNext` / `rowsFetched` 暴露状态。

### 两种绑定

- **`OwnsHandle`（独立游标，默认）**：从连接池借一条连接并钉住，直到游标关闭/析构才归还。
  期间该连接不参与池的其他借用，适合长时间、跨多次 fetch 的消费。
- **`BorrowedInSession`（会话内游标）**：在 `transaction` / `withSession` 内打开，复用会话已有的
  那条连接（及若已开的事务快照），不额外占用池连接；`close()` 只关服务端游标、不归还连接，
  连接仍归 `Session`，随其析构归还。

### 各驱动的行为

- **PostgreSQL**：服务端游标 `DECLARE CURSOR` + `FETCH FORWARD n` + `CLOSE`。游标必须活在事务里——
  已在事务中则借用现有事务；否则 `auto_transaction=true` 时自建 `pqxx::work` 兜底（关闭时提交），
  `auto_transaction=false` 且无事务则直接报 `CursorError`（不静默降级）。`scrollable=true` 仅当
  配置允许时生效，否则返回 `NotSupported`。
- **MySQL**：非缓冲结果集（`mysql_stmt_*` 且**不**调 `mysql_stmt_store_result`），按批
  `mysql_stmt_fetch` 流式消费，结果不落客户端内存；无事务要求。
- **ODBC**：真游标（`SQL_ATTR_CURSOR_TYPE` + `SQLFetch`）；配置 `allow_scrollable` 时设
  `SQL_CURSOR_STATIC` 支持滚动，其余驱动不支持滚动（`scrollable=true` 返回 `NotSupported`）。

### 配置与资源护栏

每数据源可在 JSON 里配：

```json
{
  "cursor": {
    "enabled": true,
    "default_batch_size": 256,
    "max_open_cursors": 0,
    "allow_scrollable": false
  }
}
```

- `enabled=false`：该数据源开游标直接返回 `NotSupported`。
- `default_batch_size`：调用方用驱动默认 batch_size 时以此兜底。
- `max_open_cursors`：每数据源并发游标上限，**>0 时启资源护栏**（原子 CAS 配额，超限返回
  `CursorLimit`）；`0` = 不限制。
- `allow_scrollable`：是否允许滚动游标（仅 ODBC 生效）。

游标经 `preGate`（审计 + 限流）但**不进查询缓存**（流式结果不可直接缓存，且可能跨事务快照）。
游标以 `OperationType::Select` 过审计，据此**豁免 `require_limit_select`**——游标分批消费、本就有界，
强制 LIMIT 会废掉"全量游标扫描"这一正当用法；`enforce_read_only` / `block_no_where_dml` /
黑白名单等对游标照常生效（审计主体仍按 SQL 文本分类）。

## 重试、熔断与读写路由

- 只重试 `Status::retryable == true` 的查询错误。退避时长带真随机抖动，
  用于打散并发重试、避免故障恢复瞬间的惊群。
- 写入和批量写默认不重试；只有显式设置 `retry_writes: true` 才会重试。
- 流式查询一旦向回调交付过数据便不会重放，避免重复消费。
- 数据源组的查询按权重轮询副本；写入、会话和事务始终走主库。
- 发生过写之后的 `read_after_write_ms` 窗口内，读请求固定走主库。
  会话（`withSession`）里的写同样会触发——只要回调中成功执行过
  `execute` / `executeBatch`，窗口内的读就不会打到从库。
- 副本发生连接类错误或熔断时，可自动回退主库。
- **熔断覆盖所有入口**：查询、写入、流式、批量、会话和事务都过同一个闸门。
  会话与事务只做快速失败、不自动重试（回调内容未必幂等）。

```json
{
  "groups": [{
    "name": "app",
    "primary": "main",
    "replicas": [{"name": "replica_1", "weight": 2}, "replica_2"],
    "read_after_write_ms": 1000,
    "fallback_to_primary": true
  }]
}
```

调用 `DBMW::reload(path, grace)` 可原子加载新配置，并等待旧连接池中的在途操作归还。

## 限流、审计、缓存与主库故障转移

这四项能力都是**默认关闭、可整体开关**，且只在 `DataSource` 入口过一次闸门——组转发给叶子走 `*Ungated` 内部路径，重复扣令牌会让配置的 QPS 上限凭空腰斩、重复审计会刷出成倍告警。

### 主库故障转移（failover）

组配置 `failover.primaries` 给出有序可写候选（主自动置顶）。写路径按序挑选**未熔断**的候选执行；全部不可用时：

- 若配了 `failover.write_buffer`，写请求进入有界内存队列，由后台 flush 线程在主恢复后补发，立即返回 `Buffered`（**软降级**：入队即返回，不代表已提交，进程崩溃会丢数据）；
- 否则返回 `CircuitOpen`（标记为可重试，由上层重试/熔断处理）。

约束：**故障转移和写缓冲都不用于事务**——事务回调未必幂等，重放可能造成重复写入，因此主不可用时事务直接失败，由调用方决定补发。写路径只在「连接类/熔断」失败时换节点，业务失败（如唯一键冲突）直返不换节点。

```json
{
  "groups": [{
    "name": "app",
    "primary": "main",
    "replicas": [{ "name": "replica_1", "weight": 2 }],
    "read_after_write_ms": 1000,
    "failover": {
      "primaries": ["main_standby"],
      "require_healthy": false,
      "write_buffer": {
        "enabled": false,
        "max_queue": 1000,
        "ttl_ms": 30000,
        "flush_interval_ms": 1000
      }
    }
  }]
}
```

### 限流与背压（rate_limit）

令牌桶限速，优先失败而非把连接池打满后雪崩。超限返回 `RateLimited`（`retryable=false`，调用方应本地排队或降级，不要重试——重试会放大流量）。

```json
{
  "rate_limit": {
    "enabled": false,
    "global_qps": 0,
    "per_fingerprint_qps": 0,
    "burst": 0,
    "fingerprint_mode": "off"
  }
}
```

- `global_qps`：每数据源总 QPS 上限（令牌桶 refill 速率）；`per_fingerprint_qps`：单 SQL 指纹 QPS（保护热点语句）。
- `burst`：突发容量，0 = 等于对应 qps。
- `fingerprint_mode`：`off`（不按指纹）/ `template`（结构化模板）/ `full`（模板+参数）。只有启用指纹限流时才计算指纹，纯总量场景不付这笔开销。

### SQL 审计与拦截（sql_audit）

执行前对 SQL 做轻量静态分析（启发式分类，非完整解析器，可能误判动态 SQL/存储过程）。命中策略时按 `action` 决定：

- `block`：拦截，返回 `SqlBlocked`；
- `warn`：仅记录告警、放行（**灰度期默认**，用于评估"这条策略会拦掉多少流量"，靠 `SqlAuditor::stats()` 计数判断何时切到 block）。

```json
{
  "sql_audit": {
    "enabled": false,
    "action": "warn",
    "block_no_where_dml": false,
    "require_limit_select": false,
    "enforce_read_only": false,
    "log_blocked": true,
    "blacklist_fingerprints": [],
    "whitelist_fingerprints": []
  }
}
```

- `block_no_where_dml`：无 WHERE 的 UPDATE/DELETE 拦截（防全表误改/误删）。
- `require_limit_select`：无 LIMIT 的 SELECT 拦截（防一次性拉全表）。**游标(`openCursor`)豁免此规则**，
  详见上文"游标"一节。
- `enforce_read_only`：配合 group 的 `read_only`，拦截只读数据源上的任何写。
- `blacklist_fingerprints`：命中即拦截；`whitelist_fingerprints`：非空时"仅放行名单内"，其余一律拦截（允许列表模式）。

审计在单条 `query`/`execute` 走 `DataSource` 入口时执行；`withSession`/`transaction` 的语句由用户回调临时拼出，入口处看不到，因此下沉到 `Session` 逐条把关，且只对会话显式启用审计的 `Session` 执行（不会重复审）。

### 查询结果缓存（query_cache）

仅缓存 `DataSource::query` 路径的非事务、非会话读。key = **原始 SQL + 带类型标记且长度前缀的参数序列**（不用结构模板——模板会把字面量折成 `?` 导致不同取值撞同一 key；不用 `valueToString`——会丢类型信息让 `1` 与 `"1"` 撞 key）。

```json
{
  "query_cache": {
    "enabled": false,
    "ttl_ms": 60000,
    "max_entries": 1000,
    "max_memory_bytes": 0,
    "cache_on_replica_only": false
  }
}
```

- LRU 淘汰 + TTL 过期 + 内存上限（单条结果集超过 `max_memory_bytes` 直接不缓存，否则会为放它一个清空整缓存）。
- `cache_on_replica_only=true` 时只缓存打到副本的读，主库强一致读不缓存。
- 写后按数据源名失效（`markWrite`），避免 read-after-write 读到旧结果。
- 命中率/淘汰/失效计数由 `QueryCache::stats()` 暴露，缓存关着时这些计数仍累计、不随热加载清空。

## 缓存机制详解

中间件内共有**两套真正的 KV 缓存**，外加一处统计型 LRU（慢 SQL 聚合）与两处 TTL 生命周期回收（连接池、写缓冲）。其中两套 KV 缓存在 `datasources.json` 顶层各自有独立配置块（`query_cache` 与 `prepared_cache`），二者**互相独立、不要混用**：

- `query_cache` 缓存的是**结果数据**，键是 `(SQL + 参数值)`；
- `prepared_cache` 缓存的是**语句句柄**，键是 `(SQL + 参数类型签名)`。

### 1. 查询结果缓存（QueryCache，全局单例）

仅作用于 `DataSource::query` 的叶子读路径（非事务、非会话读）。组转发叶子用数据源自身名字作为 key 前缀，使主库与副本的同一条 SQL 成为两条独立缓存项，写后失效才能按节点精确清除。

实现（`src/core/query_cache.cpp`）：全局单例，`std::unordered_map<std::string, Entry> store_` + `std::list<std::string> lru_`（最近使用在表头），一把 `std::mutex mtx_`。开关 `enabled_` / `replicaOnly_` 用 `std::atomic` 镜像——**热路径先无锁读原子标志，缓存关着时连 mtx_ 都不抢**。命中率/淘汰/失效计数均为原子量，`QueryCache::stats()` 暴露，热加载清空缓存不清计数（进程累计量）。

**KV 内容：**
- **key** = `数据源名 + '\0' + cacheKey(sql, params)`，其中 `cacheKey` = 原始 SQL + `\x1e` + 参数个数 + 每参数（`\x1f` + 类型标记 + 长度前缀值）。逐类型打标记（`n`/`b`/`i`/`d`/`t`/`s`/`x`）：double 按位序列化（十进制会丢精度）、string/blob 加长度前缀，**确保不同参数必得不同 key**。这里用的是参数**值**，不是结构模板——模板会把字面量折成 `?` 导致不同取值撞同一 key。
- **value** = `Entry { ResultSet rs; expire; list::iterator lru; bytes; }`，存的是结果集**深拷贝** + TTL 时刻 + 近似字节数。

**过期策略（三重）：**
1. **TTL**：`expire = now + ttl_ms`，`get()` 时过期即 `eraseLocked` 判 miss；`ttl_ms<=0` 整体当关闭。
2. **LRU 容量淘汰**：`evictLocked` 按 `max_entries`（条目数）与 `max_memory_bytes`（近似字节，`approxBytes` 估列名+各列值）双上限，从 `lru_` 尾部淘汰；单条超内存上限直接不收。
3. **写后失效**：`markWrite()` → `QueryCache::invalidate(数据源名)`，按 `数据源名\0` 前缀清掉该数据源全部项。`configure()` 变更配置时整体清空旧数据，避免新旧 TTL/上限混用。

**为什么**：读多写少、重复查询（字典表/配置表）省去重复 DB 往返；代价是最终一致，故**默认关**；`cache_on_replica_only=true` 时只缓存副本读，主库强一致读不缓存。

### 2. 预编译语句缓存（PreparedCache，连接级句柄缓存）

三驱动（`MySQLConnection` / `PostgresConnection` / `OdbcConnection`）各自在 `prepare()` 内维护一份本连接的句柄缓存。门面 `Session::runPreparedQuery` / `runPreparedExec` 在 `preparedPathUsable()` 时调 `conn->prepare`，由驱动内部查"本连接"的缓存——这就是 `DataSource::query/execute(params)` 的**透明自动缓存**（用法见上文「预编译语句复用」）。要生成键时不走这条路径（见下文注意事项）。

实现：每个连接对象持有 `std::unordered_map<std::string, PreparedStatementHandle> preparedCache_` + `std::list<std::string> preparedLru_` + 递增序号（生成句柄 id）+ `preparedLimit_`（每连接上限）。连接归还池后 map 保留、下次借到同一连接直接复用；连接关闭 `close()` → `closeAllPrepared()` 释放全部原生句柄。

**KV 内容：**
- **key** = `sql + common::paramTypeSignature(typesSample)`，用**参数类型签名**而非参数值——同 SQL 不同参数类型在 prepare 阶段必须视为不同语句。
- **value** = `PreparedStatementHandle { uint64_t id; void* native; }`：`native` 对 MySQL 是 `MYSQL_STMT*`、PG 是 `nullptr`（名字另存 `preparedNames_`）、ODBC 是 `SQLHSTMT`。存的是**原生服务端预备语句句柄**，不是结果。

**过期策略：**
- **LRU 容量淘汰**：`max_per_connection > 0` 时，插入后 `while(size > limit)` 从 `lru_` 头（最久未用）淘汰，并 `mysql_stmt_close` / `conn_->unprepare` / `SQLFreeHandle` 释放原生句柄；`0` = 不限制（随连接关闭回收）。命中时把 key 移到 `lru_` 尾。
- 连接关闭：`closeAllPrepared()` 释放本连接全部句柄。

**为什么**：热点语句 prepare-once / execute-many，省服务端硬解析 + 参数类型推导往返；**不改变任何查询结果**，纯性能优化，故**默认开**。

> 注意：被 LRU 淘汰的句柄若仍被上层持有会悬空——这是 LRU 固有取舍，默认不限制则不触发，显式设上限即表示接受。

### 3. 慢 SQL 聚合统计缓存（Observer LRU，非业务数据）

`observer.cpp` 的慢 SQL 聚合：被判定为慢 SQL 时按 `sqlFingerprint` 聚合到 `g_slowStats`（低频路径，专用锁，不阻塞高频快照读取）。KV：key=指纹（结构模板哈希），value=聚合统计（count/duration/histogram）。按 `aggregate_capacity` 上限淘汰（O(1) LRU）；`histogramBucketsMs` 分桶变化（旧样本无法无损重分桶）时整体清空。与上面两套缓存完全独立，纯观测用途。

### 缓存与一致性边界（重要）

- **`StreamParams`（大参数流式）和游标刻意不进结果缓存**：流式内容不是定值，无法参与 `cacheKey`（要求同参数必得同结果）；审计照常按 SQL 文本分类、流内容绝不进日志。
- **生成键路径不进预编译缓存**：`executePrepared` 拿不到 `RETURNING` 的结果集，为省一次 prepare 让调用方静默拿不到主键是本末倒置。
- **写缓冲 / 连接池的 TTL** 是对象生命周期过期，非 KV 缓存。

## 异步 API（v0.2.0：回调 / future / 协程）

三种调用形态共享同一条执行管线——治理闸门（审计/限流/熔断/缓存）、重试退避、语句超时、取消——只是结果交付方式不同。在配置中开启 `async`：

```json
{ "async": { "enabled": true, "threads": 4, "queue_size": 4096 } }
```

`threads` 是 worker 数（0 = hardware_concurrency），另有 1 个 timer 线程负责重试退避与超时检查；队列满时新操作以 `Overloaded` 快速失败（显式背压，不是隐式排队）。`DBMW::shutdown` 会先拒绝新操作、等在途操作归零后再停执行器与连接池。

**回调式（热路径）**——完成回调由完成调度器投递，绝不在调用栈上执行；返回的 `Handle` 支持取消：

```cpp
dbmw::async::Options opts;
opts.timeout = std::chrono::milliseconds(2000);   // 语句整体期限（兜底）
auto h = dbmw::async::query("SELECT id FROM users WHERE age > ?",
                            {dbmw::common::Value(std::int64_t(18))},
    [](dbmw::async::QueryResult &&r) {            // 跑在完成调度器线程，须短小
        if (r.status.ok()) useRows(std::move(r.rows));
    }, opts);
// 需要中途放弃时：h.cancel() —— Queued 不碰池；Running 尽力转发驱动 cancel
```

**future 式（便利形态）**——无取消能力（需要取消用回调式拿 `Handle`）；未取值即析构是合法用法：

```cpp
auto fut = dbmw::async::execute("UPDATE users SET active = 1 WHERE id = ?",
                                {dbmw::common::Value(std::int64_t(7))});
auto r = fut.get();   // r.status / r.affected
```

**协程式（可选，C++20）**——惰性 `Task`，`co_await` 时才启动；未 `co_await` 直接析构 = 安全放弃。治理/重试/取消/超时与回调形态完全同源，协程恢复线程 = 完成调度器线程：

```bash
cmake .. -DDBMW_ENABLE_ASYNC_CORO=ON   # 仅 task.cpp 提标 C++20，其余 TU 仍为 C++17
```

```cpp
#include "dbmw/async/task.h"   // 本 TU 必须以 C++20 编译

dbmw::async::Task<void> demo() {
    // 参数先具名构造：co_await 实参里的花括号临时会触发 GCC 13 ICE（见下方注意事项）
    dbmw::common::Params params;
    params.push_back(dbmw::common::Value(std::int64_t(18)));

    auto q = co_await dbmw::async::queryAsync("SELECT id FROM users WHERE age > ?", params);
    if (q.status.ok()) useRows(std::move(q.rows));

    auto tx = co_await dbmw::async::transactionAsync({}, [](dbmw::core::Session &s) {
        std::int64_t n = 0;
        return s.execute("UPDATE users SET active = 1", n);  // 非 Ok 自动回滚
    });
}

dbmw::async::run(demo());   // 受控 fire-and-forget：跑完自毁，不悬垂
```

**自定义执行器（asio 接入）**：`dbmw::async::setExecutor(...)` 注入 `IExecutor` 适配器后，完成回调与协程恢复发生在你自己的事件循环线程上（适配器形状见 `examples/async_example.cpp` 第 5 段）。

约束与注意：

- 回调与事务 fn 跑在 worker 上，须短小、线程安全；**事务 fn 内部禁止调用 `dbmw::async::*`**（池偏小时互相等连接造成活锁），直接用同步 `Session` 方法。
- `run()` 启动的顶层协程内未捕获异常会 `terminate`（不静默吞掉）；异常应协程内处理，或经 `co_await` 链传给有 `try/catch` 的外层。
- 协程体不要用捕获局部引用的 lambda——闭包临时对象先于异步完成销毁，捕获会悬垂；用具名函数返回 `Task`。
- GCC 13 已知缺陷：`co_await` 实参中直接写非平凡花括号临时（如 `{Value(1)}`）会触发编译器 ICE（PR109227 系）；参数先具名构造再传入即可规避，GCC 14+ / Clang / MSVC 不受影响。
- 完整设计（含排水顺序、超时判定与一致性测试矩阵）见 `docs/async-design-v0.2.0.md`。

## 可观测性

完整 SQL、慢 SQL 与池指标通过 `observability` 配置；完整参数值默认关闭：

```json
{
  "observability": {
    "sql_log": {
      "enabled": false,
      "mode": "template",
      "level": "debug",
      "slow_only": false,
      "sample_rate": 1.0,
      "max_sql_length": 8192,
      "max_param_length": 256,
      "include_string_values": false,
      "include_blob_values": false
    },
    "slow_sql": {
      "enabled": true,
      "threshold_ms": 500,
      "aggregate_capacity": 1000,
      "recent_capacity": 200,
      "retain_rendered_sql": false,
      "max_sql_length": 4096,
      "histogram_buckets_ms": [10, 50, 100, 200, 500, 1000, 3000, 10000]
    },
    "pool_metrics": { "enabled": true },
    "stats_report": {
      "enabled": true,
      "interval_ms": 60000,
      "file": "logs/dbmw_stats.log",
      "format": "text",
      "include_pool": true,
      "include_slow_sql": true,
      "slow_sql_limit": 10
    }
  }
}
```

`stats_report` 由一条后台线程按 `interval_ms` 周期采样并追加写入 `file`（父目录自动创建；
`file` 为空则只走 logger）。`format` 支持 `text`（多行可读）与 `json`（每次一行一个对象）。
`interval_ms` 低于 1000 会被静默抬到 1000，避免高频写文件反过来拖慢业务。
落盘内容与 `allPoolStats()` / `slowSqlStats()` 口径一致。统计失败永远只吞异常、不影响业务。

`sql_log.mode="full"` 会按实际驱动方言渲染参数，但仍只用于诊断，数据库执行继续使用
原生参数绑定。字符串与 BLOB 可能包含密码、Token 或个人数据，只有显式打开对应的
`include_*_values` 后才会进入日志；SQL 和单参数都有长度上限。

```cpp
dbmw::DBMW::setObserver([](const dbmw::common::OperationEvent& event) {
    // event: 数据源、操作类型、耗时、结构化状态、行数和 SQL 指纹。
    // SQL 日志与慢 SQL 均未开启时，默认仍不包含 SQL 或参数。
});

auto topSlow = dbmw::DBMW::slowSqlStats(20, "main");       // 平均耗时倒序
auto recent = dbmw::DBMW::recentSlowSql(50, "main");      // 最近发生倒序
dbmw::DBMW::clearSlowSqlStats();

dbmw::core::ConnectionPool::Stats stats;
if (dbmw::DBMW::poolStats(stats, "app")) {
    // min/max、utilization()、idle/borrowed/waiting、高水位、借出等待耗时、淘汰计数等。
}

auto physicalPools = dbmw::DBMW::allPoolStats();
```

慢 SQL 使用参数化模板指纹聚合，并通过固定容量与耗时直方图控制内存。观察器异常会被隔离，
不会改变数据库操作结果。数据源组的 `poolStats` 会聚合成员，`allPoolStats` 则返回每个物理池，
便于定位具体主库或副本。

## 错误码

`common::Status` 携带 `ErrorCode`，可用 `common::errorCodeToString()` 转成字符串。

| 错误码 | 含义 |
| --- | --- |
| `Ok` | 成功 |
| `ConfigError` | 配置解析/校验失败 |
| `ConnectionFailed` | 连接建立失败 |
| `QueryError` | 查询失败 / 参数数量不符 |
| `QueryTimeout` | 查询或事务超过期限 |
| `Cancelled` | 操作被取消 |
| `ConstraintViolation` | 唯一键、外键、非空等约束冲突 |
| `Deadlock` | 死锁或序列化失败，可结合 `retryable` 判断重试 |
| `PingFailed` | 心跳失败 |
| `TxError` | 事务失败（含回调抛异常） |
| `PoolExhausted` | 连接池耗尽（含借出等待超时） |
| `PoolClosed` | 连接池已关闭（`shutdown()` 之后仍在借连接） |
| `CircuitOpen` | 数据源熔断中，未触达数据库 |
| `RateLimited` | 被限流（令牌桶耗尽），非重试，应本地排队或降级 |
| `SqlBlocked` | SQL 被审计策略拦截（如只读写、无 WHERE 的 DML） |
| `Buffered` | 写已入写缓冲，尚未提交（软降级，进程崩溃会丢，且不重试） |
| `NotConnected` | 连接未建立/已断开 |
| `DriverDisabled` | 驱动未在编译期启用 |
| `UnknownDriver` | 未知数据源类型 |
| `NotSupported` | 驱动未实现该能力 |
| `CursorClosed` | 游标已关闭或被移动（move-from）后调用 fetch/close |
| `CursorLimit` | 超过每数据源并发游标上限（max_open_cursors 非 0） |
| `CursorError` | 游标操作失败（DECLARE/FETCH/CLOSE 或驱动取行错误） |

## 安装与下游集成

dbmw 可作为 CMake 包安装，下游用 `find_package(dbmw)` 直接接入（nlohmann/json 随包自带，无需再 `find_package`）：

```bash
mkdir -p build && cd build
cmake .. -DDBMW_ENABLE_POSTGRES=ON   # 按需开启驱动
cmake --build .
cmake --install . --prefix /usr/local
```

### CMake 下游工程

```cmake
cmake_minimum_required(VERSION 3.16)
project(my_app LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

find_package(dbmw REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE dbmw::dbmw)
```

`dbmw::dbmw` 的 PUBLIC 依赖（`dbmw::nlohmann_json`）随包自动带入。`DBMW::shutdown()`
退出前务必调用，回收连接池与心跳线程。

### 非 CMake 工程（pkg-config）

安装后会生成 `dbmw.pc`：

```bash
g++ main.cpp $(pkg-config --cflags --libs dbmw) -o my_app
```

### 驱动客户端库（务必阅读）

开启某个驱动后，安装包**只包含** `libdbmw.a` 与头文件，**不含**对应数据库客户端库
（libpqxx / libmysqlclient / unixODBC）。由于 dbmw 是静态库，这些客户端库需由下游自行
提供，否则链接时报未定义符号：

- 开启 MySQL  → 下游 `apt install default-libmysqlclient-dev` 并链接 `-lmysqlclient`
- 开启 PG     → 下游装 `libpqxx-dev libpq-dev`，链接 `-lpqxx -lpq`
- 开启 ODBC   → 下游装 `unixodbc-dev`，链接 `-lodbc`

`find_package(dbmw)` 与 `dbmw.pc` 不会自动补这些链接（静态库 + 纯路径依赖无法跨包传播）。

> **预期行为（开箱提示）**
> - 默认 `DBMW_ENABLE_*` 全 OFF；未编译期启用的驱动，调用返回 `DriverDisabled`。
> - 程序退出前务必调用 `DBMW::shutdown()` 回收连接池与心跳线程。

## 扩展新数据库类型

1. 在 `include/dbmw/driver/` 新增 `xxx_driver.h/.cpp`，实现 `MySQLConnection`
   风格的 `IDatabaseConnection` 与 `IDriver`。
2. 在 `.cpp` 中调用 `DriverRegistry::instance().registerDriver("xxx", ...)`。
3. （可选）在 `driver_factory.cpp` 的 `registerBuiltinDrivers()` 中登记，
   或在使用方启动时自行注册。
4. JSON 配置里 `type` 填 `"xxx"` 即可被识别。

基础连接方法仍保持精简；生产驱动应另外覆盖带参数的 `query`/`execute` 并让
`supportsParams()` 返回 `true`。未实现原生绑定时会明确返回 `NotSupported`。
`queryEach`、`executeBatch`、事务选项、保存点和取消都有可选扩展点；流式和批量方法有
兼容默认实现，但大数据驱动应覆盖为游标/按行抓取和数组绑定。

两个容易踩的契约：

- **有事务状态的驱动必须覆盖 `inTransaction()`**。基类默认返回 `false`，
  会导致 `executeBatch` 的默认实现在调用方已开的事务里再套一层 `begin`——
  在 MySQL 上这等于隐式 `COMMIT` 掉调用方的上半段。
- **`cancel()` 从不允许抛异常逃逸**。它会被事务超时的监控线程跨线程调用，
  未捕获的异常会 `std::terminate` 掉整个进程。驱动内部请自行包好 `try/catch`。

## 配置说明（datasources.json）

| 字段 | 含义 |
| --- | --- |
| `default_datasource` | 默认数据源名称 |
| `heartbeat_interval_ms` | 心跳间隔 |
| `pool.enabled` | 是否启用连接池（默认 `true`）；关闭后不复用连接、不预热、不受 min/max 约束 |
| `pool.min` / `pool.max` | 每数据源连接池最小/最大连接 |
| `pool.borrow_timeout_ms` | 借出连接的最长等待时间，超时返回 `PoolExhausted`（0 = 不等待） |
| `pool.idle_timeout_ms` | 超过该时间的多余空闲连接回收到 `min` |
| `pool.max_lifetime_ms` | 物理连接最长寿命，到期轮换 |
| `pool.leak_detection_threshold_ms` | 借出超过该时长记录泄漏告警（0 = 关闭） |
| `retry.*` | 最大次数、指数退避上下限、是否允许重试写入 |
| `circuit_breaker.*` | 连续失败阈值与熔断开放时间 |
| `rate_limit.*` | 限流：每数据源总 QPS / 单 SQL 指纹 QPS / 突发容量 / 指纹模式（默认关闭） |
| `sql_audit.*` | SQL 审计：动作（block/warn，默认 warn）、无 WHERE 的 DML、无 LIMIT 的 SELECT、只读拦截、指纹黑白名单（默认关闭） |
| `query_cache.*` | 查询结果缓存：TTL、条目数上限、内存上限、仅副本缓存（默认关闭） |
| `prepared_cache.*` | 预编译语句缓存：每连接最大句柄数（0=不限，LRU 驱逐）、是否启用（默认 true） |
| `datasources[].name` | 数据源名（唯一） |
| `datasources[].type` | `mysql` / `postgres` / `odbc` / 自定义 |
| `datasources[].host/port/user/password/database` | 连接参数 |
| `datasources[].dsn` | ODBC 数据源名 |
| `datasources[].password_env` | 从环境变量读取密码，优先于明文 `password` |
| `datasources[].query_timeout_ms` | 单条语句执行期限 |
| `datasources[].max_result_rows` | `query()` 单次物化的最大行数；超限返回错误并提示改用 `queryEach()`（0 = 不限制） |
| `datasources[].tls` | TLS 开关、证书校验、CA/客户端证书与私钥 |
| `datasources[].extra` | 驱动自定义扩展参数 |
| `groups[]` | 主库、副本权重、写后读窗口、主库回退、只读标志（`read_only`）与故障转移（`failover.primaries` / `require_healthy` / `write_buffer`） |

详见 `config/datasources.json.example`。

## 开源协议

本项目以 **Apache License 2.0** 发布。许可证全文见仓库根目录的 [`LICENSE`](LICENSE) 文件。

- 使用、修改、分发本项目须遵守该许可证的条款。
- 在源码或文档中引用本项目时，请保留版权与许可证声明。
- 贡献代码即表示同意在 Apache-2.0 条款下授权你的贡献。
