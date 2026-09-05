# dbmw Detailed Guide

> 中文版：[guide.md](guide.md) · Quick start: [README_en.md](../README_en.md)

A database connection middleware written in C++17, supporting:

- **Multiple data sources**: one JSON config describes any number of data sources, dispatched by name.
- **Connection pool**: each data source has its own thread-safe pool, with borrow validation, failure rebuild, and min/max connection counts.
- **Pool toggle**: `pool.enabled` decides whether connections are reused; when off, every operation opens and closes a fresh physical connection — suited to low-frequency cron jobs, short-lived processes, or databases that strictly limit long-lived connections. The upper-layer API is unchanged.
- **Heartbeat keepalive**: a background thread periodically `ping`s idle connections, reclaims dead ones, and tops back up to the minimum count.
- **Production-grade connection lifecycle**: idle reclamation, maximum-lifetime rotation, borrow-leak warnings, and pool runtime metrics.
- **Transactions & sessions**: `transaction()` runs multiple statements on one exclusive connection; commits on success, and auto-rolls-back on failure or exception.
- **Transaction enhancements**: isolation level, read-only transactions, whole-transaction timeout cancellation, and savepoints.
- **Parameterized queries**: `?` placeholders + bound parameters, eliminating the injection risk of SQL string concatenation.
- **Prepared statements / generated keys / large-parameter streaming**: connection-level prepared-statement handle cache (hot SQL prepared only once, transparently), `execute` returns the auto-increment key (`GeneratedKeys`), and large-BLOB streaming writes (`StreamSource`). See the "Prepared statements / generated keys / large-parameter streaming" section below.
- **Resilience & routing**: safe retry for read-only queries, exponential backoff, circuit breaker (open/half-open), primary/replica read-write routing, and read-after-write window.
- **Primary failover**: the write path switches in order to candidate standbys when the primary is unavailable; when all fail, an optional write buffer provides soft degradation (background replay, returns `Buffered`) — but never for transactions.
- **Rate limiting & backpressure**: a token-bucket cap on per-source total QPS and (optionally) per-SQL-fingerprint QPS; over the limit fails fast with `RateLimited` — no retry, no flooding the pool.
- **SQL auditing & interception**: lightweight static analysis of SQL before execution; can block WHERE-less UPDATE/DELETE, LIMIT-less SELECT, writes on read-only sources, and block/allow by fingerprint blacklist/whitelist; a `warn` action provides a canary phase.
- **Query result cache**: caches non-transactional, non-session reads keyed by `(data source + raw SQL + type-tagged params)` — LRU + TTL + memory cap, invalidated per source on write; off by default.
- **Large data processing**: row-by-row callbacks and batch execution; MySQL/ODBC consume by row, PostgreSQL uses server-side cursors in chunks.
- **Observability**: tunable full SQL logging, slow-SQL aggregation/recent records, per-pool detail snapshots, and an operation-event callback.
- **Periodic stats to log**: a background thread samples pool and slow-SQL stats on the `observability.stats_report` cadence and appends them to a log file (human-readable `text` or one-object-per-line `json` for collectors), without touching business threads.
- **Result-set row guard**: each data source can set `max_result_rows`; when `query()` would materialize more rows than allowed it errors out and suggests `queryEach()` streaming instead — preventing one LIMIT-less query from exhausting process memory.
- **Secure configuration**: environment-variable passwords, TLS, driver-error redaction, and structured SQLSTATE.
- **Hot reload**: a new config is built in full, then atomically swapped in, draining the old pool within a grace period.
- **Multiple database types**: built-in **MySQL / PostgreSQL / ODBC (SQL Server · Oracle)** drivers, plus a **driver extension interface** — adding a database only requires implementing `IDriver` and registering it.

> Status: the core layer (config / pool / heartbeat / transaction / parameter binding / facade) is fully implemented
> and validated by 137 behavioral tests in `tests/dbmw_core_test.cpp` (mock driver, no real database needed).
>
> Driver implementation progress:
> - **MySQL — fully implemented** (libmysqlclient): connection timeout / charset, ping, column-type-aware result mapping,
>   explicit transactions, `mysql_real_escape_string` escaping; plus **`mysql_stmt_prepare` server-side prepared statements**
>   (connection-level handle cache), `mysql_insert_id` **generated keys**, and large-BLOB **streaming writes**.
> - **PostgreSQL — fully implemented** (libpqxx): connstring assembly / timeout / charset, ping,
>   OID-based result mapping, explicit transactions; **server-side parameter binding** via `pqxx::params` (wrapped by `execParams()`),
>   plus named-statement **prepared-statement cache** (LRU `DEALLOCATE`), `RETURNING` **generated keys**, and large-BLOB **streaming writes**.
> - **ODBC — fully implemented** (unixODBC): DSN / connection string, diagnostic records, type mapping, native parameter binding,
>   query timeout / cancellation, SQL Server / standard savepoint dialects; plus **`SQLPrepare`/`SQLExecute` prepared-statement cache**,
>   `OUTPUT INSERTED`/`RETURNING` **generated keys**, and large-BLOB **streaming writes**.
>
> Prepared statements and generated keys are implemented per driver; large-parameter streaming (`StreamSource`) currently uses a
> **buffered-degradation** path uniformly across all three drivers (read fully into a `Blob` then bound as a normal parameter), so
> calling code stays identical — true chunked MySQL `send_long_data` / ODBC `SQLPutData` is a future enhancement.
> All three drivers are controlled by the `DBMW_ENABLE_*` compile-time switches.

---

## Directory layout

```
include/dbmw/
  common/    types.h(value/row/resultset/status/error code)  observer.h(observability events)  logger.h(lightweight logging)
  config/    datasource_config.h  config_loader.h(JSON parsing)
  core/      idatabase_connection.h(connection abstraction + streaming/batch defaults)
             connection_pool.h     heartbeat_manager.h  database_manager.h
  driver/    idriver.h  driver_registry.h  driver_factory.h
             mysql_driver.h  postgres_driver.h  odbc_driver.h
  async/     async_types.h(results/Handle)  executor.h(IExecutor/thread pool)
             dbmw_async.h(async facade)  task.h(coroutine layer, optional C++20)
  dbmw.h     (public facade)
src/         corresponding implementations
tests/       dbmw_core_test.cpp  dbmw_async_test.cpp  dbmw_coro_test.cpp(coro=ON)
examples/    basic_usage.cpp  async_example.cpp
config/      datasources.json.example
third_party/nlohmann/json.hpp  (vendored single-header, works offline)
scripts/     setup-wsl.sh
```

## Building (WSL / Linux)

```bash
# 1) Install the toolchain (install only the drivers you enable)
sudo apt update
sudo apt install -y build-essential cmake
# Enable MySQL: sudo apt install -y default-libmysqlclient-dev
# Enable PG:     sudo apt install -y libpqxx-dev libpq-dev
# Enable ODBC:   sudo apt install -y unixodbc-dev

# 2) Configure + build
mkdir -p build && cd build
cmake ..                                   # core layer only
# Enable drivers example:
# cmake .. -DDBMW_ENABLE_MYSQL=ON -DDBMW_ENABLE_POSTGRES=ON -DDBMW_ENABLE_ODBC=ON
# Enable the coroutine layer (optional; only task.cpp is bumped to C++20):
# cmake .. -DDBMW_ENABLE_ASYNC_CORO=ON
cmake --build .

# 3) Run the example (loads config and queries; with drivers disabled you get a DriverDisabled message)
./examples/dbmw_example_basic ../config/datasources.json.example
```

Run the tests (optional, no real database needed — uses a mock driver to validate core semantics):

```bash
cmake .. -DDBMW_BUILD_TESTS=ON && cmake --build . && ctest --output-on-failure
# or run directly: ./tests/dbmw_core_test
```

You can also run `scripts/setup-wsl.sh` in one shot (installs dependencies and builds per arguments).

## Building (macOS)

On macOS, dependencies are managed with [Homebrew](https://brew.sh) and the compiler is the system **clang++** (install Xcode Command Line Tools first). Homebrew packages live under `/opt/homebrew` (Apple Silicon) or `/usr/local` (Intel); CMake's default search paths may not cover them, so pass `CMAKE_PREFIX_PATH` explicitly to locate the client libraries.

> **Note**: `DBMW_ENABLE_ODBC` is **OFF by default**. Install unixODBC and pass
> `-DDBMW_ENABLE_ODBC=ON` only when ODBC support is needed.

```bash
# 1) Command-line tools (provides clang++ / make)
xcode-select --install

# 2) Install dependencies (the three client libraries, matching apt on Linux)
brew install mysql-client libpqxx libpq unixodbc
#    - mysql-client is keg-only and does not symlink into /usr/local automatically;
#      it MUST be added to CMAKE_PREFIX_PATH explicitly.
#    - libpqxx depends on libpq; unixodbc provides the ODBC headers and libodbc.

# 3) Configure + build (use brew --prefix to locate headers/libs; separate paths with ';')
mkdir -p build && cd build
cmake .. \
  -DCMAKE_PREFIX_PATH="$(brew --prefix);$(brew --prefix mysql-client)" \
  -DDBMW_ENABLE_MYSQL=ON -DDBMW_ENABLE_POSTGRES=ON -DDBMW_ENABLE_ODBC=ON
cmake --build . -j"$(sysctl -n hw.ncpu)"

# 4) Run the example
./examples/dbmw_example_basic ../config/datasources.json.example
```

> When enabling only some drivers, drop the corresponding `-DDBMW_ENABLE_*` and remove any uninstalled package from `CMAKE_PREFIX_PATH` (an uninstalled `brew --prefix <pkg>` errors out); the core layer needs no client library and builds with a plain `cmake ..`.

Run the tests (optional, mock driver, no real database needed):

```bash
cmake .. -DDBMW_BUILD_TESTS=ON && cmake --build . -j"$(sysctl -n hw.ncpu)" && ctest --output-on-failure
```

## Quick start

```cpp
#include "dbmw/dbmw.h"

dbmw::DBMW::init("config/datasources.json");   // load multiple data sources + start heartbeat

dbmw::common::ResultSet rs;
auto st = dbmw::DBMW::query("SELECT 1", rs);    // default data source
if (st.ok()) { /* use rs */ }

int64_t n = 0;
dbmw::DBMW::execute("UPDATE t SET c = 1 WHERE id = 2", n); // default data source

dbmw::DBMW::shutdown();
```

Target a named source: `dbmw::DBMW::query("pg", "SELECT now()", rs);`

## Transactions & sessions

`query()` / `execute()` **borrow a fresh connection every time**, so a transaction spanning multiple statements must first pin the connection down:

```cpp
auto st = dbmw::DBMW::transaction([](dbmw::core::Session& s) {
    int64_t n = 0;
    if (auto r = s.execute("UPDATE accounts SET bal = bal - 100 WHERE id = 1", n); !r.ok())
        return r;                       // return failure -> auto rollback
    return s.execute("UPDATE accounts SET bal = bal + 100 WHERE id = 2", n);
});                                      // return success -> auto commit
```

- An exception thrown by the callback also triggers rollback (captured and converted to `TxError`, never escapes `transaction`).
- The callback may call `commit()` / `rollback()` itself; once the outer layer detects the transaction has ended it will not commit again.
- When you don't need a transaction but just want to run a few things on one connection (temp tables, session variables, etc.), use `withSession()`.

## Parameterized queries

Use `?` as a placeholder in SQL and pass parameter values via `common::Params` — **they are not concatenated into the SQL string**:

```cpp
dbmw::common::ResultSet rs;
dbmw::common::Params p{ std::string("O'Brien"), std::int64_t(42) };
auto st = dbmw::DBMW::query("SELECT * FROM t WHERE name = ? AND age > ?", p, rs);
```

- PostgreSQL / MySQL / ODBC all use **native parameter binding**.
- A custom driver that has not implemented native binding returns `NotSupported` by default, never silently degrading to SQL concatenation.
- Literal interpolation is enabled only for compatible drivers that explicitly override `allowsLiteralInterpolation()`; the scanner skips `?` inside strings, identifiers, and comments.
- A placeholder count that does not match the parameter count returns `QueryError`, never silently producing wrong SQL.

## Prepared statements / generated keys / large-parameter streaming

Three high-frequency capabilities that all official drivers ship — but which `IDatabaseConnection` had not yet wrapped — are now unified. None of them break the existing architectural invariants (the gate runs only once at the `DataSource` entry point, the result-cache key is unchanged, and failover / write buffer are never used in transactions).

> Note: the explicit `prepare` / `executePrepared` handle API exists only on `Session` (a handle binds to a concrete connection, which the stateless facade cannot hold across calls); generated keys and large-parameter streaming are available on both `DataSource` (via `DBMW::dataSource()`) and `Session`.

### Prepared statement reuse (connection-level handle cache)

When the driver supports it and `prepared_cache.enabled` is on, `DBMW::query(sql, params)` / `execute(sql, params)` look up an already-compiled handle in the connection's cache keyed by `(normalized SQL + parameter type signature)`; if absent they `prepare` and store it, then run via `executePrepared`. **Fully transparent to the caller, with an unchanged signature** — a hot SQL is prepared only once automatically.

```cpp
// Transparent auto-cache: identical to before, no changes needed
dbmw::common::ResultSet rs;
dbmw::common::Params p{ std::int64_t(1) };
auto st = dbmw::DBMW::query("SELECT * FROM t WHERE id = ?", p, rs);
```

For fine-grained control on a stable connection, or to reuse one handle across many rows, use the explicit handle API on `Session`:

```cpp
auto st = dbmw::DBMW::transaction([](dbmw::core::Session& s) {
    dbmw::core::PreparedStatementHandle h;
    // typesSample is only used to infer the parameter type signature (placeholder values suffice, no real data needed)
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

- A handle's lifetime is bound to "the current physical connection" and valid only while the connection / `Session` is alive; once the connection is returned / closed the handle is invalid, and the driver calls `closeAllPrepared()` from `close()` to release the native handle (MySQL `mysql_stmt_close` / PG `DEALLOCATE` / ODBC `SQLFreeHandle`).
- The cache lives on the driver connection object; it survives being returned to the pool and is reused when the same connection is borrowed again. Pools are per data source, so prepared statements never leak across sources.
- `max_per_connection > 0` evicts the least-recently-used handle via LRU; `0` = unlimited (reclaimed naturally when the connection closes).
- A prepared execution still passes the `DataSource` entry-point `preGate` (once); the result-cache key logic is unchanged; auditing still classifies by SQL text.

### Generated keys / auto-increment ID (GeneratedKeys)

`execute` gains an overload that returns the generated columns of the inserted row:

```cpp
auto ds = dbmw::DBMW::dataSource();          // default data source (pass a name for a specific one)
int64_t n = 0;
dbmw::common::GeneratedKeys keys;

// MySQL: works out of the box, no SQL change needed
ds->execute("INSERT INTO t(name) VALUES('x')", n, keys);
int64_t id = keys.lastInsertId();            // MySQL auto-increment

// PostgreSQL / ODBC: returned directly via the SQL's own RETURNING / OUTPUT — dbmw does not append it
ds->execute("INSERT INTO t(name) VALUES('x') RETURNING id", n, keys);
if (!keys.empty()) id = keys.rows[0].asInt64(0);  // take the first column of the RETURNING row
```

Unified model: `GeneratedKeys` is always "a result set of the generated columns" — MySQL synthesizes one row / one column via `mysql_insert_id`, while PG/ODBC emit directly via `RETURNING`/`OUTPUT`. **dbmw never appends `RETURNING` to your SQL automatically** (that would change semantics and couple to a dialect), so for PG/ODBC to get the auto-increment id, write `RETURNING id` in your SQL yourself. When there is no `RETURNING` and it is not a MySQL auto-increment, `keys.empty()` is true (not an error). Call `keys.clear()` before reusing the same `GeneratedKeys` object, so a retried statement doesn't mistake stale old rows for this run's generated keys.

### Large-parameter streaming (StreamSource)

A huge BLOB/CLOB need not be materialized into memory all at once: wrap a synchronous read callback or a `std::istream` in a `StreamSource`, and the driver pulls bytes in chunks during execution. This is **input-direction** streaming — the opposite of result-set streaming (`queryEach` / cursor); don't confuse the two.

```cpp
auto ds = dbmw::DBMW::dataSource();
std::ifstream f("big.bin", std::ios::binary);
dbmw::common::StreamParams sp{ std::int64_t(1), dbmw::common::StreamSource(f) };
int64_t n = 0;
ds->execute("INSERT INTO t(id, blob) VALUES(?, ?)", sp, n);   // or query / executeBatch
```

- `StreamSource(read, totalSize, isBinary)`: a custom `read(buf, n)` callback returns the bytes read this chunk (0 = EOF); there is also a convenience `StreamSource(std::istream&)` constructor. When built from an istream, the stream must stay alive for the duration of the execution.
- `isBinary=true` means binary (bytea / blob), `false` means text (clob).
- **All three drivers currently use a buffered-degradation path**: `StreamSource` is read fully into a `Blob` and then bound as a normal parameter (libpq does not support parameter data-at-execution, so PG is this way by nature; true chunked MySQL `send_long_data` / ODBC `SQLPutData` is a future enhancement). Calling code stays identical regardless of driver.
- A query containing a `StreamSource` is **not cached** (streamed content is not a fixed value and cannot participate in `cacheKey`); auditing still classifies by SQL text and stream content never enters the log.

### Configuration

```json
{
  "prepared_cache": {
    "enabled": true,
    "max_per_connection": 0
  }
}
```

- `enabled` defaults to `true` (a pure, transparent, side-effect-free performance optimization); turning it off degrades to re-binding every time.
- `max_per_connection`: `0` = unlimited; `>0` triggers LRU eviction.
- Generated keys / large-parameter streaming are API-driven and need no config switch.

## Timeouts, cancellation & transaction options

A data source's `query_timeout_ms` maps to PostgreSQL `statement_timeout`, ODBC `SQL_ATTR_QUERY_TIMEOUT`, and the MySQL client read/write timeout. A transaction can additionally set isolation level, read-only, and an overall deadline:

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

When the deadline is reached, the middleware asks the driver (from the watchdog thread) to cancel the current statement and rolls the transaction back.

> **`options.timeout` is a best-effort upper bound, not a hard interrupt.**
> When the driver implements `cancel()` (all three built-in drivers do), the statement is truly interrupted; when it does not, C++ cannot safely force-kill a running user callback, so the result is rewritten to `QueryTimeout` only after the callback finishes naturally. In the latter case the returned `message` carries a `could not cancel` hint, so you can tell "cancelled in time" from "never actually interrupted, just judged timed-out afterward".

The cancellation path itself is exception-safe: the watchdog thread swallows any exception thrown by the driver's `cancel()`. An exception escaping from a thread would `std::terminate` the whole process — a "robustness mechanism that becomes a crash point" problem that must be blocked at the framework level.

## Streaming reads & batch execution

```cpp
std::uint64_t rows = 0;
dbmw::DBMW::queryEach("SELECT * FROM large_table", {},
    [](const dbmw::common::Row& row) {
        // returning false stops early
        return consume(row);
    }, rows);

dbmw::common::ParamBatch batch{
    {std::int64_t(1), std::string("a")},
    {std::int64_t(2), std::string("b")}
};
dbmw::common::BatchResult result;
dbmw::DBMW::executeBatch("INSERT INTO t(id, name) VALUES(?, ?)", batch, result);
```

**Batch execution is atomic**, with consistent behavior across all three drivers: when the caller has not opened a transaction, the middleware wraps it in one automatically; if any row in the middle fails, the whole batch rolls back and `BatchResult` carries no partial affected-row counts (avoiding a caller mistakenly assuming the earlier rows committed). When the caller is already in a transaction, the outer transaction is reused and the rollback scope is up to the caller.

> Implementation note: a driver that overrides `executeBatch` for higher throughput (array binding / COPY)
> must also override `inTransaction()` to return the real transaction state and preserve the same atomicity guarantee.

## Cursors

`query()` borrows a connection, materializes the whole result, and returns it; `queryEach()` streams but still consumes each row in one shot inside the callback. A **cursor** changes the connection lifecycle from "borrow → use → return" into "borrow → pin → fetch N times → close → return": one physical connection (and its transaction on PostgreSQL) is held by the cursor until `close()` or destruction. Suited to "large result set, controlled batch-by-batch consumption, without materializing it all into memory at once."

```cpp
dbmw::core::CursorOptions opts;
opts.batch_size = 1000;          // rows prefetched per fetch (or fall back to config default_batch_size)
opts.auto_transaction = true;    // when no transaction is open, the cursor opens its own to back the statement

std::unique_ptr<dbmw::core::Cursor> cur;
auto st = dbmw::DBMW::openCursor("SELECT * FROM large_table WHERE k > ?",
                                 dbmw::common::Params{std::int64_t(0)}, opts, cur);
if (!st.ok()) { /* handle error */ }

dbmw::common::ResultSet batch;
while (cur->fetch(0, batch).ok() && cur->hasNext()) {  // fetch(0) = take batch_size rows
    consume(batch);
    batch.clear();
}
cur->close();   // explicitly return the connection; if skipped, it is closed + returned on destruction
```

The facade `DBMW::openCursor` has two overloads: default data source, or a named one. Within a transaction/session you can also use `Session::openCursor(...)` (the connection is not additionally occupied and returns with the session). `fetch(n, out)` **appends** at most n rows to `out` (it does not clear, so multiple fetches accumulate the same result set); `n == 0` lets the driver decide by batch_size. `fetchRow` fetches a single row, `close` closes explicitly (idempotent), and `isOpen` / `hasNext` / `rowsFetched` expose state.

### Two binding modes

- **`OwnsHandle` (standalone cursor, default)**: borrows a connection from the pool and pins it until the cursor is closed/destroyed. During that time the connection does not participate in other pool borrows — suited to long-lived, multi-fetch consumption.
- **`BorrowedInSession` (in-session cursor)**: opened inside `transaction` / `withSession`, reusing the session's existing connection (and its transaction snapshot if one is open), without additionally occupying a pool connection; `close()` only closes the server-side cursor and does not return the connection — the connection still belongs to `Session` and returns with its destruction.

### Per-driver behavior

- **PostgreSQL**: server-side cursor `DECLARE CURSOR` + `FETCH FORWARD n` + `CLOSE`. The cursor must live inside a transaction — if already in one it borrows that transaction; otherwise, with `auto_transaction=true`, it opens its own `pqxx::work` as a fallback (committed on close); with `auto_transaction=false` and no transaction it reports `CursorError` directly (no silent degradation). `scrollable=true` only takes effect when the config allows it, otherwise returns `NotSupported`.
- **MySQL**: unbuffered result set (`mysql_stmt_*` and **not** calling `mysql_stmt_store_result`), consuming by batch `mysql_stmt_fetch` in a stream — results do not land in client memory; no transaction requirement.
- **ODBC**: real cursor (`SQL_ATTR_CURSOR_TYPE` + `SQLFetch`); with config `allow_scrollable` it sets `SQL_CURSOR_STATIC` for scrolling; other drivers do not support scrolling (`scrollable=true` returns `NotSupported`).

### Configuration & resource guard

Per data source, in JSON:

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

- `enabled=false`: opening a cursor on that source returns `NotSupported` directly.
- `default_batch_size`: fallback when the caller uses the driver's default batch_size.
- `max_open_cursors`: per-source concurrent-cursor cap; **when > 0 a resource guard engages** (atomic CAS quota, over the limit returns `CursorLimit`); `0` = unlimited.
- `allow_scrollable`: whether scrollable cursors are allowed (only ODBC honors it).

A cursor passes through `preGate` (audit + rate limit) but **does not enter the query cache** (streamed results are not directly cacheable and may span transaction snapshots). A cursor passes the audit as `OperationType::Select`, and is thereby **exempted from `require_limit_select`** — cursors consume in bounded batches by nature, so forcing a LIMIT would destroy the legitimate "full cursor scan" use case; `enforce_read_only` / `block_no_where_dml` / blacklist-whitelist still apply to cursors (the audit subject is still classified by SQL text).

## Retry, circuit breaker & read-write routing

- Only query errors with `Status::retryable == true` are retried. Backoff carries true random jitter, used to spread out concurrent retries and avoid a thundering herd at the moment of failure recovery.
- Writes and batch writes are not retried by default; only when `retry_writes: true` is set explicitly.
- A streaming query, once it has delivered data to the callback, is never replayed, avoiding duplicate consumption.
- A group's queries round-robin across replicas by weight; writes, sessions, and transactions always go to the primary.
- Within `read_after_write_ms` after a write has occurred, read requests are pinned to the primary.
  A write inside a session (`withSession`) triggers this too — as long as the callback has successfully executed
  `execute` / `executeBatch`, reads within the window will not hit a replica.
- When a replica hits a connection-class error or is open-circuited, it can fall back to the primary automatically.
- **The circuit breaker covers all entry points**: queries, writes, streaming, batch, sessions, and transactions all pass the same gate.
  Sessions and transactions only fail fast and do not auto-retry (the callback content may not be idempotent).

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

Call `DBMW::reload(path, grace)` to atomically load a new config and wait for in-flight operations on the old pool to return.

## Rate limiting, auditing, caching & primary failover

These four capabilities are all **off by default, toggled as a whole**, and pass the gate only once at the `DataSource` entry point — group forwarding to leaves goes through the `*Ungated` internal path; deducting tokens twice would halve the configured QPS cap out of nowhere, and auditing twice would emit doubled alarms.

### Primary failover

Group config `failover.primaries` gives an ordered list of writable candidates (primary auto-pinned to top). The write path picks an **un-open-circuited** candidate in order; when none is available:

- If `failover.write_buffer` is configured, the write enters a bounded in-memory queue and is replayed by a background flush thread after the primary recovers, immediately returning `Buffered` (**soft degradation**: enqueued means returned, not committed; a process crash loses the data);
- Otherwise it returns `CircuitOpen` (marked retryable, to be handled by the upper layer's retry/circuit-breaker).

Constraint: **neither failover nor the write buffer is used for transactions** — a transaction callback may not be idempotent, and replay could cause duplicate writes, so when the primary is unavailable a transaction simply fails and the caller decides on replay. The write path switches nodes only on connection-class / open-circuit failures; a business failure (e.g. unique-key conflict) returns directly without switching.

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

### Rate limiting & backpressure (rate_limit)

Token-bucket rate limiting, failing fast rather than flooding the pool and then snowballing. Over the limit returns `RateLimited` (`retryable=false` — the caller should queue locally or degrade, not retry, since retrying amplifies traffic).

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

- `global_qps`: per-source total QPS cap (token-bucket refill rate); `per_fingerprint_qps`: single-SQL-fingerprint QPS (protects hot statements).
- `burst`: burst capacity, 0 = equal to the corresponding qps.
- `fingerprint_mode`: `off` (no fingerprint) / `template` (structured template) / `full` (template + params). Fingerprints are computed only when fingerprint limiting is enabled; a pure total-volume scenario pays no such cost.

### SQL auditing & interception (sql_audit)

Lightweight static analysis of SQL before execution (heuristic classification, not a full parser — may misjudge dynamic SQL / stored procedures). On a policy hit, `action` decides:

- `block`: intercept, return `SqlBlocked`;
- `warn`: only log a warning and let it through (**the default in the canary phase**, used to gauge "how much traffic this policy would block"; use `SqlAuditor::stats()` counts to decide when to switch to `block`).

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

- `block_no_where_dml`: intercept UPDATE/DELETE without WHERE (prevents full-table accidental modify/delete).
- `require_limit_select`: intercept SELECT without LIMIT (prevents pulling a whole table at once). **Cursors (`openCursor`) are exempt from this rule**,
  see the "Cursors" section above.
- `enforce_read_only`: together with the group's `read_only`, intercepts any write on a read-only source.
- `blacklist_fingerprints`: hit = intercept; `whitelist_fingerprints`: when non-empty, "allow-list only" — everything else is intercepted (allow-list mode).

Auditing runs at the `DataSource` entry point for single `query`/`execute` calls; `withSession`/`transaction` statements are assembled temporarily by the user callback and are not visible at the entry point, so they sink down to the `Session` for per-statement checks, and only run on a `Session` that explicitly enabled auditing (never double-audited).

### Query result cache (query_cache)

Caches only non-transactional, non-session reads on the `DataSource::query` path. Key = **raw SQL + a type-tagged, length-prefixed parameter sequence** (not a structural template — a template folds literals into `?` so different values collide on one key; not `valueToString` — it loses type info so `1` and `"1"` collide on a key).

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

- LRU eviction + TTL expiry + memory cap (a single result set larger than `max_memory_bytes` is simply not cached, otherwise caching it would flush the whole cache to make room).
- `cache_on_replica_only=true` caches only reads that hit a replica; the primary's strongly-consistent reads are not cached.
- Invalidated per source name on write (`markWrite`), avoiding a read-after-write hitting a stale result.
- Hit/eviction/invalidation counts are exposed via `QueryCache::stats()`; these counts keep accumulating even when the cache is off and are not cleared on hot reload.

## Caching internals

The middleware has **two real KV caches**, plus a statistics LRU (slow-SQL aggregation) and two TTL lifecycle reclamations (connection pool, write buffer). The two KV caches each have an independent top-level config block in `datasources.json` (`query_cache` and `prepared_cache`) and are **independent — do not conflate them**:

- `query_cache` caches **result data**; its key is `(SQL + parameter values)`.
- `prepared_cache` caches **statement handles**; its key is `(SQL + parameter type signature)`.

### 1. Query result cache (QueryCache, global singleton)

Applies only to the leaf read path of `DataSource::query` (non-transactional, non-session reads). A group-forwarded leaf uses its own data-source name as a key prefix, so the same SQL hitting the primary vs. a replica becomes two independent entries — that is what lets write-invalidation clear exactly the right node.

Implementation (`src/core/query_cache.cpp`): a global singleton with `std::unordered_map<std::string, Entry> store_` + `std::list<std::string> lru_` (most-recently-used at the head), guarded by a single `std::mutex mtx_`. The `enabled_` / `replicaOnly_` flags are mirrored in `std::atomic`s — **the hot path reads the atomic flags lock-free, and never touches mtx_ when the cache is off**. Hit/eviction/invalidation counters are atomics exposed via `QueryCache::stats()`; clearing the cache on hot reload does not reset these (they are process-cumulative).

**KV contents:**
- **key** = `data-source name + '\0' + cacheKey(sql, params)`, where `cacheKey` = raw SQL + `\x1e` + param count + per-param (`\x1f` + type tag + length-prefixed value). Each type is tagged (`n`/`b`/`i`/`d`/`t`/`s`/`x`): doubles are serialized bitwise (decimal text loses precision), strings/blobs carry a length prefix, **so distinct parameters always produce distinct keys**. The key uses parameter **values**, not a structural template — a template folds literals into `?` and makes different values collide.
- **value** = `Entry { ResultSet rs; expire; list::iterator lru; bytes; }`: a **deep copy** of the result set + its TTL timestamp + an approximate byte size.

**Expiry policy (three-fold):**
1. **TTL**: `expire = now + ttl_ms`; on `get()`, an expired entry is `eraseLocked` and counts as a miss; `ttl_ms<=0` disables the whole cache.
2. **LRU capacity eviction**: `evictLocked` enforces both `max_entries` (count) and `max_memory_bytes` (approx bytes via `approxBytes`, which estimates column names + per-column values), evicting from the `lru_` tail; a single entry larger than the memory cap is simply not cached.
3. **Write-invalidation**: `markWrite()` → `QueryCache::invalidate(data-source name)`, which clears every entry for that source by the `name\0` prefix. `configure()` clears all old data on config change, avoiding mixing old and new TTL/caps.

**Why**: saves repeated DB round-trips for read-heavy, repeat-heavy queries (lookup/config tables); the cost is eventual consistency, so it is **off by default**; `cache_on_replica_only=true` caches only replica reads, never the primary's strongly-consistent reads.

### 2. Prepared-statement cache (PreparedCache, per-connection handle cache)

All three drivers (`MySQLConnection` / `PostgresConnection` / `OdbcConnection`) maintain their own per-connection handle cache inside `prepare()`. The facade's `Session::runPreparedQuery` / `runPreparedExec`, when `preparedPathUsable()`, calls `conn->prepare`, which looks up the "current connection" cache internally — that is the **transparent auto-cache** behind `DataSource::query/execute(params)` (usage in the section "Prepared statements" above). The generated-keys path does NOT use this (see caveats below).

Implementation: each connection object holds `std::unordered_map<std::string, PreparedStatementHandle> preparedCache_` + `std::list<std::string> preparedLru_` + a monotonic sequence (handle id) + `preparedLimit_` (per-connection cap). After a connection is returned to the pool the map persists and is reused next time the same connection is borrowed; on `close()` → `closeAllPrepared()` all native handles are released.

**KV contents:**
- **key** = `sql + common::paramTypeSignature(typesSample)`, using the **parameter type signature**, not values — the same SQL with different parameter types must be treated as a different statement at prepare time.
- **value** = `PreparedStatementHandle { uint64_t id; void* native; }`: `native` is `MYSQL_STMT*` for MySQL, `nullptr` for PG (name stored separately in `preparedNames_`), `SQLHSTMT` for ODBC. What is stored is the **native server-side prepared statement handle**, not a result.

**Expiry policy:**
- **LRU capacity eviction**: when `max_per_connection > 0`, after insertion `while(size > limit)` evicts from the `lru_` head (least recently used) and releases the native handle via `mysql_stmt_close` / `conn_->unprepare` / `SQLFreeHandle`; `0` = unlimited (reclaimed on connection close). On a hit the key is moved to the `lru_` tail.
- On connection close: `closeAllPrepared()` releases every handle of that connection.

**Why**: prepare-once / execute-many for hot statements, saving the server-side hard-parse + type-inference round-trip; it **changes no query result** and is a pure performance optimization, so it is **on by default**.

> Note: a handle evicted by LRU while still held by the upper layer becomes dangling — an inherent LRU trade-off; it does not trigger when unlimited (the default), and setting an explicit cap means accepting it.

### 3. Slow-SQL aggregation cache (Observer LRU, non-business data)

In `observer.cpp`, slow-SQL aggregation: when a statement is judged slow it is aggregated by `sqlFingerprint` into `g_slowStats` (a low-frequency path with its own lock, not blocking the high-frequency snapshot reads). KV: key = fingerprint (structural-template hash), value = aggregate stats (count/duration/histogram). Evicted by the `aggregate_capacity` cap (O(1) LRU); when `histogramBucketsMs` buckets change (old samples cannot be re-bucketed losslessly) the whole set is cleared. Fully independent of the two caches above; purely observational.

### Caching and consistency boundaries (important)

- **`StreamParams` (large-parameter streaming) and cursors deliberately bypass the result cache**: streamed content is not a fixed value and cannot participate in `cacheKey` (which requires identical params → identical result); auditing still classifies by SQL text and streamed content never enters logs.
- **The generated-keys path bypasses the prepared cache**: `executePrepared` cannot capture the `RETURNING` result set; saving one prepare at the cost of silently hiding the primary key from the caller would be putting the cart before the horse.
- **Write buffer / connection-pool TTL** are object-lifecycle expiries, not KV caches.

## Async API (v0.2.0: callbacks / futures / coroutines)

The three calling styles share one execution pipeline — governance gates (audit / rate limit / circuit breaker / cache), retry backoff, statement timeout, cancellation — and differ only in how results are delivered. Enable `async` in the config:

```json
{ "async": { "enabled": true, "threads": 4, "queue_size": 4096 } }
```

`threads` is the worker count (0 = hardware_concurrency); one extra timer thread drives retry backoff and timeout checks. When the queue is full, new operations fail fast with `Overloaded` (explicit backpressure, not implicit queueing). `DBMW::shutdown` rejects new operations, waits for in-flight ones to finish, then stops the executors and the pools.

**Callback style (hot path)** — completion callbacks are delivered by the completion executor, never on the caller's stack; the returned `Handle` supports cancellation:

```cpp
dbmw::async::Options opts;
opts.timeout = std::chrono::milliseconds(2000);   // overall statement deadline
auto h = dbmw::async::query("SELECT id FROM users WHERE age > ?",
                            {dbmw::common::Value(std::int64_t(18))},
    [](dbmw::async::QueryResult &&r) {            // runs on the completion executor; keep it short
        if (r.status.ok()) useRows(std::move(r.rows));
    }, opts);
// To give up mid-flight: h.cancel() — Queued never touches the pool;
// Running forwards a best-effort driver cancel
```

**Future style (convenience)** — no cancellation (use the callback style with a `Handle` if you need it); dropping a future without consuming it is legal:

```cpp
auto fut = dbmw::async::execute("UPDATE users SET active = 1 WHERE id = ?",
                                {dbmw::common::Value(std::int64_t(7))});
auto r = fut.get();   // r.status / r.affected
```

**Coroutine style (optional, C++20)** — a lazy `Task` that starts only on `co_await`; destroying an un-awaited task is a safe no-op. Governance / retry / cancellation / timeout are identical to the callback style; coroutines always resume on the completion executor thread:

```bash
cmake .. -DDBMW_ENABLE_ASYNC_CORO=ON   # only task.cpp is bumped to C++20; the rest stays C++17
```

```cpp
#include "dbmw/async/task.h"   // the including TU must be compiled as C++20

dbmw::async::Task<void> demo() {
    // Hoist parameters to a named local: braced temporaries inside co_await
    // arguments trigger a GCC 13 ICE (see notes below).
    dbmw::common::Params params;
    params.push_back(dbmw::common::Value(std::int64_t(18)));

    auto q = co_await dbmw::async::queryAsync("SELECT id FROM users WHERE age > ?", params);
    if (q.status.ok()) useRows(std::move(q.rows));

    auto tx = co_await dbmw::async::transactionAsync({}, [](dbmw::core::Session &s) {
        std::int64_t n = 0;
        return s.execute("UPDATE users SET active = 1", n);  // non-OK rolls back
    });
}

dbmw::async::run(demo());   // controlled fire-and-forget: the frame destroys itself on completion
```

**Custom executor (asio integration)**: inject an `IExecutor` adapter via `dbmw::async::setExecutor(...)`; completion callbacks and coroutine resumes then happen on your own event-loop threads (adapter sketch in segment 5 of `examples/async_example.cpp`).

Constraints and caveats:

- Callbacks and transaction lambdas run on workers: keep them short and thread-safe. **Never call `dbmw::async::*` inside a transaction lambda** — nested async can deadlock on a small pool; use the synchronous `Session` methods directly.
- An uncaught exception inside a top-level coroutine started by `run()` terminates the process (never silently swallowed); handle exceptions inside the coroutine or propagate them via `co_await` to an enclosing `try/catch`.
- Do not write coroutine bodies as lambdas capturing locals — the closure temporary dies before the async operation completes and the captures dangle; use named functions returning `Task`.
- Known GCC 13 defect: non-trivial braced temporaries directly inside `co_await` arguments (e.g. `{Value(1)}`) trigger an internal compiler error (PR109227 family); hoist parameters into a named local first. GCC 14+ / Clang / MSVC are unaffected.
- Full design (drain order, timeout semantics, consistency test matrix): `docs/async-design-v0.2.0.md`.

## Observability

Full SQL, slow SQL, and pool metrics are configured via `observability`; full parameter values are off by default:

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

`stats_report` is sampled on the `interval_ms` cadence by a background thread and appended to `file` (parent directory auto-created; if `file` is empty it only goes to the logger). `format` supports `text` (multi-line, human-readable) and `json` (one object per line). `interval_ms` below 1000 is silently raised to 1000 to avoid high-frequency file writes slowing the business in turn. The persisted content matches the口径 of `allPoolStats()` / `slowSqlStats()`. A stats failure always only swallows the exception and never affects the business.

`sql_log.mode="full"` renders parameters in the actual driver dialect, but is still for diagnostics only — database execution continues to use native parameter binding. Strings and BLOBs may contain passwords, tokens, or personal data, and only enter the log after the corresponding `include_*_values` is explicitly turned on; both SQL and individual parameters have length caps.

```cpp
dbmw::DBMW::setObserver([](const dbmw::common::OperationEvent& event) {
    // event: data source, operation type, duration, structured status, row count, and SQL fingerprint.
    // When neither SQL logging nor slow SQL is enabled, by default it still carries no SQL or parameters.
});

auto topSlow = dbmw::DBMW::slowSqlStats(20, "main");       // sorted by average duration
auto recent = dbmw::DBMW::recentSlowSql(50, "main");      // sorted by most recent
dbmw::DBMW::clearSlowSqlStats();

dbmw::core::ConnectionPool::Stats stats;
if (dbmw::DBMW::poolStats(stats, "app")) {
    // min/max, utilization(), idle/borrowed/waiting, high-water marks, borrow-wait duration, eviction counts, etc.
}

auto physicalPools = dbmw::DBMW::allPoolStats();
```

Slow SQL uses parameterized-template fingerprint aggregation, with fixed capacity and a duration histogram to bound memory. Observer exceptions are isolated and do not change the database operation result. A group's `poolStats` aggregates its members, while `allPoolStats` returns every physical pool — handy for locating a specific primary or replica.

## Error codes

`common::Status` carries an `ErrorCode`, convertible to a string via `common::errorCodeToString()`.

| Error code | Meaning |
| --- | --- |
| `Ok` | Success |
| `ConfigError` | Config parse/validation failure |
| `ConnectionFailed` | Connection establishment failure |
| `QueryError` | Query failure / parameter count mismatch |
| `QueryTimeout` | Query or transaction exceeded its deadline |
| `Cancelled` | Operation cancelled |
| `ConstraintViolation` | Unique-key, foreign-key, not-null, etc. constraint conflict |
| `Deadlock` | Deadlock or serialization failure; combine with `retryable` to decide retry |
| `PingFailed` | Heartbeat failed |
| `TxError` | Transaction failure (including callback throwing) |
| `PoolExhausted` | Connection pool exhausted (including borrow-wait timeout) |
| `PoolClosed` | Connection pool already closed (borrowing after `shutdown()`) |
| `CircuitOpen` | Data source is open-circuited, database not reached |
| `RateLimited` | Rate-limited (token bucket empty); not retryable — queue locally or degrade |
| `SqlBlocked` | SQL intercepted by audit policy (e.g. read-only, WHERE-less DML) |
| `Buffered` | Write entered the write buffer, not yet committed (soft degradation; lost on crash; not retried) |
| `NotConnected` | Connection not established / already broken |
| `DriverDisabled` | Driver not enabled at compile time |
| `UnknownDriver` | Unknown data source type |
| `NotSupported` | Driver has not implemented this capability |
| `CursorClosed` | Cursor already closed or called fetch/close after move-from |
| `CursorLimit` | Exceeded per-source concurrent-cursor cap (max_open_cursors != 0) |
| `CursorError` | Cursor operation failed (DECLARE/FETCH/CLOSE or driver row fetch error) |

## Installation & downstream integration

dbmw can be installed as a CMake package; downstream uses `find_package(dbmw)` directly (nlohmann/json ships with the package, no separate `find_package` needed):

```bash
mkdir -p build && cd build
cmake .. -DDBMW_ENABLE_POSTGRES=ON   # enable drivers as needed
cmake --build .
cmake --install . --prefix /usr/local
```

### CMake downstream project

```cmake
cmake_minimum_required(VERSION 3.16)
project(my_app LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

find_package(dbmw REQUIRED)

add_executable(my_app main.cpp)
target_link_libraries(my_app PRIVATE dbmw::dbmw)
```

`dbmw::dbmw`'s PUBLIC dependency (`dbmw::nlohmann_json`) is pulled in automatically with the package. Be sure to call
`DBMW::shutdown()` before exit, to reclaim the pool and heartbeat threads.

### Non-CMake projects (pkg-config)

A `dbmw.pc` is generated on install:

```bash
g++ main.cpp $(pkg-config --cflags --libs dbmw) -o my_app
```

### Driver client libraries (must read)

When a driver is enabled, the installed package contains **only** `libdbmw.a` and the headers — **not** the corresponding database client library (libpqxx / libmysqlclient / unixODBC). Because dbmw is a static library, these client libraries must be provided by the downstream project, otherwise linking fails with undefined symbols:

- Enable MySQL → downstream `apt install default-libmysqlclient-dev` and link `-lmysqlclient`
- Enable PG     → downstream install `libpqxx-dev libpq-dev`, link `-lpqxx -lpq`
- Enable ODBC   → downstream install `unixodbc-dev`, link `-lodbc`

Neither `find_package(dbmw)` nor `dbmw.pc` auto-appends these links (a static library + pure path dependencies cannot propagate across packages).

> **Expected behavior (out-of-the-box notes)**
> - `DBMW_ENABLE_*` are all OFF by default; a driver not enabled at compile time returns `DriverDisabled` on call.
> - Call `DBMW::shutdown()` before process exit to reclaim the pool and heartbeat threads.

## Extending a new database type

1. Add `xxx_driver.h/.cpp` under `include/dbmw/driver/`, implementing `IDatabaseConnection` and `IDriver` in the style of `MySQLConnection`.
2. In the `.cpp`, call `DriverRegistry::instance().registerDriver("xxx", ...)`.
3. (Optional) Register it in `registerBuiltinDrivers()` of `driver_factory.cpp`, or register on your own at startup.
4. Set `type` to `"xxx"` in the JSON config and it will be recognized.

The base connection methods stay minimal; a production driver should additionally override the parameterized `query`/`execute` and make `supportsParams()` return `true`. When native binding is not implemented it returns `NotSupported` explicitly. `queryEach`, `executeBatch`, transaction options, savepoints, and cancellation all have optional extension points; the streaming and batch methods have compatible default implementations, but a large-data driver should override them as cursor / row-by-row fetch and array binding.

Two easy-to-trip contracts:

- **A driver with transaction state must override `inTransaction()`.** The base default returns `false`, which would make `executeBatch`'s default implementation wrap another `begin` inside the caller's already-open transaction — on MySQL that equals an implicit `COMMIT` of the caller's first half.
- **`cancel()` must never let an exception escape.** It is called cross-thread by the transaction-timeout watchdog; an uncaught exception would `std::terminate` the whole process. Wrap it in `try/catch` inside the driver.

## Configuration reference (datasources.json)

| Field | Meaning |
| --- | --- |
| `default_datasource` | Default data source name |
| `heartbeat_interval_ms` | Heartbeat interval |
| `pool.enabled` | Whether to enable the connection pool (default `true`); when off, no reuse, no warm-up, not bound by min/max |
| `pool.min` / `pool.max` | Per-source pool min/max connections |
| `pool.borrow_timeout_ms` | Max wait when borrowing a connection; timeout returns `PoolExhausted` (0 = no wait) |
| `pool.idle_timeout_ms` | Reclaim idle connections beyond this to `min` |
| `pool.max_lifetime_ms` | Physical connection max lifetime, rotated on expiry |
| `pool.leak_detection_threshold_ms` | Warn on a borrow exceeding this duration (0 = off) |
| `retry.*` | Max attempts, exponential-backoff bounds, whether to retry writes |
| `circuit_breaker.*` | Consecutive-failure threshold and open duration |
| `rate_limit.*` | Rate limiting: per-source total QPS / per-SQL-fingerprint QPS / burst capacity / fingerprint mode (off by default) |
| `sql_audit.*` | SQL audit: action (block/warn, default warn), WHERE-less DML, LIMIT-less SELECT, read-only interception, fingerprint black/whitelist (off by default) |
| `query_cache.*` | Query result cache: TTL, entry cap, memory cap, replica-only caching (off by default) |
| `prepared_cache.*` | Prepared-statement cache: max handles per connection (0 = unlimited, LRU eviction), enabled flag (default true) |
| `datasources[].name` | Data source name (unique) |
| `datasources[].type` | `mysql` / `postgres` / `odbc` / custom |
| `datasources[].host/port/user/password/database` | Connection parameters |
| `datasources[].dsn` | ODBC data source name |
| `datasources[].password_env` | Read password from an environment variable, takes precedence over plaintext `password` |
| `datasources[].query_timeout_ms` | Per-statement execution deadline |
| `datasources[].max_result_rows` | Max rows `query()` may materialize at once; over the limit errors and suggests `queryEach()` (0 = unlimited) |
| `datasources[].tls` | TLS toggle, cert verification, CA / client cert & key |
| `datasources[].extra` | Driver-specific extension parameters |
| `groups[]` | Primary, replica weights, read-after-write window, primary fallback, read-only flag (`read_only`) and failover (`failover.primaries` / `require_healthy` / `write_buffer`) |

See `config/datasources.json.example` for details.

## License

This project is released under the **Apache License 2.0**. The full license text is in the
[`LICENSE`](../LICENSE) file at the repository root.

- Using, modifying, or distributing this project must comply with the terms of that license.
- When referencing this project in source or docs, please keep the copyright and license notices.
- By contributing code, you agree to license your contribution under the Apache-2.0 terms.
