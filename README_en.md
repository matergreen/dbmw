# dbmw — C++ Database Connection Middleware

> [中文](README.md) · [Detailed guide](docs/guide_en.md)

dbmw gives C++ applications a unified database access layer. The same API works with MySQL,
PostgreSQL, and ODBC databases while the middleware centrally manages connection pooling,
parameter binding, transactions, timeouts, routing, and runtime metrics.

It is intended for services that need to:

- manage one or more databases without handling connection lifecycles in business code;
- use parameterized SQL, transactions, batches, streaming reads, and asynchronous calls;
- configure retries, circuit breaking, read/write routing, rate limits, SQL auditing, and caching;
- inspect rendered SQL, slow-query statistics, and connection-pool metrics.

The core uses C++17. The optional coroutine API uses C++20. Database drivers are opt-in and
disabled by default.

## Quick start

### 1. Build

Install CMake, a C++ compiler, and the client development library for your database, then enable
the required driver:

```bash
cmake -S . -B build \
  -DDBMW_ENABLE_POSTGRES=ON \
  -DDBMW_BUILD_EXAMPLES=ON
cmake --build build -j
```

Available switches:

- `DBMW_ENABLE_MYSQL=ON`: MySQL; requires libmysqlclient;
- `DBMW_ENABLE_POSTGRES=ON`: PostgreSQL; requires libpqxx and libpq;
- `DBMW_ENABLE_ODBC=ON`: ODBC databases such as SQL Server and Oracle; requires unixODBC;
- `DBMW_ENABLE_ASYNC_CORO=ON`: enable the C++20 coroutine API.

See the [detailed build instructions](docs/guide_en.md#building-wsl--linux) for Linux and macOS.

### 2. Configure a data source

For PostgreSQL, create `config/datasources.json`:

```json
{
  "default_datasource": "main",
  "pool": {
    "min": 1,
    "max": 8,
    "borrow_timeout_ms": 3000
  },
  "datasources": [
    {
      "name": "main",
      "type": "postgres",
      "host": "127.0.0.1",
      "port": 5432,
      "user": "app",
      "password_env": "APP_DB_PASSWORD",
      "database": "app"
    }
  ]
}
```

```bash
export APP_DB_PASSWORD='your-password'
```

The complete template is at
[config/datasources.json.example](config/datasources.json.example). Prefer `password_env` in
production instead of storing a password in the file.

### 3. Query and execute

Use `?` placeholders. Values are bound natively by the driver:

```cpp
#include "dbmw/dbmw.h"

#include <cstdint>
#include <string>

int main() {
    auto status = dbmw::DBMW::init("config/datasources.json");
    if (!status.ok()) return 1;

    dbmw::common::ResultSet rows;
    dbmw::common::Params params{std::int64_t(42)};
    status = dbmw::DBMW::query(
        "SELECT id, name FROM users WHERE id = ?", params, rows);

    std::int64_t affected = 0;
    if (status.ok()) {
        status = dbmw::DBMW::execute(
            "UPDATE users SET last_seen = now() WHERE id = ?", params, affected);
    }

    dbmw::DBMW::shutdown();
    return status.ok() ? 0 : 1;
}
```

Pass a data-source name as the first argument to target a specific source:

```cpp
dbmw::DBMW::query("analytics", "SELECT count(*) FROM events", rows);
```

### 4. Transactions

Use `transaction()` to keep multiple statements on one connection. A successful callback is
committed; a returned error or exception is rolled back automatically:

```cpp
auto status = dbmw::DBMW::transaction([](dbmw::core::Session &session) {
    std::int64_t affected = 0;
    auto result = session.execute(
        "UPDATE accounts SET balance = balance - ? WHERE id = ?",
        {std::int64_t(100), std::int64_t(1)}, affected);
    if (!result.ok()) return result;

    return session.execute(
        "UPDATE accounts SET balance = balance + ? WHERE id = ?",
        {std::int64_t(100), std::int64_t(2)}, affected);
});
```

### 5. Run tests

```bash
cmake -S . -B build -DDBMW_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The live PostgreSQL integration test also requires the PostgreSQL driver and integration-test
target. Provide connection details through `DBMW_TEST_PG_HOST`, `DBMW_TEST_PG_PORT`,
`DBMW_TEST_PG_USER`, `DBMW_TEST_PG_PASSWORD`, and `DBMW_TEST_PG_DATABASE`:

```bash
cmake -S . -B build \
  -DDBMW_ENABLE_POSTGRES=ON \
  -DDBMW_BUILD_TESTS=ON \
  -DDBMW_BUILD_INTEGRATION_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Detailed documentation

See the [dbmw detailed guide](docs/guide_en.md) for connection pooling, asynchronous APIs,
cursors, failover, observability, error codes, configuration, and driver extensions. See the
[asynchronous design document](docs/async-design-v0.2.0.md) for implementation details.
