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

### 5. Entity mapping (optional)

`dbmw/mapping.h` is a header-only adapter layer that moves data between a `ResultSet` and your
structs, following a field declaration you write by hand. It is not an ORM — SQL stays in your
code and the engine core is untouched:

```cpp
#include "dbmw/mapping.h"

struct User {
    std::int64_t id;
    std::string  name;
    std::optional<std::string> email;   // receives SQL NULL
};

template <> struct dbmw::mapping::RowMapper<User> {
    static auto describe() {
        return dbmw::mapping::Mapping<User>()
            .field(&User::id,    "id")
            .field(&User::name,  "name")
            .field(&User::email, "email",
                   dbmw::mapping::FieldFlags::PrimaryKey);
    }
};

auto r = dbmw::queryAs<User>("SELECT id, name, email FROM users WHERE id = ?",
                             {std::int64_t(42)});
if (r.status.ok() && !r.items.empty()) use(r.items[0]);
```

Type mismatches and NULL landing in a non-`optional` member yield `MappingError` (no silent default
values). Missing columns are skipped by default and extra columns ignored; each can be tightened via
`.missingColumns(...)` / `.extraColumns(...)`. The write direction offers `paramsOf` / `insertSql` /
`updateSql` / `insertAs` / `updateAs` / `insertBatchAs`, including generated-key back-fill. On the
async side, `dbmw::async::queryAs<T>` comes in callback / future / coroutine form.

### 6. Run tests

```bash
cmake -S . -B build -DDBMW_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Live integration tests support PostgreSQL, MySQL, and SQL Server (ODBC). Provide connection
details through the `DBMW_TEST_PG_*`, `DBMW_TEST_MYSQL_*`, and `DBMW_TEST_ODBC_*`
environment variables:

```bash
cmake -S . -B build \
  -DDBMW_ENABLE_POSTGRES=ON \
  -DDBMW_ENABLE_MYSQL=ON \
  -DDBMW_ENABLE_ODBC=ON \
  -DDBMW_BUILD_TESTS=ON \
  -DDBMW_BUILD_INTEGRATION_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## Detailed documentation

See the [dbmw detailed guide](docs/guide_en.md) for connection pooling, asynchronous APIs,
cursors, failover, observability, error codes, configuration, and driver extensions. See the
[asynchronous design document](docs/async-design-v0.2.0.md) for implementation details, and the
[mapping design document](docs/mapping-design-v0.5.0.md) for entity mapping.
