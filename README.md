# dbmw — C++ 数据库连接中间件

> English: [README_en.md](README_en.md) · [详细指南](docs/guide.md)

dbmw 为 C++ 应用提供统一的数据库访问层。应用通过同一套 API 使用 MySQL、PostgreSQL
和 ODBC 数据库，并由中间件集中处理连接池、参数绑定、事务、超时、路由和运行指标。

它适合需要以下能力的服务：

- 管理一个或多个数据库连接，避免业务代码直接维护连接生命周期；
- 使用参数化 SQL、事务、批量执行、流式查询和异步调用；
- 统一配置重试、熔断、读写路由、限流、SQL 审计和查询缓存；
- 查看完整 SQL、慢 SQL 与连接池统计数据。

项目使用 C++17；可选协程接口使用 C++20。数据库驱动按需编译，默认全部关闭。

## 快速使用

### 1. 构建

先安装 CMake、C++ 编译器和目标数据库的客户端开发库，然后启用需要的驱动：

```bash
cmake -S . -B build \
  -DDBMW_ENABLE_POSTGRES=ON \
  -DDBMW_BUILD_EXAMPLES=ON
cmake --build build -j
```

可用开关：

- `DBMW_ENABLE_MYSQL=ON`：MySQL，需要 libmysqlclient；
- `DBMW_ENABLE_POSTGRES=ON`：PostgreSQL，需要 libpqxx 和 libpq；
- `DBMW_ENABLE_ODBC=ON`：SQL Server/Oracle 等 ODBC 数据库，需要 unixODBC；
- `DBMW_ENABLE_ASYNC_CORO=ON`：启用 C++20 协程接口。

Linux、macOS 的依赖安装方式见[详细构建说明](docs/guide.md#构建wsl--linux)。

### 2. 配置数据源

以 PostgreSQL 为例，新建 `config/datasources.json`：

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

完整配置模板位于 [config/datasources.json.example](config/datasources.json.example)。生产环境建议使用
`password_env`，不要把密码写入配置文件。

### 3. 查询和执行

SQL 使用 `?` 占位，参数由驱动原生绑定：

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

指定数据源时，把名称作为第一个参数：

```cpp
dbmw::DBMW::query("analytics", "SELECT count(*) FROM events", rows);
```

### 4. 事务

多条语句必须通过 `transaction()` 固定在同一连接上。回调成功时提交，返回失败或抛出异常时自动回滚：

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

### 5. 运行测试

```bash
cmake -S . -B build -DDBMW_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

真实 PostgreSQL 集成测试需要同时启用 PostgreSQL 驱动和集成测试，并通过
`DBMW_TEST_PG_HOST`、`DBMW_TEST_PG_PORT`、`DBMW_TEST_PG_USER`、
`DBMW_TEST_PG_PASSWORD`、`DBMW_TEST_PG_DATABASE` 提供连接信息：

```bash
cmake -S . -B build \
  -DDBMW_ENABLE_POSTGRES=ON \
  -DDBMW_BUILD_TESTS=ON \
  -DDBMW_BUILD_INTEGRATION_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## 详细文档

连接池、异步 API、游标、故障转移、可观测性、错误码、配置项和驱动扩展等内容见
[dbmw 详细指南](docs/guide.md)。异步实现设计见
[异步设计文档](docs/async-design-v0.2.0.md)。
