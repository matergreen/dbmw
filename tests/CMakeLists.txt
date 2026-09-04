# 核心层行为测试：不依赖任何第三方测试框架，也不需要真实数据库，
# 通过内置的 mock 驱动验证连接池、事务与参数插值的语义。
add_executable(dbmw_core_test dbmw_core_test.cpp)
target_link_libraries(dbmw_core_test PRIVATE dbmw)

# v0.2.0 异步 API 行为测试：mock 驱动 + 单 worker 执行器，
# 覆盖设计 §14 的 T5–T13（生命周期/重试/取消/超时/一致性矩阵）。
add_executable(dbmw_async_test dbmw_async_test.cpp)
target_link_libraries(dbmw_async_test PRIVATE dbmw)

# 真实 PostgreSQL 集成测试。默认不构建，避免普通单元测试依赖外部服务；
# 开启时由 DBMW_TEST_PG_* 环境变量提供连接信息，测试只操作自己的临时 schema。
if(DBMW_BUILD_INTEGRATION_TESTS AND DBMW_ENABLE_POSTGRES)
    add_executable(dbmw_postgres_integration_test dbmw_postgres_integration_test.cpp)
    target_link_libraries(dbmw_postgres_integration_test PRIVATE dbmw)
    add_test(NAME dbmw_postgres_integration_test COMMAND dbmw_postgres_integration_test)
endif()

# T15 协程层测试：仅在 DBMW_ENABLE_ASYNC_CORO=ON 时构建。
# 该 TU 包含 dbmw/async/task.h（Task/promise/awaiter 全是头文件模板），
# 必须以 C++20 编译。
if(DBMW_ENABLE_ASYNC_CORO)
    add_executable(dbmw_coro_test dbmw_coro_test.cpp)
    target_link_libraries(dbmw_coro_test PRIVATE dbmw)
    set_target_properties(dbmw_coro_test PROPERTIES
        CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
    add_test(NAME dbmw_coro_test COMMAND dbmw_coro_test)
endif()

# 线程安全相关用例需要 pthread（Linux/WSL 上必须显式链接）。
find_package(Threads QUIET)
if(Threads_FOUND)
    target_link_libraries(dbmw_core_test PRIVATE Threads::Threads)
    target_link_libraries(dbmw_async_test PRIVATE Threads::Threads)
    if(TARGET dbmw_coro_test)
        target_link_libraries(dbmw_coro_test PRIVATE Threads::Threads)
    endif()
    if(TARGET dbmw_postgres_integration_test)
        target_link_libraries(dbmw_postgres_integration_test PRIVATE Threads::Threads)
    endif()
endif()

add_test(NAME dbmw_core_test COMMAND dbmw_core_test)
add_test(NAME dbmw_async_test COMMAND dbmw_async_test)
