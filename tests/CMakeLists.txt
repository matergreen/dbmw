# 核心层行为测试：不依赖任何第三方测试框架，也不需要真实数据库，
# 通过内置的 mock 驱动验证连接池、事务与参数插值的语义。
add_executable(dbmw_core_test dbmw_core_test.cpp)
target_link_libraries(dbmw_core_test PRIVATE dbmw)

# 线程安全相关用例需要 pthread（Linux/WSL 上必须显式链接）。
find_package(Threads QUIET)
if(Threads_FOUND)
    target_link_libraries(dbmw_core_test PRIVATE Threads::Threads)
endif()

add_test(NAME dbmw_core_test COMMAND dbmw_core_test)
