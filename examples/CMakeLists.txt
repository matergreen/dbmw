add_executable(dbmw_example_basic basic_usage.cpp)
target_link_libraries(dbmw_example_basic PRIVATE dbmw)

# v0.2.0 异步示例：回调/future/取消段在 C++17 下即可编译；
# 协程段由 dbmw 目标传来的 PUBLIC 宏开启，此时本 TU 必须提标 C++20。
add_executable(dbmw_example_async async_example.cpp)
target_link_libraries(dbmw_example_async PRIVATE dbmw)
if(DBMW_ENABLE_ASYNC_CORO)
    set_target_properties(dbmw_example_async PROPERTIES
        CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
endif()
