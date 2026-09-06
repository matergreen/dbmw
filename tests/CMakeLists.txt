# 核心层行为测试：不依赖任何第三方测试框架，也不需要真实数据库，
# 通过内置的 mock 驱动验证连接池、事务与参数插值的语义。
add_executable(dbmw_core_test dbmw_core_test.cpp)
target_link_libraries(dbmw_core_test PRIVATE dbmw)

# v0.2.0 异步 API 行为测试：mock 驱动 + 单 worker 执行器，
# 覆盖设计 §14 的 T5–T13（生命周期/重试/取消/超时/一致性矩阵）。
add_executable(dbmw_async_test dbmw_async_test.cpp)
target_link_libraries(dbmw_async_test PRIVATE dbmw)

# M1 SPI 上下文层单测：SqlContext / ContextScope / W3C traceparent 解析、
# nextSpanId 行为、LIFO 不变量、线程局部性。无第三方依赖。
add_executable(dbmw_context_test dbmw_context_test.cpp)
target_link_libraries(dbmw_context_test PRIVATE dbmw)

# M1 SPI 拦截器层单测：InterceptorRegistry 生命周期 / I11 异常吞掉 /
# I9 分层 / I7 递归防护。extern 引用 core::detail 的实现入口。
add_executable(dbmw_interceptor_test dbmw_interceptor_test.cpp)
target_link_libraries(dbmw_interceptor_test PRIVATE dbmw)

# M2 追踪上下文层单测：emitSql 自动注入 trace/span / 嵌套取栈顶 /
# SlowSqlRecord 同源 / emit 不带 trace。无第三方依赖。
add_executable(dbmw_observability_test dbmw_observability_test.cpp)
target_link_libraries(dbmw_observability_test PRIVATE dbmw)

# M3 指标导出层单测：PoolMetricsEvent 透传 + Prometheus 文本格式
# 正确性（HELP/TYPE、标签转义、histogram +Inf 桶、截断）。
add_executable(dbmw_exporters_test dbmw_exporters_test.cpp)
target_link_libraries(dbmw_exporters_test PRIVATE dbmw)

# M4 动态数据源单测：运行时 addDataSource / addGroup / removeDataSource /
# removeGroup 的成功路径、重名拒绝、引用完整性、ack 校验、并发安全、grace
# 宽限期。无真实数据库依赖。
add_executable(dbmw_dynamic_test dbmw_dynamic_test.cpp)
target_link_libraries(dbmw_dynamic_test PRIVATE dbmw)

# M5 幂等声明单测：Idempotency 三态叠加到重试决策（同步 resolveWriteAttempts
# + 异步 maxAttempts）。通过"注入 N 次可重试失败 + 数 execute/query 调用次数"
# 验证 attempts，直接命中真实分支。无真实数据库依赖。
add_executable(dbmw_idempotency_test dbmw_idempotency_test.cpp)
target_link_libraries(dbmw_idempotency_test PRIVATE dbmw)

# M6 影子库路由单测：onRoute 置位 ctx.shadow=true 后，路由层把读/写打
# 到影子数据源；影子写不进写缓冲（I12）、影子读不进查询缓存；配置校验
# 拒绝（影子源不存在/是本组主/是本组副本/是另一个组名）。无真实数据库依赖。
add_executable(dbmw_shadow_test dbmw_shadow_test.cpp)
target_link_libraries(dbmw_shadow_test PRIVATE dbmw)

# M7 结果脱敏单测：查询缓存保存驱动原始结果，SPI afterExecution 在返回前
# 按当前请求改写副本并置位 rs.transformed=true，I10 落地；
# 缓存命中路径（同步包在 runWithInterceptors 内，异步提交时手动调）
# 仍要走 afterExecution，否则脱敏被绕过（§9.4 风险行）。无真实数据库依赖。
add_executable(dbmw_redaction_test dbmw_redaction_test.cpp)
target_link_libraries(dbmw_redaction_test PRIVATE dbmw)

# M8 会话级读后写单测：SqlContext.wroteInThisRequest 标记——
# ① 同步/异步写成功后置位（DataSource::markWrite 集中真源）；
# ② readTarget 检测到则返回 primary_（§10.2 优先级最高）；
# ③ 异步路径通过 entryCtx 快照透传；
# ④ config_loader 对"副本 + 零窗口"打 stderr WARN；
# ⑤ 与 M5 幂等、M6 影子正交不冲突。无真实数据库依赖。
add_executable(dbmw_raw_session_test dbmw_raw_session_test.cpp)
target_link_libraries(dbmw_raw_session_test PRIVATE dbmw)

# M9 观测层延展单测：OperationEvent.shadow / .transformed 字段填充。
#   - shadow 由 emitSql 内读栈顶 SqlContext（同步路径由 runWithInterceptors
#     push 路由后的 view.ctx；异步路径 worker 已 push entryCtx）；
#   - transformed 由 observeSql 透传 ResultSet 指针，emitSql 读 result-
#     >transformed 写进 event（query 类以外 result=nullptr，保持默认 false）。
#   不走真实驱动，全用 emitSql 直调，验证字段注入逻辑独立稳定。
add_executable(dbmw_observer_event_test dbmw_observer_event_test.cpp)
target_link_libraries(dbmw_observer_event_test PRIVATE dbmw)

# 真实数据库集成测试默认不构建，避免普通单元测试依赖外部服务；
# 开启时由各 DBMW_TEST_* 环境变量提供连接信息，测试只操作自己的临时对象。
if(DBMW_BUILD_INTEGRATION_TESTS AND DBMW_ENABLE_POSTGRES)
    add_executable(dbmw_postgres_integration_test dbmw_postgres_integration_test.cpp)
    target_link_libraries(dbmw_postgres_integration_test PRIVATE dbmw)
    add_test(NAME dbmw_postgres_integration_test COMMAND dbmw_postgres_integration_test)
endif()

if(DBMW_BUILD_INTEGRATION_TESTS AND DBMW_ENABLE_MYSQL)
    add_executable(dbmw_mysql_integration_test dbmw_mysql_integration_test.cpp)
    target_link_libraries(dbmw_mysql_integration_test PRIVATE dbmw)
    add_test(NAME dbmw_mysql_integration_test COMMAND dbmw_mysql_integration_test)
endif()

if(DBMW_BUILD_INTEGRATION_TESTS AND DBMW_ENABLE_ODBC)
    add_executable(dbmw_odbc_integration_test dbmw_odbc_integration_test.cpp)
    target_link_libraries(dbmw_odbc_integration_test PRIVATE dbmw)
    add_test(NAME dbmw_odbc_integration_test COMMAND dbmw_odbc_integration_test)
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
    target_link_libraries(dbmw_interceptor_test PRIVATE Threads::Threads)
    target_link_libraries(dbmw_observability_test PRIVATE Threads::Threads)
    target_link_libraries(dbmw_exporters_test PRIVATE Threads::Threads)
    target_link_libraries(dbmw_dynamic_test PRIVATE Threads::Threads)
    target_link_libraries(dbmw_idempotency_test PRIVATE Threads::Threads)
    target_link_libraries(dbmw_shadow_test PRIVATE Threads::Threads)
    target_link_libraries(dbmw_redaction_test PRIVATE Threads::Threads)
    target_link_libraries(dbmw_raw_session_test PRIVATE Threads::Threads)
    target_link_libraries(dbmw_observer_event_test PRIVATE Threads::Threads)
    if(TARGET dbmw_coro_test)
        target_link_libraries(dbmw_coro_test PRIVATE Threads::Threads)
    endif()
    if(TARGET dbmw_postgres_integration_test)
        target_link_libraries(dbmw_postgres_integration_test PRIVATE Threads::Threads)
    endif()
    if(TARGET dbmw_mysql_integration_test)
        target_link_libraries(dbmw_mysql_integration_test PRIVATE Threads::Threads)
    endif()
    if(TARGET dbmw_odbc_integration_test)
        target_link_libraries(dbmw_odbc_integration_test PRIVATE Threads::Threads)
    endif()
endif()

add_test(NAME dbmw_core_test COMMAND dbmw_core_test)
add_test(NAME dbmw_async_test COMMAND dbmw_async_test)
add_test(NAME dbmw_context_test COMMAND dbmw_context_test)
add_test(NAME dbmw_interceptor_test COMMAND dbmw_interceptor_test)
add_test(NAME dbmw_observability_test COMMAND dbmw_observability_test)
add_test(NAME dbmw_exporters_test COMMAND dbmw_exporters_test)
add_test(NAME dbmw_dynamic_test COMMAND dbmw_dynamic_test)
add_test(NAME dbmw_idempotency_test COMMAND dbmw_idempotency_test)
add_test(NAME dbmw_shadow_test COMMAND dbmw_shadow_test)
add_test(NAME dbmw_redaction_test COMMAND dbmw_redaction_test)
add_test(NAME dbmw_raw_session_test COMMAND dbmw_raw_session_test)
add_test(NAME dbmw_observer_event_test COMMAND dbmw_observer_event_test)
