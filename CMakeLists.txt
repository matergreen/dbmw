cmake_minimum_required(VERSION 3.16)
project(dbmw VERSION 0.2.0 LANGUAGES CXX)

# 生成 compile_commands.json，便于 CLion/clangd/VS Code 正确解析
# 工程的 include 路径与编译选项（消除“找不到头文件”类误报）。
set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE BOOL "" FORCE)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# ---------------------------------------------------------------------------
# 可选数据库驱动（默认全部关闭，保证在干净 WSL 上一行编译通过）
# 开启后需通过 apt 安装对应的 -dev 客户端库（见 scripts/setup-wsl.sh）
# ---------------------------------------------------------------------------
option(DBMW_ENABLE_MYSQL    "Build MySQL driver (needs libmysqlclient)"      OFF)
option(DBMW_ENABLE_POSTGRES "Build PostgreSQL driver (needs libpqxx + libpq)" OFF)
option(DBMW_ENABLE_ODBC     "Build ODBC driver for SQL Server/Oracle"         OFF)
option(DBMW_BUILD_EXAMPLES  "Build example programs"                          OFF)
option(DBMW_BUILD_TESTS     "Build core behaviour tests (no external deps)"   OFF)
# 协程层（设计 §5.3）：仅 src/async/task.cpp 一个 TU 被提标到 C++20，
# 库内其余源文件仍按 C++17 编译。宏以 PUBLIC 定义导出，下游包含
# dbmw/async/task.h 的 TU 会拿到同一开关（且必须自行以 C++20 编译）。
option(DBMW_ENABLE_ASYNC_CORO "Build the C++20 coroutine layer (task.h)"     OFF)

# ---------------------------------------------------------------------------
# nlohmann/json：vendored 单头文件离线可用。为让安装后的 dbmw 包自包含
# （下游 find_package(dbmw) 后无需再 find nlohmann_json），始终把该头文件
# 安装到 include/nlohmann/ 并导出 nlohmann_json 接口目标。
# ---------------------------------------------------------------------------
set(_nl_inc_dir "")
set(_nl_install_src "")
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/nlohmann/json.hpp")
    set(_nl_inc_dir "${CMAKE_CURRENT_SOURCE_DIR}/third_party")
    set(_nl_install_src "${CMAKE_CURRENT_SOURCE_DIR}/third_party/nlohmann/json.hpp")
    message(STATUS "dbmw: using vendored nlohmann/json (will install into include/nlohmann/)")
else()
    find_package(nlohmann_json QUIET)
    if(nlohmann_json_FOUND AND TARGET nlohmann_json::nlohmann_json)
        get_target_property(_nl_inc_dir nlohmann_json::nlohmann_json INTERFACE_INCLUDE_DIRECTORIES)
        find_file(_nl_install_src NAMES nlohmann/json.hpp HINTS ${_nl_inc_dir}
                  NO_DEFAULT_PATH REQUIRED)
        message(STATUS "dbmw: using system nlohmann_json (will install a copy)")
    else()
        include(FetchContent)
        FetchContent_Declare(nlohmann_json
            GIT_REPOSITORY https://github.com/nlohmann/json.git
            GIT_TAG        v3.11.3)
        FetchContent_MakeAvailable(nlohmann_json)
        set(_nl_inc_dir "${nlohmann_json_SOURCE_DIR}/single_include")
        set(_nl_install_src "${nlohmann_json_SOURCE_DIR}/single_include/nlohmann/json.hpp")
        message(STATUS "dbmw: fetched nlohmann_json via FetchContent (will install a copy)")
    endif()
endif()

# 自建一个普通（非 IMPORTED）接口库：BUILD 时指向解析出的头目录，INSTALL 时
# 指向 include/（头文件已随包安装）。它能进入 EXPORT 集，下游因而自给自足。
add_library(nlohmann_json INTERFACE)
target_include_directories(nlohmann_json INTERFACE
    $<BUILD_INTERFACE:${_nl_inc_dir}>
    $<INSTALL_INTERFACE:include>)
install(FILES ${_nl_install_src} DESTINATION include/nlohmann)
install(TARGETS nlohmann_json EXPORT dbmwTargets)

# ---------------------------------------------------------------------------
# 核心源文件（驱动桩始终参与编译，真实客户端代码由 #ifdef 控制）
# ---------------------------------------------------------------------------
set(DBMW_SOURCES
    src/common/types.cpp
    src/common/observer.cpp
    src/common/sql_analyze.cpp
    src/config/datasource_config.cpp
    src/config/config_loader.cpp
    src/async/executor.cpp
    src/async/async_engine.cpp
    src/core/idatabase_connection.cpp
    src/core/connection_pool.cpp
    src/core/heartbeat_manager.cpp
    src/core/database_manager.cpp
    src/core/sql_auditor.cpp
    src/core/query_cache.cpp
    src/core/write_buffer.cpp
    src/core/stats_reporter.cpp
    src/driver/driver_registry.cpp
    src/driver/driver_factory.cpp
    src/driver/mysql_driver.cpp
    src/driver/postgres_driver.cpp
    src/driver/odbc_driver.cpp
    src/dbmw.cpp
)

# 协程层：独立 OBJECT 库，单独提标 C++20。
# 不能用源文件级 CXX_STANDARD 属性——那是目标属性，CMake 不支持按 TU 覆盖
# （实测被目标级 C++17 吞掉）。OBJECT 库的产物直接并入 libdbmw.a，
# 不产生链接依赖，install(EXPORT) 无需导出它。
if(DBMW_ENABLE_ASYNC_CORO)
    add_library(dbmw_coro OBJECT src/async/task.cpp)
    target_include_directories(dbmw_coro PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
    set_target_properties(dbmw_coro PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
    target_compile_definitions(dbmw_coro PRIVATE DBMW_ENABLE_ASYNC_CORO=1)
    set(DBMW_CORO_OBJ "$<TARGET_OBJECTS:dbmw_coro>")
else()
    set(DBMW_CORO_OBJ "")
endif()

add_library(dbmw STATIC ${DBMW_SOURCES} ${DBMW_CORO_OBJ})
target_include_directories(dbmw PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(dbmw PUBLIC nlohmann_json)
if(DBMW_ENABLE_ASYNC_CORO)
    # PUBLIC：下游包含 dbmw/async/task.h 的 TU 拿到同一开关，
    # 且必须自行以 C++20 编译（MSVC 下自动带入 <coroutine> 所需开关，
    # GCC/Clang 10+ 无需额外 flag）。
    target_compile_definitions(dbmw PUBLIC DBMW_ENABLE_ASYNC_CORO=1)
endif()

# ---------------------------------------------------------------------------
# 各驱动的可选依赖链接
# ---------------------------------------------------------------------------
if(DBMW_ENABLE_MYSQL)
    target_compile_definitions(dbmw PRIVATE DBMW_ENABLE_MYSQL)
    find_path(MYSQL_INCLUDE_DIR NAMES mysql.h PATH_SUFFIXES mysql)
    find_library(MYSQL_LIBRARY  NAMES mysqlclient libmysql)
    if(NOT MYSQL_INCLUDE_DIR OR NOT MYSQL_LIBRARY)
        message(FATAL_ERROR "DBMW_ENABLE_MYSQL=ON but libmysqlclient not found. "
            "Run: sudo apt install -y default-libmysqlclient-dev")
    endif()
    target_include_directories(dbmw PRIVATE ${MYSQL_INCLUDE_DIR})
    target_link_libraries(dbmw PRIVATE ${MYSQL_LIBRARY})
endif()

if(DBMW_ENABLE_POSTGRES)
    target_compile_definitions(dbmw PRIVATE DBMW_ENABLE_POSTGRES)
    find_path(PQXX_INCLUDE_DIR NAMES pqxx/pqxx)
    find_library(PQXX_LIBRARY   NAMES pqxx)
    # Homebrew 把 libpq-fe.h 直接装在 <prefix>/include/，而 Ubuntu/Debian 的
    # libpq-dev 装在 <prefix>/include/postgresql/，两者布局不同。这里两种路径
    # 都列上，避免 macOS 上 find_path 找不到而 FATAL_ERROR。dbmw 自身不直接
    # include libpq-fe.h（走 pqxx 封装），该变量主要用于"已安装"的守卫判断。
    find_path(PQ_INCLUDE_DIR    NAMES postgresql/libpq-fe.h libpq-fe.h)
    find_library(PQ_LIBRARY     NAMES pq)
    if(NOT PQXX_INCLUDE_DIR OR NOT PQXX_LIBRARY OR NOT PQ_LIBRARY)
        message(FATAL_ERROR "DBMW_ENABLE_POSTGRES=ON but libpqxx/libpq not found. "
            "Run: sudo apt install -y libpqxx-dev libpq-dev")
    endif()

    # 各发行版自带的 libpqxx 主版本差距极大：Ubuntu 24.04 是 7.8，Homebrew 是 8.0。
    # 8.0 有两个破坏性变化：① 最低 C++ 标准由 17 提到 20；② 删掉了 pqxx::work。
    # ② 已在 postgres_driver.cpp 里用 PgTx 别名消化掉，这里负责 ①：按需单独给
    # dbmw 目标提标，避免出现"Linux 编得过、macOS 编不过"（或反之）的情况。
    file(STRINGS "${PQXX_INCLUDE_DIR}/pqxx/version.hxx" _dbmw_pqxx_major_line
         REGEX "^#[ ]*define[ ]+PQXX_VERSION_MAJOR[ ]+[0-9]+")
    string(REGEX MATCH "([0-9]+)" _dbmw_pqxx_major "${_dbmw_pqxx_major_line}")
    if(NOT _dbmw_pqxx_major)
        message(WARNING "dbmw: 无法从 ${PQXX_INCLUDE_DIR}/pqxx/version.hxx 中解析 "
                        "PQXX_VERSION_MAJOR，按 C++17 构建。")
        set(_dbmw_pqxx_major 7)
    endif()
    if(_dbmw_pqxx_major LESS 7)
        message(FATAL_ERROR "dbmw: 需要 libpqxx >= 7，当前主版本为 ${_dbmw_pqxx_major}。"
            " 6.x 缺少 pqxx::zview / pqxx::params，请改用 Ubuntu 24.04+ 或 Homebrew 的"
            " libpqxx，或关闭 -DDBMW_ENABLE_POSTGRES=OFF。")
    endif()
    message(STATUS "dbmw: libpqxx 主版本 ${_dbmw_pqxx_major}")
    if(_dbmw_pqxx_major GREATER_EQUAL 8)
        message(STATUS "dbmw: libpqxx >= 8 要求 C++20，dbmw 目标单独提标至 C++20")
        set_target_properties(dbmw PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
    endif()

    target_include_directories(dbmw PRIVATE ${PQXX_INCLUDE_DIR} ${PQ_INCLUDE_DIR})
    target_link_libraries(dbmw PRIVATE ${PQXX_LIBRARY} ${PQ_LIBRARY})
endif()

if(DBMW_ENABLE_ODBC)
    target_compile_definitions(dbmw PRIVATE DBMW_ENABLE_ODBC)
    find_path(ODBC_INCLUDE_DIR NAMES sql.h)
    find_library(ODBC_LIBRARY  NAMES odbc)
    if(NOT ODBC_INCLUDE_DIR OR NOT ODBC_LIBRARY)
        message(FATAL_ERROR "DBMW_ENABLE_ODBC=ON but unixODBC not found. "
            "Run: sudo apt install -y unixodbc-dev")
    endif()
    target_include_directories(dbmw PRIVATE ${ODBC_INCLUDE_DIR})
    target_link_libraries(dbmw PRIVATE ${ODBC_LIBRARY})
endif()

# ---------------------------------------------------------------------------
# 子目录
# ---------------------------------------------------------------------------
if(DBMW_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(DBMW_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ---------------------------------------------------------------------------
# 安装与包导出
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# 注意：nlohmann_json 已在上面第 63 行加入 dbmwTargets 导出集，这里不能再列一次，
# 否则 install(EXPORT) 报 "includes target nlohmann_json more than once in the export set"。
install(TARGETS dbmw
        EXPORT dbmwTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY include/dbmw DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# CMake 包配置：下游用 find_package(dbmw) 即可获得 dbmw::dbmw 与 dbmw::nlohmann_json
set(_dbmw_cmake_dir "${CMAKE_INSTALL_LIBDIR}/cmake/dbmw")
configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/dbmwConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/dbmwConfig.cmake"
    INSTALL_DESTINATION "${_dbmw_cmake_dir}")
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/dbmwConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/dbmwConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/dbmwConfigVersion.cmake"
    DESTINATION "${_dbmw_cmake_dir}")
install(EXPORT dbmwTargets
        NAMESPACE dbmw::
        DESTINATION "${_dbmw_cmake_dir}")

# pkg-config（非 CMake 用户）：g++ main.cpp $(pkg-config --cflags --libs dbmw)
#
# prefix 用 pkg-config 内置变量 ${pcfiledir}（展开为 .pc 自身所在目录）推回安装前缀，
# 而不是写死 @CMAKE_INSTALL_PREFIX@——后者在 configure 阶段就被定死，
# 用 --install --prefix <dir> 打出来的包里 .pc 仍指向 /usr/local，解压到别处就失效。
# CMAKE_INSTALL_LIBDIR 为绝对路径时无法用 ${pcfiledir} 反推，退回硬编码。
if(IS_ABSOLUTE "${CMAKE_INSTALL_LIBDIR}" OR IS_ABSOLUTE "${CMAKE_INSTALL_INCLUDEDIR}")
    set(_pc_prefix_expr    "${CMAKE_INSTALL_PREFIX}")
    set(_pc_libdir_expr    "${CMAKE_INSTALL_LIBDIR}")
    set(_pc_includedir_expr "${CMAKE_INSTALL_INCLUDEDIR}")
else()
    # .pc 位于 <prefix>/<LIBDIR>/pkgconfig，故需向上回退 (LIBDIR 层级数 + 1) 级到 prefix：
    #   lib                  -> 1 层 -> ${pcfiledir}/../..
    #   lib/x86_64-linux-gnu -> 2 层 -> ${pcfiledir}/../../..
    string(REPLACE "/" ";" _pc_libdir_parts "${CMAKE_INSTALL_LIBDIR}")
    list(LENGTH _pc_libdir_parts _pc_libdir_depth)
    set(_pc_up "..")
    foreach(_i RANGE 1 ${_pc_libdir_depth})
        string(APPEND _pc_up "/..")
    endforeach()
    set(_pc_prefix_expr     "\${pcfiledir}/${_pc_up}")
    set(_pc_libdir_expr     "${_pc_prefix_expr}/${CMAKE_INSTALL_LIBDIR}")
    set(_pc_includedir_expr "${_pc_prefix_expr}/${CMAKE_INSTALL_INCLUDEDIR}")
endif()
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/dbmw.pc.in"
               "${CMAKE_CURRENT_BINARY_DIR}/dbmw.pc" @ONLY)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/dbmw.pc"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
