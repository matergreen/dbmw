cmake_minimum_required(VERSION 3.16)
project(dbmw VERSION 0.1.0 LANGUAGES CXX)

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
option(DBMW_ENABLE_ODBC     "Build ODBC driver for SQL Server/Oracle"         ON)
option(DBMW_BUILD_EXAMPLES  "Build example programs"                          OFF)
option(DBMW_BUILD_TESTS     "Build core behaviour tests (no external deps)"   OFF)

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

add_library(dbmw STATIC ${DBMW_SOURCES})
target_include_directories(dbmw PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_link_libraries(dbmw PUBLIC nlohmann_json)

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
    find_path(PQ_INCLUDE_DIR    NAMES libpq-fe.h PATH_SUFFIXES postgresql)
    find_library(PQ_LIBRARY     NAMES pq)
    if(NOT PQXX_INCLUDE_DIR OR NOT PQXX_LIBRARY OR NOT PQ_LIBRARY)
        message(FATAL_ERROR "DBMW_ENABLE_POSTGRES=ON but libpqxx/libpq not found. "
            "Run: sudo apt install -y libpqxx-dev libpq-dev")
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
