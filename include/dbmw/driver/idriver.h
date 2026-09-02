#ifndef DBMW_DRIVER_IDRIVER_H
#define DBMW_DRIVER_IDRIVER_H

#include "dbmw/core/idatabase_connection.h"
#include <memory>


namespace dbmw::driver {
    // 数据库驱动抽象：每种数据库实现一个驱动，负责生产连接对象。
    // 新增数据库类型只需实现 IDriver 并通过 DriverRegistry 注册。
    class IDriver {
    public:
        virtual ~IDriver() = default;

        // 驱动标识（与 datasources.json 中的 type 对应，如 "mysql"）。
        virtual const char *name() const = 0;

        // 创建一个（未连接）的连接对象。
        virtual std::unique_ptr<core::IDatabaseConnection> createConnection() = 0;
    };
} // namespace dbmw::driver


#endif // DBMW_DRIVER_IDRIVER_H
