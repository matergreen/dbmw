#ifndef DBMW_DRIVER_DRIVER_FACTORY_H
#define DBMW_DRIVER_DRIVER_FACTORY_H

#include "dbmw/driver/idriver.h"

#include <memory>
#include <string>


namespace dbmw::driver {
    // 按类型名创建驱动（内部走 DriverRegistry）。
    std::unique_ptr<IDriver> createDriver(const std::string &type);

    // 注册所有内置驱动（mysql / postgres / odbc）。
    // 在 DatabaseManager::init 的开头调用，确保类型名可用。
    void registerBuiltinDrivers();
} // namespace dbmw::driver


#endif // DBMW_DRIVER_DRIVER_FACTORY_H
