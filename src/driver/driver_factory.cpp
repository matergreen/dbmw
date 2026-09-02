#include "dbmw/driver/driver_factory.h"
#include "dbmw/driver/driver_registry.h"
#include "dbmw/driver/mysql_driver.h"
#include "dbmw/driver/postgres_driver.h"
#include "dbmw/driver/odbc_driver.h"


namespace dbmw::driver {
    std::unique_ptr<IDriver> createDriver(const std::string &type) {
        return DriverRegistry::instance().create(type);
    }

    void registerBuiltinDrivers() {
        registerMySQLDriver();
        registerPostgresDriver();
        registerOdbcDriver();
    }
} // namespace dbmw::driver
