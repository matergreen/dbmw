#ifndef DBMW_DRIVER_DRIVER_REGISTRY_H
#define DBMW_DRIVER_DRIVER_REGISTRY_H

#include "dbmw/driver/idriver.h"

#include <string>
#include <functional>
#include <map>
#include <memory>
#include <vector>


namespace dbmw::driver {
    // type 字符串 -> 驱动工厂函数。
    using DriverFactoryFn = std::function<std::unique_ptr<IDriver>()>;

    // 驱动注册表（单例）：按 type 名字创建对应驱动。
    // 内置驱动在 DatabaseManager::init 时通过 registerBuiltinDrivers() 注册；
    // 第三方/自定义驱动可在程序启动时自行 registerDriver()。
    class DriverRegistry {
    public:
        static DriverRegistry &instance();

        void registerDriver(const std::string &type, DriverFactoryFn fn);

        bool has(const std::string &type) const;

        std::unique_ptr<IDriver> create(const std::string &type) const;

        std::vector<std::string> registeredTypes() const;

    private:
        DriverRegistry() = default;

        std::map<std::string, DriverFactoryFn> factories_;
    };
} // namespace dbmw::driver


#endif // DBMW_DRIVER_DRIVER_REGISTRY_H
