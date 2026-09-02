#include "dbmw/driver/driver_registry.h"

#include <memory>
#include <utility>


namespace dbmw::driver {
    DriverRegistry &DriverRegistry::instance() {
        static DriverRegistry r;
        return r;
    }

    void DriverRegistry::registerDriver(const std::string &type, DriverFactoryFn fn) {
        factories_[type] = std::move(fn);
    }

    bool DriverRegistry::has(const std::string &type) const {
        return factories_.find(type) != factories_.end();
    }

    std::unique_ptr<IDriver> DriverRegistry::create(const std::string &type) const {
        auto it = factories_.find(type);
        if (it == factories_.end()) return nullptr;
        return it->second();
    }

    std::vector<std::string> DriverRegistry::registeredTypes() const {
        std::vector<std::string> v;
        v.reserve(factories_.size());
        for (const auto &p: factories_) v.push_back(p.first);
        return v;
    }
} // namespace dbmw::driver
