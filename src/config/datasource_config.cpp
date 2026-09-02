#include "dbmw/config/datasource_config.h"

#include <string>


namespace dbmw::config {
    std::string DataSourceConfig::describe() const {
        std::string s = name + "[" + type + "]";
        if (!dsn.empty()) {
            s += " dsn=" + dsn;
        } else {
            s += " " + host + ":" + std::to_string(port) + "/" + database;
        }
        s += " user=" + user;
        return s;
    }

    std::string DataSourceConfig::redact(std::string text) const {
        auto replaceAll = [&](const std::string &secret) {
            if (secret.empty()) return;
            std::size_t pos = 0;
            while ((pos = text.find(secret, pos)) != std::string::npos) {
                text.replace(pos, secret.size(), "***");
                pos += 3;
            }
        };
        replaceAll(password);
        const auto connectionString = extra.find("connection_string");
        if (connectionString != extra.end()) replaceAll(connectionString->second);
        return text;
    }
} // namespace dbmw::config
