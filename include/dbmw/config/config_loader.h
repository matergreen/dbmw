#ifndef DBMW_CONFIG_CONFIG_LOADER_H
#define DBMW_CONFIG_CONFIG_LOADER_H

#include "dbmw/config/datasource_config.h"
#include <string>


namespace dbmw::config {
    // 从 JSON 文件加载全局配置。
    class ConfigLoader {
    public:
        // 成功返回 true；失败返回 false，并通过 error 输出原因。
        static bool loadFromFile(const std::string &path, GlobalConfig &out, std::string &error);
    };
} // namespace dbmw::config


#endif // DBMW_CONFIG_CONFIG_LOADER_H
