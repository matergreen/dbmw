#ifndef DBMW_COMMON_LOGGER_H
#define DBMW_COMMON_LOGGER_H

#include <iostream>
#include <string>
#include <chrono>
#include <ctime>
#include <iomanip>


namespace dbmw::common {
    enum class LogLevel { Debug = 0, Info = 1, Warn = 2, Error = 3 };

    inline const char *logLevelStr(LogLevel l) {
        switch (l) {
            case LogLevel::Debug: return "DEBUG";
            case LogLevel::Info: return "INFO";
            case LogLevel::Warn: return "WARN";
            case LogLevel::Error: return "ERROR";
        }
        return "?";
    }

    // 轻量日志器：输出到 std::clog，带时间戳与级别，可全局设置最低级别。
    class Logger {
    public:
        static LogLevel minLevel() { return minLevel_; }
        static void setMinLevel(LogLevel l) { minLevel_ = l; }

        static void log(LogLevel level, const std::string &msg) {
            if (level < minLevel_) return;
            auto now = std::chrono::system_clock::now();
            auto t = std::chrono::system_clock::to_time_t(now);
            std::tm tm{};
#if defined(_WIN32)
            localtime_s(&tm, &t); // MSVC 安全版本
#else
            localtime_r(&t, &tm); // POSIX（WSL/Linux）
#endif
            char buf[32] = {0};
            std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
            std::cerr << "[" << logLevelStr(level) << "] " << buf << " " << msg << std::endl;
        }

    private:
        static LogLevel minLevel_;
    };

    inline LogLevel Logger::minLevel_ = LogLevel::Info;
} // namespace dbmw::common


#define DBMW_LOG_DEBUG(m) ::dbmw::common::Logger::log(::dbmw::common::LogLevel::Debug, m)
#define DBMW_LOG_INFO(m)  ::dbmw::common::Logger::log(::dbmw::common::LogLevel::Info,  m)
#define DBMW_LOG_WARN(m)  ::dbmw::common::Logger::log(::dbmw::common::LogLevel::Warn,  m)
#define DBMW_LOG_ERROR(m) ::dbmw::common::Logger::log(::dbmw::common::LogLevel::Error, m)

#endif // DBMW_COMMON_LOGGER_H
