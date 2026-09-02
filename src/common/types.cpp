#include "dbmw/common/types.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <iomanip>
#include <istream>
#include <sstream>
#include <string>
#include <variant>
#include <vector>


namespace dbmw::common {
    const char *errorCodeToString(const ErrorCode c) {
        switch (c) {
            case ErrorCode::Ok:              return "Ok";
            case ErrorCode::ConfigError:     return "ConfigError";
            case ErrorCode::ConnectionFailed:return "ConnectionFailed";
            case ErrorCode::QueryError:      return "QueryError";
            case ErrorCode::QueryTimeout:    return "QueryTimeout";
            case ErrorCode::Cancelled:       return "Cancelled";
            case ErrorCode::ConstraintViolation:return "ConstraintViolation";
            case ErrorCode::Deadlock:        return "Deadlock";
            case ErrorCode::PingFailed:      return "PingFailed";
            case ErrorCode::TxError:         return "TxError";
            case ErrorCode::PoolExhausted:   return "PoolExhausted";
            case ErrorCode::PoolClosed:      return "PoolClosed";
            case ErrorCode::CircuitOpen:     return "CircuitOpen";
            case ErrorCode::NotConnected:    return "NotConnected";
            case ErrorCode::DriverDisabled:  return "DriverDisabled";
            case ErrorCode::UnknownDriver:   return "UnknownDriver";
            case ErrorCode::NotSupported:    return "NotSupported";
            case ErrorCode::RateLimited:     return "RateLimited";
            case ErrorCode::SqlBlocked:      return "SqlBlocked";
            case ErrorCode::Buffered:        return "Buffered";
            case ErrorCode::CursorClosed:     return "CursorClosed";
            case ErrorCode::CursorLimit:      return "CursorLimit";
            case ErrorCode::CursorError:      return "CursorError";
            case ErrorCode::Unknown:         break;
        }
        return "Unknown";
    }

    Status Status::databaseError(const ErrorCode fallback, std::string msg,
                                 std::string state, const std::int64_t vendorCode) {
        Status status = Status::error(fallback, std::move(msg));
        status.sqlState = std::move(state);
        status.nativeCode = vendorCode;

        const std::string sqlClass = status.sqlState.size() >= 2
            ? status.sqlState.substr(0, 2) : std::string();
        if (status.sqlState == "HYT00" || status.sqlState == "HYT01") {
            status.code = ErrorCode::QueryTimeout;
            status.retryable = true;
        } else if (status.sqlState == "57014") {
            if (status.message.find("timeout") != std::string::npos) {
                status.code = ErrorCode::QueryTimeout;
                status.retryable = true;
            } else {
                status.code = ErrorCode::Cancelled;
            }
        } else if (status.sqlState == "HY008") {
            status.code = ErrorCode::Cancelled;
        } else if (status.sqlState == "40001" || status.sqlState == "40P01" ||
                   sqlClass == "40") {
            status.code = ErrorCode::Deadlock;
            status.retryable = true;
        } else if (sqlClass == "23") {
            status.code = ErrorCode::ConstraintViolation;
        } else if (sqlClass == "08") {
            status.connectionBroken = true;
            status.retryable = true;
            if (fallback == ErrorCode::QueryError || fallback == ErrorCode::TxError)
                status.code = ErrorCode::ConnectionFailed;
        }
        return status;
    }

    namespace {
        // 把 time_point 拆成 civil time；秒以下单独由 fracNs 返回。
        std::tm toLocalTm(const Timestamp &t, long long &fracNs) {
            const auto secs = std::chrono::floor<std::chrono::seconds>(t.time_since_epoch());
            fracNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         t.time_since_epoch() - secs).count();
            const auto tt = static_cast<std::time_t>(secs.count());
            std::tm tm{};
#if defined(_WIN32)
            localtime_s(&tm, &tt);
#else
            localtime_r(&tt, &tm);
#endif
            return tm;
        }

        std::string formatTimestamp(const Timestamp &t, bool withMillis) {
            long long fracNs = 0;
            const std::tm tm = toLocalTm(t, fracNs);
            char buf[32] = {0};
            std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
            std::string s(buf);
            if (withMillis) {
                const long long ms = (fracNs / 1000000LL) % 1000LL;
                std::ostringstream os;
                os << '.' << std::setw(3) << std::setfill('0') << ms;
                s += os.str();
            }
            return s;
        }

        void appendHex(std::string &out, const Blob &b) {
            static const char *kHex = "0123456789ABCDEF";
            for (std::uint8_t byte: b) {
                out.push_back(kHex[(byte >> 4) & 0x0F]);
                out.push_back(kHex[byte & 0x0F]);
            }
        }
    } // namespace

    std::string timestampToString(const Timestamp &t) {
        return formatTimestamp(t, false);
    }

    std::string timestampToStringMs(const Timestamp &t) {
        return formatTimestamp(t, true);
    }

    bool tryParseTimestamp(const std::string &s, Timestamp &out) {
        // 手动解析，避免依赖 locale 与各平台 strptime 的行为差异。
        auto readInt = [&](size_t &i, const int width, int &val) -> bool {
            if (i + static_cast<size_t>(width) > s.size()) return false;
            val = 0;
            for (int k = 0; k < width; ++k) {
                const char c = s[i + static_cast<size_t>(k)];
                if (c < '0' || c > '9') return false;
                val = val * 10 + (c - '0');
            }
            i += static_cast<size_t>(width);
            return true;
        };

        size_t i = 0;
        int y = 0, mo = 0, d = 0, h = 0, mi = 0, se = 0;
        if (!readInt(i, 4, y)) return false;
        if (i >= s.size() || s[i] != '-') return false;
        ++i;
        if (!readInt(i, 2, mo)) return false;
        if (i >= s.size() || s[i] != '-') return false;
        ++i;
        if (!readInt(i, 2, d)) return false;

        if (i < s.size() && (s[i] == ' ' || s[i] == 'T')) {
            ++i;
            if (!readInt(i, 2, h)) return false;
            if (i >= s.size() || s[i] != ':') return false;
            ++i;
            if (!readInt(i, 2, mi)) return false;
            if (i >= s.size() || s[i] != ':') return false;
            ++i;
            if (!readInt(i, 2, se)) return false;
        }

        long long fracNs = 0;
        if (i < s.size() && s[i] == '.') {
            ++i;
            long long v = 0;
            int digits = 0;
            while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
                if (digits < 9) { v = v * 10 + (s[i] - '0'); ++digits; }
                ++i;
            }
            while (digits < 9) { v *= 10; ++digits; } // 归一到纳秒
            fracNs = v;
        }

        if (mo < 1 || mo > 12 || d < 1 || d > 31 || h < 0 || h > 23 ||
            mi < 0 || mi > 59 || se < 0 || se > 60 || y < 1900) {
            return false;
        }

        std::tm tm{};
        tm.tm_year = y - 1900;
        tm.tm_mon = mo - 1;
        tm.tm_mday = d;
        tm.tm_hour = h;
        tm.tm_min = mi;
        tm.tm_sec = se;
        tm.tm_isdst = -1; // 交由 mktime 判断夏令时
        const std::time_t tt = std::mktime(&tm);
        if (tt == static_cast<std::time_t>(-1)) return false;

        // 各平台 system_clock 精度不同（Linux 多为纳秒、macOS 为微秒），
        // 统一转回时钟自身的 duration 再相加，避免类型不匹配。
        out = std::chrono::system_clock::from_time_t(tt)
              + std::chrono::duration_cast<Timestamp::duration>(
                  std::chrono::nanoseconds(fracNs));
        return true;
    }

    std::string valueToString(const Value &v) {
        if (std::holds_alternative<std::nullptr_t>(v)) return "NULL";
        if (const auto *p = std::get_if<bool>(&v)) return *p ? "true" : "false";
        if (const auto *p = std::get_if<std::int64_t>(&v)) return std::to_string(*p);
        if (const auto *p = std::get_if<double>(&v)) {
            std::ostringstream os;
            os << std::setprecision(17) << *p;
            return os.str();
        }
        if (const auto *p = std::get_if<std::string>(&v)) return *p;
        if (const auto *p = std::get_if<Timestamp>(&v)) return timestampToStringMs(*p);
        if (const auto *p = std::get_if<Blob>(&v)) {
            std::string s = "blob[" + std::to_string(p->size()) + "]:";
            const size_t show = std::min<size_t>(p->size(), 16);
            std::string hex;
            for (size_t i = 0; i < show; ++i) {
                std::uint8_t byte = (*p)[i];
                static const char *kHex = "0123456789ABCDEF";
                hex.push_back(kHex[(byte >> 4) & 0x0F]);
                hex.push_back(kHex[byte & 0x0F]);
            }
            s += hex;
            if (p->size() > show) s += "...";
            return s;
        }
        return "?";
    }

    std::string escapeLiteralGeneric(const Value &v) {
        if (std::holds_alternative<std::nullptr_t>(v)) return "NULL";
        if (const auto *p = std::get_if<bool>(&v)) return *p ? "TRUE" : "FALSE";
        if (const auto *p = std::get_if<std::int64_t>(&v)) return std::to_string(*p);
        if (const auto *p = std::get_if<double>(&v)) {
            // NaN/Inf 无法用 SQL 字面量表达，退化成 NULL 而不是产生语法错误。
            if (!std::isfinite(*p)) return "NULL";
            std::ostringstream os;
            os << std::setprecision(17) << *p;
            return os.str();
        }
        if (const auto *p = std::get_if<Timestamp>(&v)) {
            std::string s = "'";
            s += timestampToStringMs(*p);
            s += '\'';
            return s;
        }
        if (const auto *p = std::get_if<Blob>(&v)) {
            // 标准 SQL 的二进制字面量写法。方言差异较大，
            // 具体驱动应覆盖 escapeLiteral() 给出本方言的正确形式。
            std::string s = "X'";
            appendHex(s, *p);
            s += '\'';
            return s;
        }
        if (const auto *p = std::get_if<std::string>(&v)) {
            std::string s = "'";
            for (const char c: *p) {
                if (c == '\'') s += "''"; // SQL 标准：单引号翻倍
                else s.push_back(c);
            }
            s += '\'';
            return s;
        }
        return "NULL";
    }

    std::string quoteIdentifier(const std::string &ident) {
        std::string s = "\"";
        for (const char c: ident) {
            if (c == '"') s += "\"\"";
            else s.push_back(c);
        }
        s += '\"';
        return s;
    }

    std::int64_t GeneratedKeys::lastInsertId() const {
        if (rows.empty()) return 0;

        // 取首行首列。列顺序按 ResultSet::fields() 声明的 SELECT 顺序；
        // 驱动没声明 fields 时退化为 Row 内部的字典序首键。
        const auto &firstRow = rows.rows().front();
        std::string column;
        const auto &fields = rows.fields();
        if (!fields.empty()) {
            column = fields.front();
        } else {
            const auto &data = firstRow.data();
            if (data.empty()) return 0;
            column = data.begin()->first;
        }

        const Value &v = firstRow.at(column);
        if (const auto *i = std::get_if<std::int64_t>(&v)) return *i;
        // PG / ODBC 的 RETURNING 可能把 int8 以文本形式送回，容错解析一次。
        if (const auto *s = std::get_if<std::string>(&v)) {
            try {
                return static_cast<std::int64_t>(std::stoll(*s));
            } catch (...) {
                return 0; // 非数字文本：调用方应直接读 rows 自行解释
            }
        }
        return 0;
    }

    StreamSource::StreamSource(std::istream &in, const bool isBinary) : isBinary_(isBinary) {
        // 按引用捕获：流必须在本次执行期间存活。
        // StreamSource 的副本共享同一个流与读位置——这与"顺序读一次"的语义一致。
        read_ = [&in](void *buf, const std::size_t n) -> std::size_t {
            if (!in.good()) return 0;
            in.read(static_cast<char *>(buf), static_cast<std::streamsize>(n));
            const std::streamsize got = in.gcount();
            return got > 0 ? static_cast<std::size_t>(got) : 0;
        };
    }

    Status streamParamsToParams(const StreamParams &params, Params &out) {
        out.clear();
        out.reserve(params.size());

        for (const auto &param: params) {
            if (const auto *src = std::get_if<StreamSource>(&param)) {
                // 参数是 const 的，而 read() 要推进读位置，只能拷一份——
                // 拷贝共享底层流，读位置是同一个，正是期望行为。
                StreamSource source = *src;
                Blob blob;
                // 有预告长度就预留，但封顶 64MB：一个被伪造的巨大 totalSize
                // 不该直接把进程内存打爆。
                if (const auto total = source.totalSize()) {
                    constexpr std::uint64_t kReserveCap = 64ULL * 1024 * 1024;
                    blob.reserve(static_cast<std::size_t>(std::min(*total, kReserveCap)));
                }
                std::array<char, 8192> buf{};
                std::size_t n = 0;
                while ((n = source.read(buf.data(), buf.size())) > 0)
                    blob.insert(blob.end(), buf.data(), buf.data() + static_cast<std::ptrdiff_t>(n));
                out.emplace_back(std::move(blob));
            } else {
                out.push_back(std::get<Value>(param));
            }
        }
        return Status::OK();
    }

    std::string paramTypeSignature(const Params &params) {
        std::string sig;
        sig.reserve(params.size());
        for (const auto &v: params) {
            if (std::holds_alternative<std::nullptr_t>(v)) sig.push_back('n');
            else if (std::holds_alternative<bool>(v)) sig.push_back('b');
            else if (std::holds_alternative<std::int64_t>(v)) sig.push_back('i');
            else if (std::holds_alternative<double>(v)) sig.push_back('d');
            else if (std::holds_alternative<std::string>(v)) sig.push_back('s');
            else if (std::holds_alternative<Timestamp>(v)) sig.push_back('t');
            else sig.push_back('x'); // Blob：Value 变体的兜底分支
        }
        return sig;
    }
} // namespace dbmw::common
