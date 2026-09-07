#ifndef DBMW_MAPPING_H
#define DBMW_MAPPING_H

// ---------------------------------------------------------------------------
// v0.5.0 结果集实体映射层（设计：docs/mapping-design-v0.5.0.md）
//
// 定位：**结果集 ↔ 业务实体的适配层**，不是 ORM。
//   - 做：行 → 对象（读）、对象 → 绑定参数（写），严格报错、显式声明。
//   - 不做：关系映射 / 关联加载 / 懒加载 / 脏跟踪 / 自动生成业务 SQL / 实体缓存。
//
// 形态：header-only、全模板、零引擎改动。不修改 dbmw.h / dbmw_async.h /
// database_manager.cpp / async_engine.cpp 的任何既有签名（I4）。
//
// 使用三步：
//   1) 业务侧特化 dbmw::mapping::RowMapper<T> 并提供 describe()；
//   2) 读：dbmw::queryAs<T>(...) / queryOneAs<T> / queryEachAs<T>；
//      写：dbmw::insertAs<T>(...) / updateAs<T> / insertBatchAs<T>；
//   3) 异步三形态在 dbmw::async::queryAs<T>（回调 / future / 协程）。
//
// 宽松模式（用户决策 v0.5.0）：类型不符、NULL 落进非 std::optional 目标
// 仍返回 ErrorCode::MappingError（这两类静默填值是线上最难查的 bug）；
// 但「声明的列在结果集中缺失」默认**跳过**该字段（保持默认构造值），
// 不报错、不返回半成品（I5 / I8）。需要严格时可 `.missingColumns(MissingColumns::Error)`。
//
// 关键实现约束：
//   - 判缺列必须用 row.data().find()：Row::at() 对缺失列返回静态 NULL，
//     会把"SQL 少查一列"伪装成"这列是 NULL"（设计 C2）。
//   - 映射发生在查询缓存命中之后、SPI 脱敏之后（I1 / I2）。
//   - 异步映射发生在完成投递线程（默认主执行器 worker；注入 asio 时为
//     io_context 线程），必须保持轻量（I7）。
// ---------------------------------------------------------------------------

#include "dbmw/common/types.h"
#include "dbmw/core/cursor.h"
#include "dbmw/core/database_manager.h"
#include "dbmw/dbmw.h"
#include "dbmw/async/dbmw_async.h"

#if defined(DBMW_ENABLE_ASYNC_CORO)
#include "dbmw/async/task.h"
#endif

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace dbmw {
    // 读结果：status 非 Ok 时 items 保证为空（I8）。
    template<class T>
    struct EntityResult {
        common::Status status;
        std::vector<T> items;
    };

    // 单行结果：零行 → value 为空且状态为 Ok（不是错误）；多于一行 → 报错。
    template<class T>
    struct EntityOne {
        common::Status status;
        std::optional<T> value;
    };

    // 写结果：keys 只有 insert 形态会填（且依赖驱动/语句是否给出生成键）。
    template<class T>
    struct WriteResult {
        common::Status status;
        std::int64_t affected = 0;
        common::GeneratedKeys keys;
    };

    template<class T>
    struct BatchWriteResult {
        common::Status status;
        common::BatchResult batch;
    };
} // namespace dbmw

namespace dbmw::mapping {
    // ---- 字段标志（位掩码，可组合）----
    enum class FieldFlags : unsigned {
        None = 0,
        PrimaryKey = 1u << 0, // 参与 UPDATE ... WHERE（多个 = 复合主键）
        Generated = 1u << 1, // 数据库生成：INSERT 参数跳过，回填时接收
        ReadOnly = 1u << 2, // 视图/计算列：写方向跳过
        Lossy = 1u << 3, // 允许有损数值转换（Decimal / string → 数值）
        Textual = 1u << 4 // 允许与文本表示互转（Date/Time/Uuid/Json/Decimal ↔ string）
    };

    constexpr FieldFlags operator|(const FieldFlags a, const FieldFlags b) noexcept {
        return static_cast<FieldFlags>(static_cast<unsigned>(a) | static_cast<unsigned>(b));
    }

    constexpr bool hasFlag(const FieldFlags v, const FieldFlags bit) noexcept {
        return (static_cast<unsigned>(v) & static_cast<unsigned>(bit)) != 0u;
    }

    // 结果集中出现未声明列时的策略：默认忽略（兼容 SELECT * 与联表）。
    enum class ExtraColumns { Ignore, Error };

    // 实体声明的列在结果集中缺失时的策略：默认忽略（跳过该字段，保持默认值）。
    // 与 ExtraColumns 对称；严格校验需显式设为 Error。
    enum class MissingColumns { Ignore, Error };

    // 写方向取哪些列。
    enum class WriteCols { Writable, All, PrimaryKey };

    namespace detail {
        template<class...>
        struct AlwaysFalse : std::false_type {
        };

        template<class T>
        struct IsOptional : std::false_type {
        };

        template<class T>
        struct IsOptional<std::optional<T> > : std::true_type {
        };
    } // namespace detail

    // ---- 目标类型名（仅用于错误信息，不依赖 RTTI）----
    template<class U>
    struct TypeName {
        static std::string name() {
            if constexpr (std::is_same_v<U, bool>) return "bool";
            else if constexpr (std::is_same_v<U, std::int8_t>) return "int8_t";
            else if constexpr (std::is_same_v<U, std::int16_t>) return "int16_t";
            else if constexpr (std::is_same_v<U, std::int32_t>) return "int32_t";
            else if constexpr (std::is_same_v<U, std::int64_t>) return "int64_t";
            else if constexpr (std::is_same_v<U, std::uint8_t>) return "uint8_t";
            else if constexpr (std::is_same_v<U, std::uint16_t>) return "uint16_t";
            else if constexpr (std::is_same_v<U, std::uint32_t>) return "uint32_t";
            else if constexpr (std::is_same_v<U, std::uint64_t>) return "uint64_t";
            else if constexpr (std::is_same_v<U, float>) return "float";
            else if constexpr (std::is_same_v<U, double>) return "double";
            else if constexpr (std::is_same_v<U, std::string>) return "std::string";
            else if constexpr (std::is_same_v<U, common::Decimal>) return "Decimal";
            else if constexpr (std::is_same_v<U, common::Date>) return "Date";
            else if constexpr (std::is_same_v<U, common::Time>) return "Time";
            else if constexpr (std::is_same_v<U, common::Timestamp>) return "Timestamp";
            else if constexpr (std::is_same_v<U, common::Uuid>) return "Uuid";
            else if constexpr (std::is_same_v<U, common::Json>) return "Json";
            else if constexpr (std::is_same_v<U, common::Blob>) return "Blob";
            else if constexpr (detail::IsOptional<U>::value)
                return "std::optional<" + TypeName<typename U::value_type>::name() + ">";
            else if constexpr (std::is_enum_v<U>) return "enum";
            else return "custom-type";
        }
    };

    // 源 Value 的 alternative 名（与 common::Value 的声明顺序一一对应）。
    inline const char *valueTypeName(const common::Value &v) {
        switch (v.index()) {
            case 0: return "NULL";
            case 1: return "bool";
            case 2: return "int64";
            case 3: return "uint64";
            case 4: return "double";
            case 5: return "Decimal";
            case 6: return "string";
            case 7: return "Date";
            case 8: return "Time";
            case 9: return "Timestamp";
            case 10: return "Uuid";
            case 11: return "Json";
            case 12: return "Blob";
            default: return "unknown";
        }
    }

    inline common::Status mapError(std::string msg) {
        return common::Status::error(common::ErrorCode::MappingError, std::move(msg));
    }

    inline common::Status typeError(const std::string &target, const common::Value &v) {
        return mapError("cannot convert " + std::string(valueTypeName(v)) + " to " + target);
    }

    // ---- 解析辅助（仅 Lossy 路径使用，要求整串可解析，不接受前缀）----
    inline bool tryParseIntegral(const std::string &s, std::int64_t &out) {
        if (s.empty()) return false;
        try {
            std::size_t pos = 0;
            const long long v = std::stoll(s, &pos);
            if (pos != s.size()) return false;
            out = static_cast<std::int64_t>(v);
            return true;
        } catch (...) { return false; }
    }

    inline bool tryParseReal(const std::string &s, double &out) {
        if (s.empty()) return false;
        try {
            std::size_t pos = 0;
            const double v = std::stod(s, &pos);
            if (pos != s.size()) return false;
            out = v;
            return true;
        } catch (...) { return false; }
    }

    // 整型范围检查（跨符号安全，不做窄化比较）
    template<class Dst, class Src>
    constexpr bool fitsIn(const Src s) {
        static_assert(std::is_integral_v<Src> && std::is_integral_v<Dst>);
        if constexpr (std::is_unsigned_v<Dst> && std::is_unsigned_v<Src>)
            return static_cast<std::uint64_t>(s) <=
                   static_cast<std::uint64_t>(std::numeric_limits<Dst>::max());
        else if constexpr (std::is_unsigned_v<Dst> && std::is_signed_v<Src>)
            return s >= 0 && static_cast<std::uint64_t>(s) <=
                   static_cast<std::uint64_t>(std::numeric_limits<Dst>::max());
        else if constexpr (std::is_signed_v<Dst> && std::is_unsigned_v<Src>)
            return static_cast<std::uint64_t>(s) <=
                   static_cast<std::uint64_t>(std::numeric_limits<Dst>::max());
        else
            return static_cast<std::int64_t>(s) >=
                   static_cast<std::int64_t>(std::numeric_limits<Dst>::min()) &&
                   static_cast<std::int64_t>(s) <=
                   static_cast<std::int64_t>(std::numeric_limits<Dst>::max());
    }

    // =======================================================================
    // 值转换：ValueConverter<U>
    //
    // 内置实现覆盖 common::Value 的全部 alternative 与常见 C++ 目标类型；
    // 业务自定义类型（强类型 ID、第三方时间库…）通过**特化**接入，不改库代码。
    // =======================================================================
    template<class U, class Enable = void>
    struct ValueConverter {
        static common::Status fromValue(const common::Value &, U &, FieldFlags) {
            static_assert(detail::AlwaysFalse<U>::value,
                          "dbmw::mapping: 目标类型没有内置转换规则，"
                          "请特化 dbmw::mapping::ValueConverter<T>");
            return mapError("unsupported target type");
        }

        static common::Value toValue(const U &) {
            static_assert(detail::AlwaysFalse<U>::value,
                          "dbmw::mapping: 目标类型没有内置转换规则，"
                          "请特化 dbmw::mapping::ValueConverter<T>");
            return common::Value(nullptr);
        }
    };

    // ---- bool：只接受 bool（int64 0/1 属声明与列类型不符，严格报错）----
    template<>
    struct ValueConverter<bool, void> {
        static common::Status fromValue(const common::Value &v, bool &out, FieldFlags) {
            if (const auto p = std::get_if<bool>(&v)) {
                out = *p;
                return common::Status::OK();
            }
            return typeError("bool", v);
        }

        static common::Value toValue(const bool in) { return common::Value(in); }
    };

    // ---- 整型：int64/uint64 + 范围检查；Lossy 才接受 double / Decimal / string ----
    template<class U>
    struct ValueConverter<U, std::enable_if_t<std::is_integral_v<U> && !std::is_same_v<U, bool>> > {
        static common::Status fromValue(const common::Value &v, U &out, const FieldFlags flags) {
            const std::string target = TypeName<U>::name();
            if (const auto p = std::get_if<std::int64_t>(&v)) {
                if (!fitsIn<U>(*p))
                    return mapError("value " + std::to_string(*p) + " out of range for " + target);
                out = static_cast<U>(*p);
                return common::Status::OK();
            }
            if (const auto p = std::get_if<std::uint64_t>(&v)) {
                if (!fitsIn<U>(*p))
                    return mapError("value " + std::to_string(*p) + " out of range for " + target);
                out = static_cast<U>(*p);
                return common::Status::OK();
            }
            if (hasFlag(flags, FieldFlags::Lossy)) {
                if (const auto p = std::get_if<double>(&v)) {
                    if (!std::isfinite(*p)) return mapError("non-finite double for " + target);
                    const double t = *p < 0 ? std::ceil(*p) : std::floor(*p);
                    if (t < static_cast<double>(std::numeric_limits<std::int64_t>::min()) ||
                        t > static_cast<double>(std::numeric_limits<std::int64_t>::max()))
                        return mapError("double " + std::to_string(*p) + " out of range for " + target);
                    const auto iv = static_cast<std::int64_t>(t);
                    if (!fitsIn<U>(iv))
                        return mapError("value " + std::to_string(iv) + " out of range for " + target);
                    out = static_cast<U>(iv);
                    return common::Status::OK();
                }
                std::int64_t iv = 0;
                bool parsed = false;
                if (const auto p = std::get_if<common::Decimal>(&v)) parsed = tryParseIntegral(p->value, iv);
                else if (const auto p = std::get_if<std::string>(&v)) parsed = tryParseIntegral(*p, iv);
                if (parsed) {
                    if (!fitsIn<U>(iv))
                        return mapError("value " + std::to_string(iv) + " out of range for " + target);
                    out = static_cast<U>(iv);
                    return common::Status::OK();
                }
            }
            return typeError(target, v);
        }

        static common::Value toValue(const U in) {
            if constexpr (std::is_unsigned_v<U>) return common::Value(static_cast<std::uint64_t>(in));
            else return common::Value(static_cast<std::int64_t>(in));
        }
    };

    // ---- 浮点：只接受 double；Decimal 默认拒绝（防丢精度，与 Value 设计同源）----
    template<class U>
    struct ValueConverter<U, std::enable_if_t<std::is_floating_point_v<U> > > {
        static common::Status fromValue(const common::Value &v, U &out, const FieldFlags flags) {
            const std::string target = TypeName<U>::name();
            if (const auto p = std::get_if<double>(&v)) {
                if constexpr (!std::is_same_v<U, double>) {
                    if (*p > static_cast<double>(std::numeric_limits<U>::max()) ||
                        *p < static_cast<double>(std::numeric_limits<U>::lowest()))
                        return mapError("double " + std::to_string(*p) + " out of range for " + target);
                }
                out = static_cast<U>(*p);
                return common::Status::OK();
            }
            if (hasFlag(flags, FieldFlags::Lossy)) {
                if (const auto p = std::get_if<std::int64_t>(&v)) {
                    out = static_cast<U>(*p);
                    return common::Status::OK();
                }
                if (const auto p = std::get_if<std::uint64_t>(&v)) {
                    out = static_cast<U>(*p);
                    return common::Status::OK();
                }
                double d = 0;
                bool parsed = false;
                if (const auto p = std::get_if<common::Decimal>(&v)) parsed = tryParseReal(p->value, d);
                else if (const auto p = std::get_if<std::string>(&v)) parsed = tryParseReal(*p, d);
                if (parsed) {
                    out = static_cast<U>(d);
                    return common::Status::OK();
                }
            }
            return typeError(target, v);
        }

        static common::Value toValue(const U in) { return common::Value(static_cast<double>(in)); }
    };

    // ---- std::string：接受文本型强类型的文本形式；Blob 拒绝（二进制不是文本）----
    template<>
    struct ValueConverter<std::string, void> {
        static common::Status fromValue(const common::Value &v, std::string &out, const FieldFlags flags) {
            if (const auto p = std::get_if<std::string>(&v)) {
                out = *p;
                return common::Status::OK();
            }
            if (const auto p = std::get_if<common::Decimal>(&v)) {
                out = p->value;
                return common::Status::OK();
            }
            if (const auto p = std::get_if<common::Date>(&v)) {
                out = p->value;
                return common::Status::OK();
            }
            if (const auto p = std::get_if<common::Time>(&v)) {
                out = p->value;
                return common::Status::OK();
            }
            if (const auto p = std::get_if<common::Uuid>(&v)) {
                out = p->value;
                return common::Status::OK();
            }
            if (const auto p = std::get_if<common::Json>(&v)) {
                out = p->value;
                return common::Status::OK();
            }
            if (hasFlag(flags, FieldFlags::Lossy)) {
                if (std::holds_alternative<std::nullptr_t>(v)) return typeError("std::string", v);
                if (std::holds_alternative<common::Blob>(v)) return typeError("std::string", v);
                if (const auto p = std::get_if<common::Timestamp>(&v)) {
                    out = common::timestampToStringMs(*p);
                    return common::Status::OK();
                }
                out = common::valueToString(v);
                return common::Status::OK();
            }
            return typeError("std::string", v);
        }

        static common::Value toValue(const std::string &in) { return common::Value(in); }
    };

    // ---- 文本型强类型：精确匹配；Textual 时接受 string ----
    template<class Strong>
    struct StrongTextConverter {
        static common::Status fromValue(const common::Value &v, Strong &out, const FieldFlags flags) {
            if (const auto p = std::get_if<Strong>(&v)) {
                out = *p;
                return common::Status::OK();
            }
            if (hasFlag(flags, FieldFlags::Textual))
                if (const auto p = std::get_if<std::string>(&v)) {
                    out = Strong{*p};
                    return common::Status::OK();
                }
            return typeError(TypeName<Strong>::name(), v);
        }

        static common::Value toValue(const Strong &in) { return common::Value(in); }
    };

    template<>
    struct ValueConverter<common::Decimal, void> : StrongTextConverter<common::Decimal> {
    };

    template<>
    struct ValueConverter<common::Date, void> : StrongTextConverter<common::Date> {
    };

    template<>
    struct ValueConverter<common::Time, void> : StrongTextConverter<common::Time> {
    };

    template<>
    struct ValueConverter<common::Uuid, void> : StrongTextConverter<common::Uuid> {
    };

    template<>
    struct ValueConverter<common::Json, void> : StrongTextConverter<common::Json> {
    };

    // ---- Blob：只接受 Blob（文本与 Blob 互转需业务自行 base64）----
    template<>
    struct ValueConverter<common::Blob, void> {
        static common::Status fromValue(const common::Value &v, common::Blob &out, FieldFlags) {
            if (const auto p = std::get_if<common::Blob>(&v)) {
                out = *p;
                return common::Status::OK();
            }
            return typeError("Blob", v);
        }

        static common::Value toValue(const common::Blob &in) { return common::Value(in); }
    };

    // ---- Timestamp：精确匹配；Textual 时从 string/Date/Time 解析 ----
    template<>
    struct ValueConverter<common::Timestamp, void> {
        static common::Status fromValue(const common::Value &v, common::Timestamp &out, const FieldFlags flags) {
            if (const auto p = std::get_if<common::Timestamp>(&v)) {
                out = *p;
                return common::Status::OK();
            }
            if (hasFlag(flags, FieldFlags::Textual)) {
                std::string text;
                if (const auto p = std::get_if<std::string>(&v)) text = *p;
                else if (const auto p = std::get_if<common::Date>(&v)) text = p->value;
                else if (const auto p = std::get_if<common::Time>(&v)) text = p->value;
                if (!text.empty() && common::tryParseTimestamp(text, out)) return common::Status::OK();
            }
            return typeError("Timestamp", v);
        }

        static common::Value toValue(const common::Timestamp &in) { return common::Value(in); }
    };

    // ---- std::optional<U>：NULL → nullopt ----
    template<class U>
    struct ValueConverter<std::optional<U> > {
        static_assert(!detail::IsOptional<U>::value, "不支持嵌套 std::optional");

        static common::Status fromValue(const common::Value &v, std::optional<U> &out, const FieldFlags flags) {
            if (std::holds_alternative<std::nullptr_t>(v)) {
                out.reset();
                return common::Status::OK();
            }
            U tmp{};
            if (const auto s = ValueConverter<U>::fromValue(v, tmp, flags); !s.ok()) return s;
            out = std::move(tmp);
            return common::Status::OK();
        }

        static common::Value toValue(const std::optional<U> &in) {
            return in ? ValueConverter<U>::toValue(*in) : common::Value(nullptr);
        }
    };

    // ---- enum class：整型 → 底层类型 + 范围检查 ----
    template<class U>
    struct ValueConverter<U, std::enable_if_t<std::is_enum_v<U> > > {
        using Under = std::underlying_type_t<U>;

        static common::Status fromValue(const common::Value &v, U &out, FieldFlags) {
            Under u{};
            if (const auto s = ValueConverter<Under>::fromValue(v, u, FieldFlags::None); !s.ok()) return s;
            out = static_cast<U>(u);
            return common::Status::OK();
        }

        static common::Value toValue(const U in) {
            return ValueConverter<Under>::toValue(static_cast<Under>(in));
        }
    };

    // =======================================================================
    // 字段表与实体声明
    // =======================================================================
    template<class T>
    class Mapping {
    public:
        using Assign = std::function<common::Status(const common::Value &, T &)>;
        using Write = std::function<common::Value(const T &)>;

        struct Column {
            std::string name;
            std::string target; // 目标类型名（错误信息用）
            FieldFlags flags = FieldFlags::None;
            Assign assign;
            Write write;
        };

        template<class M>
        Mapping &field(M T::*ptr, std::string column, const FieldFlags flags = FieldFlags::None) {
            Column c;
            c.name = column;
            c.target = TypeName<M>::name();
            c.flags = flags;
            c.assign = [ptr, flags, target = c.target](const common::Value &v, T &out) -> common::Status {
                auto s = ValueConverter<M>::fromValue(v, out.*ptr, flags);
                if (!s.ok() && s.code != common::ErrorCode::MappingError)
                    return mapError("target " + target + ": " + s.message);
                return s;
            };
            c.write = [ptr](const T &in) -> common::Value { return ValueConverter<M>::toValue(in.*ptr); };
            columns_.push_back(std::move(c));
            return *this;
        }

        Mapping &extraColumns(const ExtraColumns p) {
            extra_ = p;
            return *this;
        }

        Mapping &missingColumns(const MissingColumns p) {
            missing_ = p;
            return *this;
        }

        [[nodiscard]] std::size_t size() const noexcept { return columns_.size(); }
        [[nodiscard]] const std::vector<Column> &columns() const noexcept { return columns_; }
        [[nodiscard]] ExtraColumns extraPolicy() const noexcept { return extra_; }
        [[nodiscard]] MissingColumns missingPolicy() const noexcept { return missing_; }

        [[nodiscard]] std::vector<std::string> columnNames(const WriteCols which = WriteCols::All) const {
            std::vector<std::string> out;
            for (const auto &c: columns_) {
                if (which == WriteCols::PrimaryKey && !hasFlag(c.flags, FieldFlags::PrimaryKey)) continue;
                if (which == WriteCols::Writable && !writable(c)) continue;
                out.push_back(c.name);
            }
            return out;
        }

        // 行 → 实体。任一步失败即返回，out 可能已被部分填充（调用方应丢弃）。
        [[nodiscard]] common::Status fromRow(const common::Row &row, T &out) const {
            for (const auto &c: columns_) {
                // 必须用 find：Row::at() 对缺失列返回静态 NULL，会掩盖"少查一列"。
                const auto it = row.data().find(c.name);
                if (it == row.data().end()) {
                    if (missing_ == MissingColumns::Error)
                        return mapError("column '" + c.name + "' not found in result set (target " + c.target + ")");
                    continue;  // 宽松：跳过失缺列，字段保持默认构造值
                }
                if (const auto s = c.assign(it->second, out); !s.ok())
                    return mapError("column '" + c.name + "': " + s.message);
            }
            if (extra_ == ExtraColumns::Error) {
                for (const auto &kv: row.data())
                    if (!isDeclaredByName(kv.first))
                        return mapError("undeclared column '" + kv.first + "' in result set");
            }
            return common::Status::OK();
        }

        static bool writable(const Column &c) {
            return !hasFlag(c.flags, FieldFlags::Generated) && !hasFlag(c.flags, FieldFlags::ReadOnly);
        }

        // 该列名是否已在字段表中声明（供 updateSql 的显式列版本校验）。
        [[nodiscard]] bool isDeclaredByName(const std::string &n) const {
            for (const auto &c: columns_) if (c.name == n) return true;
            return false;
        }

    private:
        std::vector<Column> columns_;
        ExtraColumns extra_ = ExtraColumns::Ignore;
        MissingColumns missing_ = MissingColumns::Ignore;
    };

    // 用户特化点：提供 static Mapping<T> describe()
    template<class T>
    struct RowMapper;

    namespace detail {
        template<class T, class = void>
        struct HasDescribe : std::false_type {
        };

        template<class T>
        struct HasDescribe<T, std::void_t<decltype(RowMapper<T>::describe())> > : std::true_type {
        };
    } // namespace detail

    // 字段表单例：一次构建、进程内复用（magic static，线程安全，无初始化顺序问题）。
    template<class T>
    const Mapping<T> &mappingFor() {
        static_assert(detail::HasDescribe<T>::value,
                      "dbmw::mapping: 请为实体特化 dbmw::mapping::RowMapper<T> "
                      "并提供 static Mapping<T> describe()");
        static const Mapping<T> m = RowMapper<T>::describe();
        return m;
    }

    // ---- 结果集 → 实体（失败时清空 out：不返回半成品，I8）----
    template<class T>
    common::Status fromRows(const common::ResultSet &rs, std::vector<T> &out) {
        const auto &m = mappingFor<T>();
        out.clear();
        out.reserve(rs.rowCount());
        for (const auto &row: rs.rows()) {
            T item{};
            if (const auto s = m.fromRow(row, item); !s.ok()) {
                out.clear();
                return s;
            }
            out.push_back(std::move(item));
        }
        return common::Status::OK();
    }

    template<class T>
    common::Status fromRow(const common::Row &row, T &out) {
        return mappingFor<T>().fromRow(row, out);
    }

    // ---- 写方向：实体 → 参数 ----
    template<class T>
    common::Params paramsOf(const T &entity, const WriteCols which = WriteCols::Writable) {
        const auto &m = mappingFor<T>();
        common::Params out;
        out.reserve(m.size());
        for (const auto &c: m.columns()) {
            if (which == WriteCols::PrimaryKey && !hasFlag(c.flags, FieldFlags::PrimaryKey)) continue;
            if (which == WriteCols::Writable && !Mapping<T>::writable(c)) continue;
            out.push_back(c.write(entity));
        }
        return out;
    }

    // UPDATE 的参数顺序必须与 updateSql 一致：先 SET 列（可写非主键），后主键列。
    template<class T>
    common::Params updateParamsOf(const T &entity) {
        const auto &m = mappingFor<T>();
        common::Params set;
        common::Params keys;
        for (const auto &c: m.columns()) {
            if (hasFlag(c.flags, FieldFlags::PrimaryKey)) keys.push_back(c.write(entity));
            else if (Mapping<T>::writable(c)) set.push_back(c.write(entity));
        }
        set.insert(set.end(), keys.begin(), keys.end());
        return set;
    }

    template<class T>
    common::ParamBatch batchOf(const std::vector<T> &entities, const WriteCols which = WriteCols::Writable) {
        common::ParamBatch out;
        out.reserve(entities.size());
        for (const auto &e: entities) out.push_back(paramsOf(e, which));
        return out;
    }

    // ---- 结构确定的 SQL 片段（只拼列名与占位符，不生成业务条件）----
    inline std::string joinIdentifiers(const std::vector<std::string> &cols) {
        std::string s;
        for (std::size_t i = 0; i < cols.size(); ++i) {
            if (i) s += ", ";
            s += common::quoteIdentifier(cols[i]);
        }
        return s;
    }

    inline std::string placeholders(const std::size_t n) {
        std::string s;
        for (std::size_t i = 0; i < n; ++i) {
            if (i) s += ", ";
            s += "?";
        }
        return s;
    }

    // "col1" = ?, "col2" = ?（SET 与 WHERE 共用同一形态）
    inline std::string buildAssignList(const std::vector<std::string> &cols) {
        std::string s;
        for (std::size_t i = 0; i < cols.size(); ++i) {
            if (i) s += ", ";
            s += common::quoteIdentifier(cols[i]) + " = ?";
        }
        return s;
    }

    template<class T>
    std::string insertSql(std::string table) {
        const auto cols = mappingFor<T>().columnNames(WriteCols::Writable);
        return "INSERT INTO " + common::quoteIdentifier(table) + " (" + joinIdentifiers(cols) +
               ") VALUES (" + placeholders(cols.size()) + ")";
    }

    // 主键为空或无 SET 列时返回空串（调用方必须据此报错，绝不生成无条件 UPDATE）。
    template<class T>
    std::string updateSql(std::string table) {
        const auto &m = mappingFor<T>();
        std::vector<std::string> setCols;
        std::vector<std::string> keyCols;
        for (const auto &c: m.columns()) {
            if (hasFlag(c.flags, FieldFlags::PrimaryKey)) keyCols.push_back(c.name);
            else if (Mapping<T>::writable(c)) setCols.push_back(c.name);
        }
        if (keyCols.empty() || setCols.empty()) return std::string();
        return "UPDATE " + common::quoteIdentifier(table) + " SET " + buildAssignList(setCols) +
               " WHERE " + buildAssignList(keyCols);
    }

    // 显式指定 SET / WHERE 列（必须是已声明的列，否则返回空串）
    template<class T>
    std::string updateSql(std::string table, const std::vector<std::string> &setCols,
                          const std::vector<std::string> &whereCols) {
        const auto &m = mappingFor<T>();
        if (setCols.empty() || whereCols.empty()) return std::string();
        for (const auto &n: setCols)
            if (!m.isDeclaredByName(n)) return std::string();
        for (const auto &n: whereCols)
            if (!m.isDeclaredByName(n)) return std::string();
        return "UPDATE " + common::quoteIdentifier(table) + " SET " + buildAssignList(setCols) +
               " WHERE " + buildAssignList(whereCols);
    }

    // 生成键回填：优先按 Generated 列的列名取；MySQL 只有合成列 insert_id 时，
    // 回退 lastInsertId() 并写入第一个能接受整型的 Generated 列。
    template<class T>
    common::Status applyGeneratedKeys(const common::GeneratedKeys &keys, T &entity) {
        if (keys.empty()) return common::Status::OK();
        const auto &m = mappingFor<T>();
        const auto &rows = keys.rows.rows();
        const common::Row *row = rows.empty() ? nullptr : &rows.front();
        std::size_t filled = 0;
        for (const auto &c: m.columns()) {
            if (!hasFlag(c.flags, FieldFlags::Generated) || row == nullptr) continue;
            const auto it = row->data().find(c.name);
            if (it == row->data().end()) continue;
            if (const auto s = c.assign(it->second, entity); !s.ok())
                return mapError("generated key column '" + c.name + "': " + s.message);
            ++filled;
        }
        if (filled == 0) {
            const auto id = keys.lastInsertId();
            if (id != 0) {
                const common::Value v(static_cast<std::int64_t>(id));
                for (const auto &c: m.columns()) {
                    if (!hasFlag(c.flags, FieldFlags::Generated)) continue;
                    if (c.assign(v, entity).ok()) break;
                }
            }
        }
        return common::Status::OK();
    }
} // namespace dbmw::mapping

// ===========================================================================
// 同步门面（自由函数，不修改 dbmw.h）
// ===========================================================================
namespace dbmw {
    template<class T>
    EntityResult<T> queryAs(const std::string &sql) {
        common::ResultSet rs;
        EntityResult<T> r;
        r.status = DBMW::query(sql, rs);
        if (r.status.ok()) r.status = mapping::fromRows<T>(rs, r.items);
        return r;
    }

    template<class T>
    EntityResult<T> queryAs(const std::string &sql, const common::Params &params) {
        common::ResultSet rs;
        EntityResult<T> r;
        r.status = DBMW::query(sql, params, rs);
        if (r.status.ok()) r.status = mapping::fromRows<T>(rs, r.items);
        return r;
    }

    template<class T>
    EntityResult<T> queryAs(const std::string &dataSource, const std::string &sql,
                            const common::Params &params) {
        common::ResultSet rs;
        EntityResult<T> r;
        r.status = DBMW::query(dataSource, sql, params, rs);
        if (r.status.ok()) r.status = mapping::fromRows<T>(rs, r.items);
        return r;
    }

    template<class T>
    EntityResult<T> queryAs(core::Session &s, const std::string &sql) {
        common::ResultSet rs;
        EntityResult<T> r;
        r.status = s.query(sql, rs);
        if (r.status.ok()) r.status = mapping::fromRows<T>(rs, r.items);
        return r;
    }

    template<class T>
    EntityResult<T> queryAs(core::Session &s, const std::string &sql,
                            const common::Params &params) {
        common::ResultSet rs;
        EntityResult<T> r;
        r.status = s.query(sql, params, rs);
        if (r.status.ok()) r.status = mapping::fromRows<T>(rs, r.items);
        return r;
    }

    namespace detail {
        // 多行不是"取第一行"，而是声明与 SQL 不符 —— 报错（严格模式）。
        template<class T>
        EntityOne<T> queryOneAsImpl(EntityResult<T> &&r) {
            EntityOne<T> o;
            o.status = r.status;
            if (!r.status.ok()) return o;
            if (r.items.size() > 1) {
                o.status = mapping::mapError("queryOneAs: expected at most 1 row, got " +
                                             std::to_string(r.items.size()));
                return o;
            }
            if (!r.items.empty()) o.value = std::move(r.items.front());
            return o;
        }
    } // namespace detail

    template<class T>
    EntityOne<T> queryOneAs(const std::string &sql) {
        return detail::queryOneAsImpl<T>(queryAs<T>(sql));
    }

    template<class T>
    EntityOne<T> queryOneAs(const std::string &sql, const common::Params &params) {
        return detail::queryOneAsImpl<T>(queryAs<T>(sql, params));
    }

    template<class T>
    EntityOne<T> queryOneAs(const std::string &dataSource, const std::string &sql,
                            const common::Params &params) {
        return detail::queryOneAsImpl<T>(queryAs<T>(dataSource, sql, params));
    }

    template<class T>
    EntityOne<T> queryOneAs(core::Session &s, const std::string &sql) {
        return detail::queryOneAsImpl<T>(queryAs<T>(s, sql));
    }

    template<class T>
    EntityOne<T> queryOneAs(core::Session &s, const std::string &sql,
                            const common::Params &params) {
        return detail::queryOneAsImpl<T>(queryAs<T>(s, sql, params));
    }

    // 流式：逐行映射后回调；返回 false 提前终止。映射失败 → 立即停并以
    // MappingError 收尾（rows 记录已成功映射的行数）。
    template<class T>
    common::Status queryEachAs(const std::string &sql, const common::Params &params,
                               const std::function<bool(T &&)> &cb, std::uint64_t &rows) {
        rows = 0;
        std::uint64_t counted = 0;
        common::Status mapErr;
        common::RowCallback raw = [&cb, &counted, &mapErr](const common::Row &row) -> bool {
            T item{};
            if (const auto s = mapping::fromRow<T>(row, item); !s.ok()) {
                mapErr = s;
                return false;
            }
            ++counted;
            return cb(std::move(item));
        };
        std::uint64_t driverRows = 0;
        const auto st = DBMW::queryEach(sql, params, raw, driverRows);
        rows = counted;
        if (st.ok() && !mapErr.ok()) return mapErr;
        return st;
    }

    template<class T>
    common::Status queryEachAs(const std::string &dataSource, const std::string &sql,
                               const common::Params &params,
                               const std::function<bool(T &&)> &cb, std::uint64_t &rows) {
        rows = 0;
        std::uint64_t counted = 0;
        common::Status mapErr;
        common::RowCallback raw = [&cb, &counted, &mapErr](const common::Row &row) -> bool {
            T item{};
            if (const auto s = mapping::fromRow<T>(row, item); !s.ok()) {
                mapErr = s;
                return false;
            }
            ++counted;
            return cb(std::move(item));
        };
        std::uint64_t driverRows = 0;
        const auto st = DBMW::queryEach(dataSource, sql, params, raw, driverRows);
        rows = counted;
        if (st.ok() && !mapErr.ok()) return mapErr;
        return st;
    }

    template<class T>
    common::Status queryEachAs(core::Session &s, const std::string &sql, const common::Params &params,
                               const std::function<bool(T &&)> &cb, std::uint64_t &rows) {
        rows = 0;
        std::uint64_t counted = 0;
        common::Status mapErr;
        common::RowCallback raw = [&cb, &counted, &mapErr](const common::Row &row) -> bool {
            T item{};
            if (const auto s = mapping::fromRow<T>(row, item); !s.ok()) {
                mapErr = s;
                return false;
            }
            ++counted;
            return cb(std::move(item));
        };
        std::uint64_t driverRows = 0;
        const auto st = s.queryEach(sql, params, raw, driverRows);
        rows = counted;
        if (st.ok() && !mapErr.ok()) return mapErr;
        return st;
    }

    // 游标：fetch 之后映射（游标本身不受影响，可继续 fetch）
    template<class T>
    EntityResult<T> fetchAs(core::ICursor &c, const std::size_t n) {
        common::ResultSet rs;
        EntityResult<T> r;
        r.status = c.fetch(n, rs);
        if (r.status.ok()) r.status = mapping::fromRows<T>(rs, r.items);
        return r;
    }

    // 生成键结果集 → 实体（PG/ODBC 的 RETURNING / OUTPUT）
    template<class T>
    EntityResult<T> keysAs(const common::GeneratedKeys &keys) {
        EntityResult<T> r;
        r.status = mapping::fromRows<T>(keys.rows, r.items);
        return r;
    }

    // ---- 写方向 ----

    // insertAs 走 withSession：生成键只在 Session::execute 上可取（门面 execute
    // 没有生成键重载），而 insert_id / RETURNING 语义本来就要求同一条连接。
    template<class T>
    WriteResult<T> insertAs(std::string table, T &entity) {
        WriteResult<T> r;
        const std::string sql = mapping::insertSql<T>(table);
        const common::Params p = mapping::paramsOf(entity);
        r.status = DBMW::withSession([&](core::Session &s) { return s.execute(sql, p, r.affected, r.keys); });
        if (r.status.ok()) r.status = mapping::applyGeneratedKeys(r.keys, entity);
        return r;
    }

    template<class T>
    WriteResult<T> insertAs(core::Session &s, std::string table, T &entity) {
        WriteResult<T> r;
        const std::string sql = mapping::insertSql<T>(table);
        const common::Params p = mapping::paramsOf(entity);
        r.status = s.execute(sql, p, r.affected, r.keys);
        if (r.status.ok()) r.status = mapping::applyGeneratedKeys(r.keys, entity);
        return r;
    }

    template<class T>
    WriteResult<T> updateAs(std::string table, const T &entity) {
        WriteResult<T> r;
        const std::string sql = mapping::updateSql<T>(table);
        if (sql.empty()) {
            r.status = mapping::mapError(
                "updateAs: 实体未声明 PrimaryKey 列或没有可更新列，拒绝生成 UPDATE");
            return r;
        }
        r.status = DBMW::execute(sql, mapping::updateParamsOf(entity), r.affected);
        return r;
    }

    template<class T>
    WriteResult<T> updateAs(core::Session &s, std::string table, const T &entity) {
        WriteResult<T> r;
        const std::string sql = mapping::updateSql<T>(table);
        if (sql.empty()) {
            r.status = mapping::mapError(
                "updateAs: 实体未声明 PrimaryKey 列或没有可更新列，拒绝生成 UPDATE");
            return r;
        }
        r.status = s.execute(sql, mapping::updateParamsOf(entity), r.affected);
        return r;
    }

    template<class T>
    BatchWriteResult<T> insertBatchAs(std::string table, const std::vector<T> &entities) {
        BatchWriteResult<T> r;
        r.status = DBMW::executeBatch(mapping::insertSql<T>(table),
                                      mapping::batchOf(entities), r.batch);
        return r;
    }

    template<class T>
    BatchWriteResult<T> insertBatchAs(core::Session &s, std::string table,
                                      const std::vector<T> &entities) {
        BatchWriteResult<T> r;
        r.status = s.executeBatch(mapping::insertSql<T>(table),
                                  mapping::batchOf(entities), r.batch);
        return r;
    }
} // namespace dbmw

// ===========================================================================
// 异步门面：回调 / future（协程在下一段，需 DBMW_ENABLE_ASYNC_CORO）
//
// 全部是既有异步 API 的薄封装：拿到原始结果 → 映射 → 交给用户回调。
// 映射发生在**完成投递线程**（默认主执行器 worker；注入 asio 时为
// io_context 线程），因此映射必须保持轻量（I7）。
// ===========================================================================
namespace dbmw::async {
    template<class T>
    using EntityQueryCallback = std::function<void(EntityResult<T> &&)>;

    template<class T>
    Handle queryAs(const std::string &sql, EntityQueryCallback<T> cb, Options opts = {}) {
        return query(sql, QueryCallback([cb](QueryResult &&r) {
            EntityResult<T> out;
            out.status = r.status;
            if (r.status.ok()) out.status = mapping::fromRows<T>(r.rows, out.items);
            cb(std::move(out));
        }), opts);
    }

    template<class T>
    Handle queryAs(const std::string &sql, const common::Params &params,
                   EntityQueryCallback<T> cb, Options opts = {}) {
        return query(sql, params, QueryCallback([cb](QueryResult &&r) {
            EntityResult<T> out;
            out.status = r.status;
            if (r.status.ok()) out.status = mapping::fromRows<T>(r.rows, out.items);
            cb(std::move(out));
        }), opts);
    }

    template<class T>
    Handle queryAs(const std::string &dataSource, const std::string &sql,
                   const common::Params &params, EntityQueryCallback<T> cb, Options opts = {}) {
        return query(dataSource, sql, params, QueryCallback([cb](QueryResult &&r) {
            EntityResult<T> out;
            out.status = r.status;
            if (r.status.ok()) out.status = mapping::fromRows<T>(r.rows, out.items);
            cb(std::move(out));
        }), opts);
    }

    // 流式：rowCb 在 worker 上逐行执行（映射在其中完成）；done 经完成调度器投递。
    // rows 统计的是**成功映射**的行数。
    template<class T>
    Handle queryEachAs(const std::string &sql, const common::Params &params,
                       const std::function<bool(T &&)> &rowCb, EachCallback done,
                       Options opts = {}) {
        struct EachState {
            std::uint64_t rows = 0;
            common::Status err;
        };
        auto state = std::make_shared<EachState>();
        common::RowCallback raw = [rowCb, state](const common::Row &row) -> bool {
            T item{};
            if (const auto s = mapping::fromRow<T>(row, item); !s.ok()) {
                state->err = s;
                return false;
            }
            ++state->rows;
            return rowCb(std::move(item));
        };
        return queryEach(sql, params, raw, [done, state](EachResult &&r) {
            EachResult out;
            out.rows = state->rows;
            out.status = r.status.ok() ? (state->err.ok() ? common::Status::OK() : state->err) : r.status;
            done(std::move(out));
        }, opts);
    }

    // future 式：无 Handle、无取消；用 promise 桥接，不额外占用线程。
    template<class T>
    std::future<EntityResult<T> > queryAs(const std::string &sql, const common::Params &params) {
        auto p = std::make_shared<std::promise<EntityResult<T> > >();
        auto fut = p->get_future();
        queryAs<T>(sql, params, [p](EntityResult<T> &&r) { p->set_value(std::move(r)); });
        return fut;
    }
} // namespace dbmw::async

#if defined(DBMW_ENABLE_ASYNC_CORO)

namespace dbmw::async {
    // 协程形态：复用 task.h 的 OpAwaiter（回调 → 协程帧 → resume）。
    //
    // !! GCC 13 已知缺陷（PR109227 系）：co_await 实参里出现**非平凡的花括号
    // 临时**会 ICE。调用方请把参数先具名构造再传入：
    //     common::Params p{Value(1)};
    //     auto r = co_await queryAsAsync<User>("SELECT ...", p);
    template<class T>
    Task<EntityResult<T> > queryAsAsync(std::string sql, common::Params params = {},
                                        Options opts = {}) {
        using Awaiter = detail::OpAwaiter<EntityResult<T> >;
        co_return co_await Awaiter(
            [sql = std::move(sql), params = std::move(params), opts]
    (typename Awaiter::Callback cb) mutable {
                queryAs<T>(sql, params, [cb](EntityResult<T> &&r) { cb(std::move(r)); }, opts);
            });
    }
} // namespace dbmw::async

#endif // DBMW_ENABLE_ASYNC_CORO

#endif // DBMW_MAPPING_H
