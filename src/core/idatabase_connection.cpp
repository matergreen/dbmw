#include "dbmw/core/idatabase_connection.h"

#include <cctype>
#include <string>


namespace dbmw::core {
    common::Status IDatabaseConnection::cancel() {
        return common::Status::error(common::ErrorCode::NotSupported,
                                     "driver does not support query cancellation");
    }

    common::Status IDatabaseConnection::begin(const common::TransactionOptions &options) {
        if (options.isolation != common::IsolationLevel::Default || options.readOnly) {
            return common::Status::error(common::ErrorCode::NotSupported,
                                         "driver does not support transaction options");
        }
        return begin();
    }

    common::Status IDatabaseConnection::openCursor(const std::string &sql,
                                                   const common::Params &params,
                                                   const CursorOptions &opts,
                                                   std::unique_ptr<ICursor> &out) {
        (void) sql;
        (void) params;
        (void) opts;
        out.reset();
        return common::Status::error(common::ErrorCode::NotSupported,
                                     "driver does not support cursors");
    }

    common::Status IDatabaseConnection::queryEach(
        const std::string &sql, const common::Params &params,
        const common::RowCallback &callback, std::uint64_t &rows) {
        rows = 0;
        common::ResultSet result;
        const auto status = params.empty() ? query(sql, result) : query(sql, params, result);
        if (!status.ok()) return status;
        for (const auto &row: result.rows()) {
            ++rows;
            if (callback && !callback(row)) break;
        }
        return common::Status::OK();
    }

    common::Status IDatabaseConnection::executeBatch(
        const std::string &sql, const common::ParamBatch &batch,
        common::BatchResult &out) {
        out.clear();
        out.affected.reserve(batch.size());
        if (batch.empty()) return common::Status::OK();

        // 原子性：整批要么全成、要么全滚。
        //
        // 逐条直发的话，第 N 组失败会留下前 N-1 组的写入，而调用方拿到的
        // BatchResult 又看不出到底落了几组——这类部分写入是最难排查的数据损坏。
        // 这里与 PostgreSQL 驱动自建 pqxx::work 的行为对齐，让三个驱动语义一致。
        //
        // 调用方已在事务里时直接沿用，失败交由调用方决定回滚范围。
        const bool ownTx = !inTransaction();
        if (ownTx) {
            if (const auto st = begin(); !st.ok()) return st;
        }

        common::Status status = common::Status::OK();
        for (const auto &params: batch) {
            std::int64_t affected = 0;
            status = execute(sql, params, affected);
            if (!status.ok()) break;
            out.affected.push_back(affected);
        }

        if (!ownTx) return status;

        if (!status.ok()) {
            // 已回滚，部分影响行数不再有参考价值，清空避免误导。
            // 回滚失败不覆盖原始错误——批次本身的失败原因才是调用方要看的。
            (void) rollback();
            out.clear();
            return status;
        }
        if (const auto st = commit(); !st.ok()) {
            out.clear();
            return st;
        }
        return status;
    }

    common::Status IDatabaseConnection::savepoint(const std::string &name) {
        (void) name;
        return common::Status::error(common::ErrorCode::NotSupported,
                                     "driver does not support savepoints");
    }

    common::Status IDatabaseConnection::releaseSavepoint(const std::string &name) {
        (void) name;
        return common::Status::error(common::ErrorCode::NotSupported,
                                     "driver does not support savepoints");
    }

    common::Status IDatabaseConnection::rollbackToSavepoint(const std::string &name) {
        (void) name;
        return common::Status::error(common::ErrorCode::NotSupported,
                                     "driver does not support savepoints");
    }

    common::Status IDatabaseConnection::query(const std::string &sql,
                                             const common::Params &params,
                                             common::ResultSet &out) {
        if (!allowsLiteralInterpolation()) {
            return common::Status::error(
                common::ErrorCode::NotSupported,
                "driver does not implement native parameter binding");
        }
        std::string built;
        if (const auto st = buildSql(sql, params, built); !st.ok()) return st;
        return query(built, out);
    }

    common::Status IDatabaseConnection::execute(const std::string &sql,
                                                const common::Params &params,
                                                std::int64_t &affected) {
        if (!allowsLiteralInterpolation()) {
            affected = 0;
            return common::Status::error(
                common::ErrorCode::NotSupported,
                "driver does not implement native parameter binding");
        }
        std::string built;
        if (const auto st = buildSql(sql, params, built); !st.ok()) return st;
        return execute(built, affected);
    }

    std::string IDatabaseConnection::escapeLiteral(const common::Value &v) const {
        return common::escapeLiteralGeneric(v);
    }

    common::Status IDatabaseConnection::renderSqlForLogging(
        const std::string &sql, const common::Params &params,
        const common::SqlRenderOptions &options, std::string &out) const {
        std::size_t found = 0;
        out = replacePlaceholders(sql, [&](const std::size_t index) {
            if (index >= params.size()) return std::string("?");
            const auto &value = params[index];
            if (const auto *text = std::get_if<std::string>(&value)) {
                if (!options.includeStringValues) return std::string("'<redacted>'");
                std::string limited = *text;
                if (limited.size() > options.maxParamLength) {
                    limited.resize(options.maxParamLength);
                    limited += "...[truncated]";
                }
                return escapeLiteral(limited);
            }
            if (const auto *blob = std::get_if<common::Blob>(&value)) {
                if (!options.includeBlobValues) {
                    return std::string("'<blob:") + std::to_string(blob->size()) + " bytes>'";
                }
                common::Blob limited = *blob;
                const bool wasTruncated = limited.size() > options.maxParamLength;
                if (wasTruncated)
                    limited.resize(options.maxParamLength);
                auto rendered = escapeLiteral(limited);
                if (wasTruncated) {
                    rendered += "/* truncated, original_bytes=" +
                        std::to_string(blob->size()) + " */";
                }
                return rendered;
            }
            return escapeLiteral(value);
        }, found);
        if (found != params.size()) {
            out.clear();
            return common::Status::error(
                common::ErrorCode::QueryError,
                "SQL placeholder count (" + std::to_string(found) +
                ") does not match params count (" + std::to_string(params.size()) + ")");
        }
        if (out.size() > options.maxSqlLength) {
            const auto originalLength = out.size();
            out.resize(options.maxSqlLength);
            out += "...[truncated, original_length=" + std::to_string(originalLength) + "]";
        }
        return common::Status::OK();
    }

    std::string IDatabaseConnection::replacePlaceholders(const std::string &sql,
                                                         const PlaceholderVisitor &visitor,
                                                         std::size_t &found)
    {
        std::string out;
        out.reserve(sql.size());

        found = 0;
        const size_t n = sql.size();

        for (size_t i = 0; i < n; ++i) {
            const char c = sql[i];

            // PostgreSQL dollar-quoted 字符串（$$...$$ / $tag$...$tag$）。
            // 其中的问号是正文，不是参数占位符。
            if (c == '$') {
                size_t tagEnd = i + 1;
                while (tagEnd < n &&
                       (std::isalnum(static_cast<unsigned char>(sql[tagEnd])) ||
                        sql[tagEnd] == '_')) ++tagEnd;
                const bool validTag = tagEnd < n && sql[tagEnd] == '$' &&
                    (tagEnd == i + 1 ||
                     !std::isdigit(static_cast<unsigned char>(sql[i + 1])));
                if (validTag) {
                    const std::string delimiter = sql.substr(i, tagEnd - i + 1);
                    const size_t close = sql.find(delimiter, tagEnd + 1);
                    if (close != std::string::npos) {
                        out.append(sql, i, close + delimiter.size() - i);
                        i = close + delimiter.size() - 1;
                        continue;
                    }
                }
            }

            // 字符串字面量 / 标识符：原样搬运，内部的 '?' 不参与替换。
            // 连续两个同种引号表示转义而不是结束。
            if (c == '\'' || c == '"' || c == '`') {
                out.push_back(c);
                ++i;
                while (i < n) {
                    out.push_back(sql[i]);
                    if (sql[i] == '\\' && i + 1 < n) {
                        ++i;
                        out.push_back(sql[i]);
                        ++i;
                        continue;
                    }
                    if (sql[i] == c) {
                        if (i + 1 < n && sql[i + 1] == c) {
                            ++i;
                            out.push_back(sql[i]);
                        } else {
                            break;
                        }
                    }
                    ++i;
                }
                continue;
            }

            // 行注释
            if (c == '-' && i + 1 < n && sql[i + 1] == '-') {
                while (i < n && sql[i] != '\n') {
                    out.push_back(sql[i]);
                    ++i;
                }
                if (i < n) out.push_back(sql[i]); // 换行符
                continue;
            }

            // 块注释
            if (c == '/' && i + 1 < n && sql[i + 1] == '*') {
                out.push_back(c);
                ++i;
                while (i < n) {
                    out.push_back(sql[i]);
                    if (sql[i] == '*' && i + 1 < n && sql[i + 1] == '/') {
                        ++i;
                        out.push_back(sql[i]);
                        break;
                    }
                    ++i;
                }
                continue;
            }

            if (c == '?') {
                out += visitor(found++);
                continue;
            }

            out.push_back(c);
        }
        return out;
    }

    common::Status IDatabaseConnection::buildSql(const std::string &sql,
                                                 const common::Params &params,
                                                 std::string &out) const {
        std::size_t used = 0;
        out = replacePlaceholders(sql, [&](std::size_t i) -> std::string {
            // 占位符多于参数时原样保留，由下面的数量校验统一报错。
            if (i >= params.size()) return "?";
            return escapeLiteral(params[i]);
        }, used);

        if (used != params.size()) {
            return common::Status::error(
                common::ErrorCode::QueryError,
                "parameter mismatch: supplied " + std::to_string(params.size())
                + " parameter(s) but SQL has " + std::to_string(used)
                + " '?' placeholder(s)");
        }
        return common::Status::OK();
    }

    // -----------------------------------------------------------------------
    // 预编译语句：基类默认不支持，驱动按需覆盖。
    // -----------------------------------------------------------------------

    common::Status IDatabaseConnection::prepare(const std::string &sql,
                                                const common::Params &typesSample,
                                                PreparedStatementHandle &out) {
        (void) sql;
        (void) typesSample;
        out = PreparedStatementHandle{};
        return common::Status::error(common::ErrorCode::NotSupported,
                                     "driver does not support prepared statements");
    }

    common::Status IDatabaseConnection::executePrepared(const PreparedStatementHandle &h,
                                                        const common::Params &params,
                                                        common::ResultSet &out) {
        (void) h;
        (void) params;
        out.clear();
        return common::Status::error(common::ErrorCode::NotSupported,
                                     "driver does not support prepared statements");
    }

    common::Status IDatabaseConnection::executePrepared(const PreparedStatementHandle &h,
                                                        const common::Params &params,
                                                        std::int64_t &affected) {
        (void) h;
        (void) params;
        affected = 0;
        return common::Status::error(common::ErrorCode::NotSupported,
                                     "driver does not support prepared statements");
    }

    void IDatabaseConnection::closeAllPrepared() {
        // 默认没有句柄可释放：预编译缓存是驱动内部实现，未实现的驱动无需关心。
    }

    // -----------------------------------------------------------------------
    // 生成键：基类默认委托无键 execute，out 留空。
    //
    // 这样"拿不到生成键"是一个安静的事实（empty()），而不是一个 NotSupported 错误：
    // 老驱动与不支持 RETURNING 的语句因此不会因为多传了一个 out 参数而失败。
    // -----------------------------------------------------------------------

    common::Status IDatabaseConnection::execute(const std::string &sql, std::int64_t &affected,
                                                common::GeneratedKeys &out) {
        out = common::GeneratedKeys{};
        return execute(sql, affected);
    }

    common::Status IDatabaseConnection::execute(const std::string &sql,
                                                const common::Params &params,
                                                std::int64_t &affected,
                                                common::GeneratedKeys &out) {
        out = common::GeneratedKeys{};
        return execute(sql, params, affected);
    }

    // -----------------------------------------------------------------------
    // 大参数流式：基类默认走"读入 Blob 再委托既有路径"的降级。
    //
    // 这是有意为之——libpq 协议不支持参数的 data-at-execution，PostgreSQL 驱动
    // 必须继承该默认；而所有老驱动也因此自动获得了流式 API，不必改一行代码。
    // -----------------------------------------------------------------------

    common::Status IDatabaseConnection::query(const std::string &sql,
                                              const common::StreamParams &params,
                                              common::ResultSet &out) {
        common::Params plain;
        if (const auto st = common::streamParamsToParams(params, plain); !st.ok()) return st;
        return query(sql, plain, out);
    }

    common::Status IDatabaseConnection::execute(const std::string &sql,
                                                const common::StreamParams &params,
                                                std::int64_t &affected,
                                                common::GeneratedKeys &out) {
        common::Params plain;
        if (const auto st = common::streamParamsToParams(params, plain); !st.ok()) return st;
        return execute(sql, plain, affected, out);
    }

    common::Status IDatabaseConnection::executeBatch(const std::string &sql,
                                                     const common::StreamParamBatch &batch,
                                                     common::BatchResult &out) {
        common::ParamBatch plain;
        plain.reserve(batch.size());
        for (const auto &group: batch) {
            common::Params params;
            if (const auto st = common::streamParamsToParams(group, params); !st.ok()) return st;
            plain.push_back(std::move(params));
        }
        return executeBatch(sql, plain, out);
    }
} // namespace dbmw::core
