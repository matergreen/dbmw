#include "dbmw/common/sql_analyze.h"

#include <cctype>
#include <cstring>


namespace dbmw::common::sql {
    namespace {
        // 与 observer.cpp 中历史实现保持一致：折叠字符串/数值字面量、保留标识符。
        // 任何改动都应同步 observer 侧的使用方——这里已抽取为唯一来源。
        std::string buildStructural(const std::string &sql) {
            std::string out;
            out.reserve(sql.size());
            const std::size_t n = sql.size();
            const auto isIdent = [](unsigned char c) {
                return std::isalnum(c) || c == '_';
            };
            bool pendingSpace = false;
            const auto flushSpace = [&] {
                if (pendingSpace && !out.empty() && out.back() != ' ') out.push_back(' ');
                pendingSpace = false;
            };
            std::size_t i = 0;
            while (i < n) {
                const auto c = static_cast<unsigned char>(sql[i]);

                if (std::isspace(c)) {
                    pendingSpace = !out.empty();
                    ++i;
                    continue;
                }

                // 普通注释不影响 SQL 结构；MySQL 版本注释和优化器 Hint 会影响执行，保留。
                if (c == '-' && i + 1 < n && sql[i + 1] == '-') {
                    while (i < n && sql[i] != '\n') ++i;
                    pendingSpace = !out.empty();
                    continue;
                }
                if (c == '/' && i + 1 < n && sql[i + 1] == '*') {
                    const bool semantic = i + 2 < n &&
                        (sql[i + 2] == '!' || sql[i + 2] == '+');
                    const auto close = sql.find("*/", i + 2);
                    const auto end = close == std::string::npos ? n : close + 2;
                    if (semantic) {
                        flushSpace();
                        out.append(sql, i, end - i);
                    } else {
                        pendingSpace = !out.empty();
                    }
                    i = end;
                    continue;
                }

                // PostgreSQL $$...$$ / $tag$...$tag$。
                if (c == '$') {
                    std::size_t tagEnd = i + 1;
                    while (tagEnd < n &&
                           (std::isalnum(static_cast<unsigned char>(sql[tagEnd])) ||
                            sql[tagEnd] == '_')) ++tagEnd;
                    const bool validTag = tagEnd < n && sql[tagEnd] == '$' &&
                        (tagEnd == i + 1 ||
                         !std::isdigit(static_cast<unsigned char>(sql[i + 1])));
                    if (validTag) {
                        const std::string delimiter = sql.substr(i, tagEnd - i + 1);
                        const auto close = sql.find(delimiter, tagEnd + 1);
                        if (close != std::string::npos) {
                            flushSpace();
                            const auto end = close + delimiter.size();
                            std::uint64_t bodyHash = 1469598103934665603ULL;
                            for (auto p = tagEnd + 1; p < close; ++p) {
                                bodyHash ^= static_cast<unsigned char>(sql[p]);
                                bodyHash *= 1099511628211ULL;
                            }
                            out += delimiter + "<body_hash:" +
                                   std::to_string(bodyHash) + ">" + delimiter;
                            i = end;
                            continue;
                        }
                    }
                }

                if (c == '\'') {
                    flushSpace();
                    out += '?';
                    ++i;
                    while (i < n) {
                        if (sql[i] == '\\' && i + 1 < n) {
                            i += 2;
                            continue;
                        }
                        if (sql[i] == '\'') {
                            if (i + 1 < n && sql[i + 1] == '\'') { i += 2; continue; }
                            ++i;
                            break;
                        }
                        ++i;
                    }
                    continue;
                }

                if (c == '"' || c == '`') {
                    flushSpace();
                    const char quote = static_cast<char>(c);
                    out.push_back(quote);
                    ++i;
                    while (i < n) {
                        out.push_back(sql[i]);
                        if (sql[i] == '\\' && i + 1 < n) {
                            out.push_back(sql[++i]);
                        } else if (sql[i] == quote) {
                            if (i + 1 < n && sql[i + 1] == quote)
                                out.push_back(sql[++i]);
                            else {
                                ++i;
                                break;
                            }
                        }
                        ++i;
                    }
                    continue;
                }

                const bool boundary = i == 0 ||
                    !isIdent(static_cast<unsigned char>(sql[i - 1]));
                const bool signedNumber = (c == '+' || c == '-') && i + 1 < n &&
                    (std::isdigit(static_cast<unsigned char>(sql[i + 1])) ||
                     (sql[i + 1] == '.' && i + 2 < n &&
                      std::isdigit(static_cast<unsigned char>(sql[i + 2]))));
                const bool plainNumber = std::isdigit(c) ||
                    (c == '.' && i + 1 < n &&
                     std::isdigit(static_cast<unsigned char>(sql[i + 1])));
                if (boundary && (plainNumber || signedNumber)) {
                    flushSpace();
                    out += '?';
                    if (signedNumber) ++i;
                    if (i + 1 < n && sql[i] == '0' &&
                        (sql[i + 1] == 'x' || sql[i + 1] == 'X')) {
                        i += 2;
                        while (i < n && (std::isxdigit(static_cast<unsigned char>(sql[i])) ||
                                         sql[i] == '_')) ++i;
                        continue;
                    }
                    if (i + 1 < n && sql[i] == '0' &&
                        (sql[i + 1] == 'b' || sql[i + 1] == 'B')) {
                        i += 2;
                        while (i < n && (sql[i] == '0' || sql[i] == '1' || sql[i] == '_')) ++i;
                        continue;
                    }
                    while (i < n && (std::isdigit(static_cast<unsigned char>(sql[i])) ||
                                     sql[i] == '_')) ++i;
                    if (i < n && sql[i] == '.') {
                        ++i;
                        while (i < n && (std::isdigit(static_cast<unsigned char>(sql[i])) ||
                                         sql[i] == '_')) ++i;
                    }
                    if (i < n && (sql[i] == 'e' || sql[i] == 'E')) {
                        std::size_t exponent = i + 1;
                        if (exponent < n && (sql[exponent] == '+' || sql[exponent] == '-'))
                            ++exponent;
                        const auto digits = exponent;
                        while (exponent < n &&
                               (std::isdigit(static_cast<unsigned char>(sql[exponent])) ||
                                sql[exponent] == '_')) ++exponent;
                        if (exponent > digits) i = exponent;
                    }
                    continue;
                }

                flushSpace();
                out += static_cast<char>(c);
                ++i;
            }
            return out;
        }

        // 把字符串字面量、注释、引号标识符区域替换为空格（保持长度），
        // 便于在剩下的"裸结构"上做关键字扫描，避免字符串里的 WHERE/LIMIT 误判。
        std::string maskLiteralRegions(const std::string &sql) {
            std::string out = sql;
            const std::size_t n = out.size();
            std::size_t i = 0;
            while (i < n) {
                const auto c = static_cast<unsigned char>(out[i]);
                if (c == '-' && i + 1 < n && out[i + 1] == '-') {
                    while (i < n && out[i] != '\n') out[i++] = ' ';
                    continue;
                }
                if (c == '/' && i + 1 < n && out[i + 1] == '*') {
                    out[i] = ' '; out[i + 1] = ' ';
                    const auto close = out.find("*/", i + 2);
                    const auto end = close == std::string::npos ? n : close + 2;
                    for (std::size_t p = i + 2; p < end; ++p) out[p] = ' ';
                    i = end;
                    continue;
                }
                if (c == '\'' || c == '"' || c == '`') {
                    const char quote = static_cast<char>(c);
                    out[i++] = ' ';
                    while (i < n) {
                        if (out[i] == '\\' && i + 1 < n) { out[i] = ' '; out[++i] = ' '; continue; }
                        if (out[i] == quote) {
                            out[i] = ' ';
                            if (i + 1 < n && out[i + 1] == quote) out[++i] = ' ';
                            else { ++i; break; }
                        }
                        out[i++] = ' ';
                    }
                    continue;
                }
                if (c == '$') {
                    std::size_t tagEnd = i + 1;
                    while (tagEnd < n &&
                           (std::isalnum(static_cast<unsigned char>(out[tagEnd])) ||
                            out[tagEnd] == '_')) ++tagEnd;
                    const bool validTag = tagEnd < n && out[tagEnd] == '$' &&
                        (tagEnd == i + 1 ||
                         !std::isdigit(static_cast<unsigned char>(out[i + 1])));
                    if (validTag) {
                        const std::string delimiter = out.substr(i, tagEnd - i + 1);
                        const auto close = out.find(delimiter, tagEnd + 1);
                        if (close != std::string::npos) {
                            for (std::size_t p = i; p < close + delimiter.size(); ++p) out[p] = ' ';
                            i = close + delimiter.size();
                            continue;
                        }
                    }
                }
                ++i;
            }
            return out;
        }

        // 在裸结构文本中查找独立的 SQL 关键字（大小写不敏感，词边界匹配）。
        bool containsKeyword(const std::string &masked, const char *kw) {
            const std::size_t klen = std::char_traits<char>::length(kw);
            const std::size_t n = masked.size();
            for (std::size_t i = 0; i + klen <= n; ++i) {
                if (std::strncmp(masked.c_str() + i, kw, klen) != 0) continue;
                const bool leftOk = (i == 0) ||
                    !std::isalnum(static_cast<unsigned char>(masked[i - 1])) && masked[i - 1] != '_';
                const bool rightOk = (i + klen == n) ||
                    !std::isalnum(static_cast<unsigned char>(masked[i + klen])) && masked[i + klen] != '_';
                if (leftOk && rightOk) return true;
            }
            return false;
        }

        std::string upperFirstVerb(const std::string &masked) {
            // 跳过前导空白，取第一个空白/括号/分号前的 token。
            std::size_t i = 0;
            const std::size_t n = masked.size();
            while (i < n && std::isspace(static_cast<unsigned char>(masked[i]))) ++i;
            std::string verb;
            while (i < n) {
                const auto c = masked[i];
                if (std::isspace(static_cast<unsigned char>(c)) || c == '(' || c == ';' || c == '{')
                    break;
                verb.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
                ++i;
            }
            return verb;
        }
    }

    std::string structuralTemplate(const std::string &sql) {
        return buildStructural(sql);
    }

    std::uint64_t fingerprintTemplate(const std::string &sql) {
        const std::string tpl = buildStructural(sql);
        std::uint64_t hash = 1469598103934665603ULL;
        for (const unsigned char c: tpl) {
            hash ^= c;
            hash *= 1099511628211ULL;
        }
        return hash;
    }

    StatementKind classifyStatement(const std::string &sql) {
        const std::string verb = upperFirstVerb(maskLiteralRegions(sql));
        if (verb == "SELECT" || verb == "WITH" || verb == "SHOW" || verb == "EXPLAIN" ||
            verb == "DESC" || verb == "DESCRIBE")
            return StatementKind::Select;
        if (verb == "INSERT" || verb == "REPLACE") return StatementKind::Insert;
        if (verb == "UPDATE") return StatementKind::Update;
        if (verb == "DELETE" || verb == "TRUNCATE") return StatementKind::Delete;
        if (verb == "CREATE" || verb == "ALTER" || verb == "DROP" || verb == "RENAME" ||
            verb == "GRANT" || verb == "REVOKE" || verb == "COMMENT")
            return StatementKind::Ddl;
        return StatementKind::Other;
    }

    bool isWrite(StatementKind kind) {
        return kind == StatementKind::Insert || kind == StatementKind::Update ||
            kind == StatementKind::Delete || kind == StatementKind::Ddl;
    }

    bool hasWhereClause(const std::string &sql) {
        return containsKeyword(maskLiteralRegions(sql), "WHERE");
    }

    bool hasLimitClause(const std::string &sql) {
        return containsKeyword(maskLiteralRegions(sql), "LIMIT");
    }
} // namespace dbmw::common::sql
