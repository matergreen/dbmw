#ifndef DBMW_COMMON_SQL_ANALYZE_H
#define DBMW_COMMON_SQL_ANALYZE_H

#include <cstdint>
#include <string>
#include <vector>

namespace dbmw::common::sql {
    // SQL 语句类型（best-effort 启发式分类，非完整解析器）。
    enum class StatementKind {
        Unknown,
        Select,
        Insert,
        Update,
        Delete,
        // DDL：CREATE / ALTER / DROP / TRUNCATE / RENAME / GRANT / REVOKE 等。
        Ddl,
        // 其它写操作（REPLACE / MERGE / CALL 等）或无法判定。
        Other
    };

    // 结构模板：折叠字符串/数值字面量为 '?'、压缩空白、去掉普通注释，
    // 同时保留标识符、优化器 Hint 与 PostgreSQL dollar-quoted 代码块。
    // 同一业务 SQL 的不同取值会得到相同模板——慢 SQL 聚合、审计指纹、
    // 查询缓存 key 都应以它为准，保证口径一致。
    std::string structuralTemplate(const std::string &sql);

    // 仅依据结构模板计算的指纹（不含数据源名/操作类型），用于审计黑白名单匹配。
    std::uint64_t fingerprintTemplate(const std::string &sql);

    // 首条语句的动词分类。多语句只取第一条。
    StatementKind classifyStatement(const std::string &sql);

    // 是否为写操作（Insert/Update/Delete/Ddl）。
    bool isWrite(StatementKind kind);

    // best-effort：UPDATE/DELETE 之后是否存在独立 WHERE 关键字。
    // 仅扫描结构模板，跳过字符串/注释/标识符，避免误判。
    bool hasWhereClause(const std::string &sql);

    // best-effort：SELECT 之后是否存在独立 LIMIT 关键字。
    bool hasLimitClause(const std::string &sql);
} // namespace dbmw::common::sql

#endif // DBMW_COMMON_SQL_ANALYZE_H
