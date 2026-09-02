#ifndef DBMW_CORE_SQL_AUDITOR_H
#define DBMW_CORE_SQL_AUDITOR_H

#include "dbmw/common/types.h"
#include "dbmw/common/observer.h"
#include "dbmw/config/datasource_config.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>


namespace dbmw::core {
    // SQL 审计与拦截：在执行前对 SQL 做轻量静态分析并施加策略。
    //
    // 全局单例（与 Observability 同形态）：策略对所有数据源生效，黑名单/白名单
    // 按 SQL 结构指纹匹配。返回 Ok 表示放行；返回 SqlBlocked 表示被拦截。
    class SqlAuditor {
    public:
        SqlAuditor() = delete;

        static void configure(const config::SqlAuditConfig &cfg);

        // 返回 Ok 放行；否则 status.code == SqlBlocked，message 说明原因。
        // readOnly 表示该数据源为只读（来自所属 group 的 read_only 标志）。
        static common::Status check(const std::string &sql, common::OperationType type,
                                   bool readOnly = false);

        // 累计放行/告警/拦截计数（观测用）。
        //
        // 灰度期靠 action="warn" 先跑一段，而"这条策略到底会拦掉多少流量"
        // 只能从计数看出来——没有它就没法判断什么时候可以切到 block。
        struct Stats {
            std::uint64_t checked = 0;
            std::uint64_t warned = 0;  // 命中策略但 action=warn，已放行
            std::uint64_t blocked = 0; // 命中策略且被拦截
        };

        static Stats stats();

    private:
        // 预编译的策略快照。
        //
        // 直接存 SqlAuditConfig 会让每条语句都在锁里深拷两个 vector；
        // 指纹名单用线性扫描也会随名单变长而变慢。这里一次性编译成
        // 哈希集合 + 布尔量，check() 只需拷一个 shared_ptr。
        struct Policy {
            bool block = false; // action == "block"
            bool log_blocked = true;
            bool block_no_where_dml = false;
            bool require_limit_select = false;
            bool enforce_read_only = false;
            std::unordered_set<std::uint64_t> blacklist;
            std::unordered_set<std::uint64_t> whitelist;
            // 是否需要算 SQL 结构指纹（只有配了名单才需要）。
            bool needsFingerprint = false;
            // 是否需要做语句分类（只有配了这三项策略才需要）。
            bool needsKind = false;
        };

        // 快速失败开关：关闭时 check() 连锁都不抢。
        //
        // 审计是逐条语句调用的，默认关闭的功能不该在热路径上留下一把全局锁。
        static std::atomic<bool> enabled_;
        static std::mutex mtx_;
        static std::shared_ptr<const Policy> policy_;
        static std::atomic<std::uint64_t> checked_;
        static std::atomic<std::uint64_t> warned_;
        static std::atomic<std::uint64_t> blocked_;
    };
} // namespace dbmw::core

#endif // DBMW_CORE_SQL_AUDITOR_H
