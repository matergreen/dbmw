#include "dbmw/core/sql_auditor.h"

#include "dbmw/common/sql_analyze.h"
#include "dbmw/common/logger.h"

#include <memory>
#include <mutex>


namespace dbmw::core {
    std::atomic<bool> SqlAuditor::enabled_{false};
    std::mutex SqlAuditor::mtx_;
    std::shared_ptr<const SqlAuditor::Policy> SqlAuditor::policy_;
    std::atomic<std::uint64_t> SqlAuditor::checked_{0};
    std::atomic<std::uint64_t> SqlAuditor::warned_{0};
    std::atomic<std::uint64_t> SqlAuditor::blocked_{0};

    namespace {
        // 统一的判定出口：命中策略后按 action 决定是拦截还是仅告警。
        //
        // 把"记日志 + 计数 + 按 action 分流"收在一处，是因为这三件事必须同步：
        // 少记一次数就会让灰度期的"这条策略会拦掉多少流量"失真，
        // 而那正是决定何时从 warn 切到 block 的唯一依据。
        common::Status verdict(const bool block, const bool logIt,
                              const char *reason, const std::string &sql,
                              std::atomic<std::uint64_t> &warned,
                              std::atomic<std::uint64_t> &blocked) {
            if (logIt) {
                DBMW_LOG_WARN(std::string("sql audit ") + (block ? "blocked" : "warn") + " ("
                              + reason + "): " + sql);
            }
            if (block) {
                blocked.fetch_add(1, std::memory_order_relaxed);
                return common::Status::error(common::ErrorCode::SqlBlocked, reason);
            }
            warned.fetch_add(1, std::memory_order_relaxed);
            return common::Status::OK();
        }
    }

    void SqlAuditor::configure(const config::SqlAuditConfig &cfg) {
        auto policy = std::make_shared<Policy>();
        policy->block = cfg.action == "block";
        policy->log_blocked = cfg.log_blocked;
        policy->block_no_where_dml = cfg.block_no_where_dml;
        policy->require_limit_select = cfg.require_limit_select;
        policy->enforce_read_only = cfg.enforce_read_only;
        policy->blacklist.insert(cfg.blacklist_fingerprints.begin(),
                                 cfg.blacklist_fingerprints.end());
        policy->whitelist.insert(cfg.whitelist_fingerprints.begin(),
                                 cfg.whitelist_fingerprints.end());
        policy->needsFingerprint = !policy->blacklist.empty() || !policy->whitelist.empty();
        policy->needsKind = policy->enforce_read_only || policy->block_no_where_dml ||
                            policy->require_limit_select;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            policy_ = std::move(policy);
        }
        // enabled_ 最后置位：check() 见到 true 时 policy_ 必定已就位。
        enabled_.store(cfg.enabled, std::memory_order_release);
    }

    common::Status SqlAuditor::check(const std::string &sql, const common::OperationType type,
                                     const bool readOnly) {
        // 无锁快速失败。审计是逐条语句调用的，关闭时不该有任何锁开销。
        if (!enabled_.load(std::memory_order_acquire)) return common::Status::OK();

        std::shared_ptr<const Policy> policy;
        {
            // 只拷一个 shared_ptr，不深拷配置里的两个 vector。
            std::lock_guard<std::mutex> lk(mtx_);
            policy = policy_;
        }
        if (!policy) return common::Status::OK();

        checked_.fetch_add(1, std::memory_order_relaxed);
        using namespace common::sql;

        // 轻量分析器不尝试跨语句追踪副作用。多语句若只按第一个动词
        // 审计，"SELECT 1; DELETE ..." 就能直接绕过只读与 DML 护栏。
        if (hasMultipleStatements(sql)) {
            return verdict(policy->block, policy->log_blocked,
                           "multiple SQL statements are not allowed", sql,
                           warned_, blocked_);
        }

        // 指纹要完整扫一遍 SQL，只有真配了名单才算——
        // 绝大多数人只开 no-where / limit 这类规则，不该替他们付这笔开销。
        if (policy->needsFingerprint) {
            const std::uint64_t fp = fingerprintTemplate(sql);

            // 白名单优先：非空时"仅放行名单内"，其余一律拦截（允许列表模式）。
            if (!policy->whitelist.empty() &&
                policy->whitelist.find(fp) == policy->whitelist.end()) {
                // 允许列表是安全边界，不受用于其它启发式规则的
                // action=warn 影响；否则“仅放行名单内”会在灰度模式下失效。
                return verdict(true, policy->log_blocked,
                               "SQL not in audit whitelist", sql, warned_, blocked_);
            }
            // 黑名单：命中即判定。
            if (policy->blacklist.find(fp) != policy->blacklist.end()) {
                return verdict(true, policy->log_blocked,
                               "SQL in audit blacklist", sql, warned_, blocked_);
            }
        }

        if (!policy->needsKind) {
            (void) type;
            return common::Status::OK();
        }

        const StatementKind kind = classifyStatement(sql);

        if (policy->enforce_read_only && readOnly && isWrite(kind)) {
            return verdict(policy->block, policy->log_blocked,
                           "write on read-only datasource", sql, warned_, blocked_);
        }

        if (policy->block_no_where_dml &&
            (kind == StatementKind::Update || kind == StatementKind::Delete) &&
            !hasWhereClause(sql)) {
            return verdict(policy->block, policy->log_blocked,
                           "UPDATE/DELETE without WHERE clause", sql, warned_, blocked_);
        }

        // 游标（type==Select）豁免 require_limit_select：游标的意义就在于分批消费大结果集，
        // 要求它带 LIMIT 等于废掉"全量游标扫描"这一正当用法。其余读路径（Query/Stream）照常要求。
        const bool isCursor = (type == common::OperationType::Select);
        if (policy->require_limit_select && !isCursor && kind == StatementKind::Select &&
            !hasLimitClause(sql)) {
            return verdict(policy->block, policy->log_blocked,
                           "SELECT without LIMIT clause", sql, warned_, blocked_);
        }

        // 除上述游标豁免外，type 不参与判定：分类完全由 SQL 文本决定，比调用方声明的
        // 操作类型可靠（同一条 DELETE 既可能走 execute 也可能被塞进 query）。type 仅用于
        // 表达"消费方式"这类 SQL 文本无法体现的差异（游标的分批读取即其一）。
        return common::Status::OK();
    }

    SqlAuditor::Stats SqlAuditor::stats() {
        Stats out;
        out.checked = checked_.load(std::memory_order_relaxed);
        out.warned = warned_.load(std::memory_order_relaxed);
        out.blocked = blocked_.load(std::memory_order_relaxed);
        return out;
    }
} // namespace dbmw::core
