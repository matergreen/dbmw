#ifndef DBMW_ASYNC_DBMW_ASYNC_H
#define DBMW_ASYNC_DBMW_ASYNC_H

#include "dbmw/async/async_types.h"
#include "dbmw/async/executor.h"
#include "dbmw/config/datasource_config.h"
#include "dbmw/core/database_manager.h"

#include <future>
#include <string>

namespace dbmw::async {

    // ============ 回调式（热路径）。末位参数是回调 → 返回 Handle ============
    //
    // 重载消歧：回调式以末位的 *Callback 参数区分（std::function 无法从
    // Params/Options 隐式转换），future 式不带回调——同名重载无歧义，
    // 且 API 面与同步门面 DBMW 保持镜像对称。
    //
    // dataSource 参数语义与 DBMW 同名方法一致：空串/缺省 = 默认数据源，
    // 不存在 → 回调收到 ConfigError，返回的 Handle 状态为 Done。

    Handle query(const std::string &sql, QueryCallback cb, Options opts = {});
    Handle query(const std::string &dataSource, const std::string &sql,
                 QueryCallback cb, Options opts = {});
    Handle query(const std::string &sql, const common::Params &params,
                 QueryCallback cb, Options opts = {});
    Handle query(const std::string &dataSource, const std::string &sql,
                 const common::Params &params, QueryCallback cb, Options opts = {});

    Handle execute(const std::string &sql, ExecCallback cb, Options opts = {});
    Handle execute(const std::string &dataSource, const std::string &sql,
                   ExecCallback cb, Options opts = {});
    Handle execute(const std::string &sql, const common::Params &params,
                   ExecCallback cb, Options opts = {});
    Handle execute(const std::string &dataSource, const std::string &sql,
                   const common::Params &params, ExecCallback cb, Options opts = {});

    // 生成键形态（MySQL 自增 / PG·ODBC 的 RETURNING·OUTPUT，语义同同步版；
    // 要求生成键的写不入写缓冲——补发时拿不到键）。
    Handle execute(const std::string &sql, const common::Params &params,
                   ExecKeysCallback cb, Options opts = {});
    Handle execute(const std::string &dataSource, const std::string &sql,
                   const common::Params &params, ExecKeysCallback cb, Options opts = {});

    // queryEach：rowCb 在 worker 上逐行回调（须短小、线程安全）；
    // done 在完成后经完成调度器投递。已交付过行时绝不自动重放。
    Handle queryEach(const std::string &sql, const common::Params &params,
                     const common::RowCallback &rowCb, EachCallback done, Options opts = {});
    Handle queryEach(const std::string &dataSource, const std::string &sql,
                     const common::Params &params,
                     const common::RowCallback &rowCb, EachCallback done, Options opts = {});

    Handle executeBatch(const std::string &sql, const common::ParamBatch &batch,
                        BatchCallback cb, Options opts = {});
    Handle executeBatch(const std::string &dataSource, const std::string &sql,
                        const common::ParamBatch &batch, BatchCallback cb, Options opts = {});

    // 事务 / 会话：fn 跑在 worker 上，内部整段复用同步实现
    // （闸门、看门狗、cancel、回滚、read-after-write 语义原样保留）。
    //
    // !! 约束：fn 内部禁止调用 dbmw::async::* ——回调已在 worker 上执行，
    // 嵌套异步会在池偏小时互相等连接造成活锁；fn 内直接用同步 Session 方法。
    Handle transaction(const core::SessionFn &fn, OpCallback cb, Options opts = {});
    Handle transaction(const std::string &dataSource, const core::SessionFn &fn,
                       OpCallback cb, Options opts = {});
    Handle transaction(const common::TransactionOptions &txOpts, const core::SessionFn &fn,
                       OpCallback cb, Options opts = {});
    Handle transaction(const std::string &dataSource, const common::TransactionOptions &txOpts,
                       const core::SessionFn &fn, OpCallback cb, Options opts = {});

    Handle withSession(const core::SessionFn &fn, OpCallback cb, Options opts = {});
    Handle withSession(const std::string &dataSource, const core::SessionFn &fn,
                       OpCallback cb, Options opts = {});

    // ============ future 式（便利形态）。无回调参数 → 返回 future ============
    //
    // future 式无 Handle、无取消；需要取消请用回调式。
    // future 析构不阻塞（不参与 shutdown 排水；未取值即放弃是合法用法）。

    std::future<QueryResult> query(const std::string &sql);
    std::future<QueryResult> query(const std::string &sql, const common::Params &params);
    std::future<QueryResult> query(const std::string &dataSource, const std::string &sql,
                                   const common::Params &params);
    std::future<ExecResult> execute(const std::string &sql);
    std::future<ExecResult> execute(const std::string &sql, const common::Params &params);
    std::future<ExecResult> execute(const std::string &dataSource, const std::string &sql,
                                    const common::Params &params);
    // 生成键的 future 形态。注意：不能叫 execute —— 回调式靠末位的
    // ExecKeysCallback 消歧，future 式没有该参数，(dataSource, sql, params)
    // 与上面的 ExecResult 版完全同参（C++ 禁止仅按返回类型重载），
    // 故独立命名为 executeKeys（对设计 §5.2 的一处必要偏离）。
    std::future<ExecKeysResult> executeKeys(const std::string &sql,
                                            const common::Params &params);
    std::future<ExecKeysResult> executeKeys(const std::string &dataSource,
                                            const std::string &sql,
                                            const common::Params &params);
    std::future<EachResult> queryEach(const std::string &sql, const common::Params &params,
                                      const common::RowCallback &rowCb);
    std::future<BatchResult> executeBatch(const std::string &sql,
                                          const common::ParamBatch &batch);
    std::future<OpResult> transaction(const core::SessionFn &fn);
    std::future<OpResult> transaction(const std::string &dataSource,
                                      const common::TransactionOptions &txOpts,
                                      const core::SessionFn &fn);

    // ============ 执行器管理 ============

    // 注入自定义执行器（如 asio 适配器）。必须在任何异步调用之前设置；
    // 传入后 dbmw 不再自建线程池，且执行器必须活到 DBMW::shutdown() 完成。
    void setExecutor(std::shared_ptr<IExecutor> ex);

    // 完成回调/协程恢复的调度器；默认复用主执行器。
    void setCompletionExecutor(std::shared_ptr<IExecutor> ex);

    // 执行器运行时统计（队列深度/活跃线程/拒绝数/在途操作数）。
    ExecutorStats stats();

    namespace detail {
        // ---- 引擎内部接线（由 DBMW::init/reload/shutdown 调用，用户勿用）----
        //
        // 放在 dbmw.cpp 而非 DatabaseManager::init 中接线：
        // core 模块不反向依赖 async（与 ConnectionPool::AsyncIo 的设计一致）。

        // init/reload：按配置创建内置执行器（已存在则保留，热加载不换线程池）。
        void initEngine(const config::AsyncConfig &cfg);

        // shutdown §9.3 的第 1–4 步：拒绝新操作 → 等在途归零 →
        // 停完成调度器 → 停主执行器。池的关闭仍在 DatabaseManager::shutdown。
        void drainAndStop(std::chrono::milliseconds grace);

        // 在途异步操作数（观测/测试用）。
        std::size_t inFlight();
    } // namespace detail

} // namespace dbmw::async

#endif // DBMW_ASYNC_DBMW_ASYNC_H
