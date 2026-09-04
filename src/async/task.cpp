// 协程层实现：每个 *Async 工厂是一个协程，函数体只有一条
// co_await（detail::OpAwaiter 桥）+ co_return。真正的执行管线全部
// 复用 M2 的回调式门面——设计 §5.3："回调 API 的薄封装，零额外语义"。
//
// 本 TU 仅在 -DDBMW_ENABLE_ASYNC_CORO=ON 时参与构建，且被 CMake 单独
// 提标到 C++20；库内其余 TU 仍按 C++17 编译。
#include "dbmw/async/task.h"

namespace dbmw::async {

    Task<QueryResult> queryAsync(std::string sql, common::Params params, Options opts) {
        co_return co_await detail::OpAwaiter<QueryResult>{
            [sql = std::move(sql), params = std::move(params), opts](
                detail::OpAwaiter<QueryResult>::Callback cb) {
                // 参数按值捕获进协程帧（lambda 存于帧内 OpAwaiter），
                // 挂起期间调用方栈上的东西一概不引用。
                query(sql, params, QueryCallback(std::move(cb)), opts);
            }};
    }

    Task<QueryResult> queryAsync(std::string dataSource, std::string sql,
                                 common::Params params, Options opts) {
        co_return co_await detail::OpAwaiter<QueryResult>{
            [dataSource = std::move(dataSource), sql = std::move(sql),
             params = std::move(params), opts](
                detail::OpAwaiter<QueryResult>::Callback cb) {
                query(dataSource, sql, params, QueryCallback(std::move(cb)), opts);
            }};
    }

    Task<ExecResult> executeAsync(std::string sql, common::Params params, Options opts) {
        co_return co_await detail::OpAwaiter<ExecResult>{
            [sql = std::move(sql), params = std::move(params), opts](
                detail::OpAwaiter<ExecResult>::Callback cb) {
                execute(sql, params, ExecCallback(std::move(cb)), opts);
            }};
    }

    Task<ExecKeysResult> executeAsync(std::string dataSource, std::string sql,
                                      common::Params params, Options opts) {
        co_return co_await detail::OpAwaiter<ExecKeysResult>{
            [dataSource = std::move(dataSource), sql = std::move(sql),
             params = std::move(params), opts](
                detail::OpAwaiter<ExecKeysResult>::Callback cb) {
                execute(dataSource, sql, params, ExecKeysCallback(std::move(cb)), opts);
            }};
    }

    Task<ExecKeysResult> executeKeysAsync(std::string sql, common::Params params,
                                          Options opts) {
        co_return co_await detail::OpAwaiter<ExecKeysResult>{
            [sql = std::move(sql), params = std::move(params), opts](
                detail::OpAwaiter<ExecKeysResult>::Callback cb) {
                execute(sql, params, ExecKeysCallback(std::move(cb)), opts);
            }};
    }

    Task<BatchResult> executeBatchAsync(std::string sql, common::ParamBatch batch,
                                        Options opts) {
        co_return co_await detail::OpAwaiter<BatchResult>{
            [sql = std::move(sql), batch = std::move(batch), opts](
                detail::OpAwaiter<BatchResult>::Callback cb) {
                executeBatch(sql, batch, BatchCallback(std::move(cb)), opts);
            }};
    }

    Task<OpResult> transactionAsync(common::TransactionOptions txOpts,
                                    core::SessionFn fn) {
        co_return co_await detail::OpAwaiter<OpResult>{
            [txOpts, fn = std::move(fn)](detail::OpAwaiter<OpResult>::Callback cb) {
                // fn 跑在 worker 上，内部用同步 Session 方法（与回调式
                // transaction 同一约束：fn 内禁止再调 dbmw::async::*）。
                transaction(txOpts, fn, OpCallback(std::move(cb)));
            }};
    }

} // namespace dbmw::async
