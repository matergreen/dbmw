#ifndef DBMW_ASYNC_ASYNC_TYPES_H
#define DBMW_ASYNC_ASYNC_TYPES_H

#include "dbmw/common/types.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace dbmw::async {

    // ---- 结果载体（I2 不变量：值语义，整体 move，绝不跨线程共享引用）----

    struct QueryResult {
        common::Status status;
        common::ResultSet rows;
    };

    struct ExecResult {
        common::Status status;
        std::int64_t affected = 0;
    };

    struct ExecKeysResult {
        common::Status status;
        std::int64_t affected = 0;
        common::GeneratedKeys keys;
    };

    struct EachResult {
        common::Status status;
        std::uint64_t rows = 0;
    };

    struct BatchResult {
        common::Status status;
        common::BatchResult batch;
    };

    // transaction / withSession 的结果：细节都在 Status 里。
    struct OpResult {
        common::Status status;
    };

    // ---- 回调类型 ----

    using QueryCallback = std::function<void(QueryResult &&)>;
    using ExecCallback = std::function<void(ExecResult &&)>;
    using ExecKeysCallback = std::function<void(ExecKeysResult &&)>;
    using EachCallback = std::function<void(EachResult &&)>;
    using BatchCallback = std::function<void(BatchResult &&)>;
    using OpCallback = std::function<void(OpResult &&)>;

    // ---- 每操作可选项 ----

    struct Options {
        // 借出连接的等待上限：<0 = 池默认；0 = 不等待（池满立即失败）。
        std::chrono::milliseconds borrowTimeout{-1};
        // 语句整体期限：0 = 不限（可用全局 async.statement_timeout_ms 兜底）。
        std::chrono::milliseconds timeout{0};
    };

    namespace detail {
        struct OpState;     // 操作上下文，见 async_engine.cpp（Handle 的实现载体）
        class AsyncEngine;  // 管线引擎（Handle 的唯一构造者）
    }

    // 取消句柄：轻量、可丢弃（不 cancel 直接析构完全合法）。
    //
    // 线程安全：cancel() 可从任意线程调用；与完成回调并发时最多一方生效。
    class Handle {
    public:
        Handle() = default;

        [[nodiscard]] bool valid() const { return s_ != nullptr; }

        enum class State { Queued, Running, Done };

        [[nodiscard]] State state() const;

        // 请求取消：
        //   Queued  → 标记取消，操作启动时直接以 Cancelled 完成，不碰池；
        //   Running → 尽力转发 Session::cancel()（驱动不支持时语句照跑完，
        //             完成状态仍标 Cancelled——如实反映"用户已放弃"）；
        //   Done    → 返回错误（无事可做）。
        common::Status cancel() const;

    private:
        friend class detail::AsyncEngine; // 注意限定名：引擎在 detail 子命名空间

        explicit Handle(std::shared_ptr<detail::OpState> s) : s_(std::move(s)) {}

        std::shared_ptr<detail::OpState> s_;
    };

} // namespace dbmw::async

#endif // DBMW_ASYNC_ASYNC_TYPES_H
