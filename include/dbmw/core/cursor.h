#ifndef DBMW_CORE_CURSOR_H
#define DBMW_CORE_CURSOR_H

#include "dbmw/common/types.h"

#include <cstddef>
#include <chrono>
#include <cstdint>
#include <memory>

namespace dbmw::core {

    // 游标抽象：可持有、可多次 fetch、可显式关闭。
    //
    // 与一次性 query() 不同，游标把一条物理连接（PG 上连带事务）从“借→用→还”
    // 变成“借→钉住→取 N 次→显式关→还”。上层用 Cursor（RAII）持有它时连接不归还，
    // 直到 close() 或析构。各驱动提供自己的 ICursor 实现（服务端游标或非缓冲结果集）。
    class ICursor {
    public:
        virtual ~ICursor() = default;

        // 预取最多 n 行，追加写进 out（不清空 out，多次 fetch 可累积同一结果集）。
        // n == 0 时由实现按默认 batch_size 处理。返回成功时 out.rowCount() 可能 < n：
        // 表示已到结果集末尾（可配合 hasNext() 判定 EOF）。
        virtual common::Status fetch(std::size_t n, common::ResultSet &out) = 0;

        // 取单行；无更多行时 ok=false，状态为 CursorClosed（非错误，正常 EOF）。
        virtual common::Status fetchRow(common::Row &out, bool &ok) = 0;

        // 显式关闭：释放服务端游标（PG 还会提交/回滚其事务）、释放驱动资源。
        // 幂等：重复 close 返回 OK。析构时会自动调用。
        virtual common::Status close() = 0;

        [[nodiscard]] virtual bool isOpen() const = 0;
        // 是否还有更多行（仅前进游标为“近似”：取决于驱动是否暴露 EOF/行数）。
        [[nodiscard]] virtual bool hasNext() const = 0;
        [[nodiscard]] virtual std::uint64_t rowsFetched() const = 0;
    };

    // 游标打开选项。
    struct CursorOptions {
        std::size_t batch_size = 256;            // 每次 fetch 预取行数（驱动可 clamp）
        bool scrollable = false;                // 仅 ODBC 生效；其余驱动忽略或报 NotSupported
        std::chrono::milliseconds timeout{0};   // 单条 FETCH 期限；0 = 用驱动默认
        bool auto_transaction = true;           // 调用方未开事务时是否由游标自建事务
                                                // （PG 必须 true；MySQL/ODBC 忽略）
    };

} // namespace dbmw::core

#endif // DBMW_CORE_CURSOR_H
