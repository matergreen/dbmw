#ifndef DBMW_COMMON_TYPES_H
#define DBMW_COMMON_TYPES_H

#include <chrono>
#include <functional>
#include <iosfwd>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>


namespace dbmw::common {
    // 时间点：对应 SQL 的 TIMESTAMP / DATETIME / DATE。
    using Timestamp = std::chrono::system_clock::time_point;

    // 二进制大对象：对应 SQL 的 BLOB / BYTEA / VARBINARY。
    using Blob = std::vector<std::uint8_t>;

    // 字段值：支持常见 SQL 列类型；nullptr_t 表示 SQL NULL。
    using Value = std::variant<std::nullptr_t, bool, std::int64_t, double,
                               std::string, Timestamp, Blob>;

    // 一行数据：列名 -> 值。
    //
    // 注意：底层是有序 map，若查询返回重名列（如 SELECT a.id, b.id），
    // 后一列会覆盖前一列。需要区分时请在 SQL 里显式取别名。
    class Row {
    public:
        using Map = std::map<std::string, Value>;

        void set(const std::string &col, Value v) { data_[col] = std::move(v); }
        [[nodiscard]] bool has(const std::string &col) const { return data_.find(col) != data_.end(); }

        // 取值：缺失时返回静态 NULL 值（不会插入新键）。
        [[nodiscard]] const Value &at(const std::string &col) const {
            static const Value kNull{nullptr};
            auto it = data_.find(col);
            return it == data_.end() ? kNull : it->second;
        }

        [[nodiscard]] const Map &data() const { return data_; }
        [[nodiscard]] size_t size() const { return data_.size(); }

    private:
        Map data_;
    };

    // 查询结果集：多行 + 字段顺序。
    //
    // Row 内部是有序 map，遍历 Row 得到的是字典序而非 SELECT 顺序；
    // 需要按 SELECT 原序遍历列时，请用 fields() 给出的列名顺序。
    class ResultSet {
    public:
        // 由驱动在填充数据前调用一次，声明列的顺序。
        void setFields(std::vector<std::string> fields) { fields_ = std::move(fields); }

        [[nodiscard]] const std::vector<std::string> &fields() const { return fields_; }

        void addRow(Row row) { rows_.push_back(std::move(row)); }
        [[nodiscard]] const std::vector<Row> &rows() const { return rows_; }
        [[nodiscard]] size_t rowCount() const { return rows_.size(); }
        [[nodiscard]] bool empty() const { return rows_.empty(); }
        void clear() { fields_.clear(); rows_.clear(); }

    private:
        std::vector<std::string> fields_;
        std::vector<Row> rows_;
    };

    // 绑定参数列表：与 SQL 中的 '?' 占位符按位置一一对应。
    using Params = std::vector<Value>;
    using ParamBatch = std::vector<Params>;
    using RowCallback = std::function<bool(const Row &)>;

    // 仅用于诊断日志的 SQL 渲染策略；渲染结果绝不能替代原生参数绑定执行。
    struct SqlRenderOptions {
        bool includeStringValues = false;
        bool includeBlobValues = false;
        std::size_t maxParamLength = 256;
        std::size_t maxSqlLength = 8192;
    };

    struct BatchResult {
        std::vector<std::int64_t> affected;
        [[nodiscard]] std::int64_t totalAffected() const {
            std::int64_t total = 0;
            for (const auto rows: affected) total += rows;
            return total;
        }
        void clear() { affected.clear(); }
    };

    // 生成键：INSERT 之后由数据库生成的列（MySQL 自增主键、PG/ODBC RETURNING 出的列）。
    //
    // 统一模型是"生成列的结果集"：MySQL 用 mysql_insert_id 合成一行一列，
    // PG / ODBC 由语句自带的 RETURNING / OUTPUT 直出。因此 lastInsertId() 在
    // MySQL 上顺手可用，在 PG / ODBC 上同样适用于 RETURNING 出来的行。
    //
    // 注意：dbmw **不会**给 SQL 自动追加 RETURNING（那会改写语义并耦合方言）；
    // PG / ODBC 想拿生成键，就在 SQL 里自己写 RETURNING / OUTPUT。
    struct GeneratedKeys {
        ResultSet rows; // 每行 = 一条被插记录生成的列

        [[nodiscard]] bool empty() const { return rows.empty(); }

        // 复用同一对象前必须清空：重试着法会重新下发语句，
        // 残留的旧行会被当成这次生成的键。
        void clear() { rows.clear(); }

        // 便捷取法：首行首列的整数值（MySQL 单列自增语义）。
        // 为空、或首列既不是整数也不是纯数字文本时返回 0。
        [[nodiscard]] std::int64_t lastInsertId() const;
    };

    // 流式数据源：同步执行期间由驱动按块拉取字节，调用结束后即失效（不跨调用存活）。
    //
    // 只用于**输入参数**（超大 BLOB / CLOB 写入），与结果集的流式消费
    // （queryEach / 游标）是两个相反的方向，不要混用。
    // 用 istream 构造时，流必须在本次执行期间保持存活。
    class StreamSource {
    public:
        // 返回本块实际读到的字节数；返回 0 表示 EOF。
        using ReadFn = std::function<std::size_t(void *buf, std::size_t n)>;

        explicit StreamSource(ReadFn read, const std::optional<std::uint64_t> totalSize = std::nullopt,
                              const bool isBinary = true)
            : read_(std::move(read)), totalSize_(totalSize), isBinary_(isBinary) {}

        // 便捷构造：从 istream 按块读取（不预知总长度）。
        explicit StreamSource(std::istream &in, bool isBinary = true);

        std::size_t read(void *buf, std::size_t n) {
            if (!read_ || n == 0) return 0;
            return read_(buf, n);
        }

        // 预先声明的总字节数；为空时 ODBC 用 SQL_DATA_AT_EXEC、PG 按 bytea 读。
        [[nodiscard]] std::optional<std::uint64_t> totalSize() const { return totalSize_; }

        // true = 二进制（bytea / blob），false = 文本（clob）。
        [[nodiscard]] bool isBinary() const { return isBinary_; }

    private:
        ReadFn read_;
        std::optional<std::uint64_t> totalSize_;
        bool isBinary_ = true;
    };

    // 位置参数：每个位置要么是普通值，要么是流式源。
    //
    // 刻意**不扩展 Value**：Value 参与 cacheKey / 结果映射 / 游标等既有逻辑，
    // 把流塞进 variant 会牵连一大片。这里用并列的载体，老路径零改动。
    using StreamParam = std::variant<Value, StreamSource>;
    using StreamParams = std::vector<StreamParam>;
    using StreamParamBatch = std::vector<StreamParams>;

    enum class IsolationLevel {
        Default,
        ReadUncommitted,
        ReadCommitted,
        RepeatableRead,
        Serializable
    };

    struct TransactionOptions {
        IsolationLevel isolation = IsolationLevel::Default;
        bool readOnly = false;
        // 整个事务回调的期限。
        //
        // 到期时会请求驱动取消当前语句，随后回滚并返回 QueryTimeout。
        // 注意这是**尽力而为**的上限，不是硬中断：
        //   - 驱动实现了 cancel()（MySQL / PostgreSQL / ODBC 内置驱动）时，
        //     语句会被真正打断，通常很快返回；
        //   - 驱动未实现时（基类默认返回 NotSupported），C++ 无法安全地强杀
        //     正在跑的用户回调，只能等它自然结束后把结果改写成 QueryTimeout。
        //     此时返回的 message 会带上 "could not cancel" 提示，
        //     便于区分"被及时取消"和"其实没打断，只是事后判了超时"。
        // 因此回调本身仍应避免无限阻塞；超时机制是兜底而非替代。
        std::chrono::milliseconds timeout{0};
    };

    // 错误码。
    enum class ErrorCode {
        Ok = 0,
        ConfigError,       // 配置解析/校验失败
        ConnectionFailed,  // 连接建立失败
        QueryError,        // 查询失败
        QueryTimeout,      // 查询或事务超过执行期限
        Cancelled,         // 操作被调用方或服务端取消
        ConstraintViolation, // 唯一键/外键/非空等约束冲突
        Deadlock,          // 死锁或序列化冲突
        PingFailed,        // 心跳失败
        TxError,           // 事务失败
        PoolExhausted,     // 连接池耗尽（含等待借出超时）
        PoolClosed,        // 连接池已关闭（shutdown 之后仍在借连接）
        CircuitOpen,       // 数据源熔断中，快速失败
        NotConnected,      // 连接未建立/已断开
        DriverDisabled,    // 驱动未在编译期启用
        UnknownDriver,     // 未知数据源类型
        NotSupported,      // 驱动未实现该能力
        RateLimited,       // 被限流（非重试，调用方应立即失败或本地排队）
        SqlBlocked,        // SQL 被审计策略拦截
        Buffered,          // 写请求已入缓冲队列，尚未真正提交（语义非"成功提交"）
        CursorClosed,      // 在已关闭/已移动的游标上调用 fetch/fetchRow（EOF 或 reuse 后）
        CursorLimit,       // 超过 max_open_cursors，拒绝开新游标（调用方应等待/降级）
        CursorError,       // 游标底层错误（驱动/服务端游标相关）
        Unknown
    };

    // 错误码 -> 稳定字符串（便于日志与跨语言边界传递）。
    const char *errorCodeToString(ErrorCode c);

    // 操作状态：成功/失败 + 消息。
    //
    // 注意：成员函数 ok() 是判定函数，构造成功状态请用静态 OK()。
    struct Status {
        ErrorCode code = ErrorCode::Ok;
        std::string message;
        // 驱动提供时保留底层错误信息，调用方不必解析 message。
        std::string sqlState;
        std::int64_t nativeCode = 0;
        bool retryable = false;
        bool connectionBroken = false;

        [[nodiscard]] bool ok() const { return code == ErrorCode::Ok; }

        static Status OK() { return Status{}; }

        static Status error(ErrorCode c, std::string msg) {
            Status status;
            status.code = c;
            status.message = std::move(msg);
            return status;
        }

        // 根据 SQLSTATE 分类超时、约束、死锁及连接故障，并标出是否可重试。
        static Status databaseError(ErrorCode fallback, std::string msg,
                                    std::string state, std::int64_t vendorCode = 0);
    };

    // 值 -> 字符串（用于日志/调试，不做 SQL 转义）。
    std::string valueToString(const Value &v);

    // 时间点 -> "YYYY-MM-DD HH:MM:SS"（秒级，用于日志）。
    std::string timestampToString(const Timestamp &t);

    // 时间点 -> 带毫秒的形式（用于 SQL 字面量，精度更高）。
    std::string timestampToStringMs(const Timestamp &t);

    // 尝试解析 "YYYY-MM-DD[ HH:MM:SS[.fff]]" 形式的时间字符串。
    // 解析失败返回 false，调用方应回退为字符串形式，避免丢数据。
    bool tryParseTimestamp(const std::string &s, Timestamp &out);

    // 通用字面量转义（不依赖具体驱动）。
    // 单引号翻倍；供不支持服务端绑定的驱动做插值时兜底。
    std::string escapeLiteralGeneric(const Value &v);

    // 转义标识符（表名/列名）：双引号翻倍并整体加引号，防注入。
    std::string quoteIdentifier(const std::string &ident);

    // 流式降级：把 StreamSource 整段读入 Blob 并转成普通 Params。
    //
    // libpq 协议不支持参数的 data-at-execution（参数必须一次性随请求包发出），
    // 因此 PostgreSQL 驱动、以及不实现流式能力的所有老驱动，都走这条缓冲路径。
    // 语义上等价于调用方自己把整个大对象读进内存，只是省了一道手工搬运。
    //
    // 声明刻意放在文件末尾：它返回 Status，而 Status 要到本文件中段才定义。
    Status streamParamsToParams(const StreamParams &params, Params &out);

    // 参数类型签名：按位置的类型标记序列。
    //
    // 用作预编译语句缓存 key 的一部分——PG / MySQL 在 prepare 阶段就需要参数类型，
    // 类型序列不同、文本相同的两条 SQL 必须视为两条不同的预备语句。
    // 每个参数一个字符：n=NULL, b=bool, i=int64, d=double, s=string, t=Timestamp, x=Blob。
    std::string paramTypeSignature(const Params &params);
} // namespace dbmw::common


#endif // DBMW_COMMON_TYPES_H
