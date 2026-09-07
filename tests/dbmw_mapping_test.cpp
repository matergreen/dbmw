// dbmw v0.5.0 实体映射层行为验证（docs/mapping-design-v0.5.0.md §10：M1–M23）。
//
// 手法与 dbmw_core_test / dbmw_async_test 一致：mock 驱动 + 自研 check 宏，
// 不依赖第三方测试框架与真实数据库。
//
// 运行顺序有依赖：
//   - 各节通过替换全局行夹具（gRows）控制 mock 返回；
//   - M18（缓存）/ M19（脱敏）会 reload 配置，之后必须 reload 回基础配置；
//   - 协程段仅在 DBMW_ENABLE_ASYNC_CORO=ON 时编译。
#include "dbmw/dbmw.h"
#include "dbmw/mapping.h"
#include "dbmw/async/dbmw_async.h"
#include "dbmw/core/idatabase_connection.h"
#include "dbmw/core/interceptor.h"
#include "dbmw/driver/driver_registry.h"

#if defined(DBMW_ENABLE_ASYNC_CORO)
#include "dbmw/async/task.h"
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

using namespace dbmw;
using common::Status;

static int g_failed = 0;
static int g_passed = 0;

static void check(const bool cond, const std::string &name) {
    if (cond) { ++g_passed; std::cout << "  [PASS] " << name << "\n"; }
    else { ++g_failed; std::cout << "  [FAIL] " << name << "\n"; }
}

// ---------------------------------------------------------------------------
// Mock 驱动：查询返回全局行夹具；写操作返回自增键 / RETURNING 夹具
// ---------------------------------------------------------------------------
using RowData = std::vector<std::pair<std::string, common::Value>>;

static std::vector<RowData>   gRows;
static std::atomic<int>       gQueryCalls{0};
static std::atomic<int>       gExecuteCalls{0};
static std::int64_t           gInsertId = 0;              // MySQL 路径：合成列 insert_id
static RowData                gKeyRow;                    // PG/ODBC 路径：RETURNING 行

static common::ResultSet buildResultSet(const std::vector<RowData> &rows) {
    common::ResultSet rs;
    if (!rows.empty()) {
        std::vector<std::string> fields;
        for (const auto &kv : rows.front()) fields.push_back(kv.first);
        rs.setFields(std::move(fields));
    }
    for (const auto &rd : rows) {
        common::Row r;
        for (const auto &kv : rd) r.set(kv.first, kv.second);
        rs.addRow(std::move(r));
    }
    return rs;
}

class MappingMockConnection : public core::IDatabaseConnection {
public:
    Status connect(const config::DataSourceConfig &cfg) override {
        (void) cfg;
        open_ = true;
        return Status::OK();
    }
    Status ping() override {
        return open_ ? Status::OK() : Status::error(common::ErrorCode::PingFailed, "closed");
    }
    Status query(const std::string &sql, common::ResultSet &out) override {
        (void) sql;
        ++gQueryCalls;
        out = buildResultSet(gRows);
        return Status::OK();
    }
    Status execute(const std::string &sql, std::int64_t &affected) override {
        (void) sql;
        ++gExecuteCalls;
        affected = 1;
        return Status::OK();
    }
    Status execute(const std::string &sql, const common::Params &params,
                   std::int64_t &affected, common::GeneratedKeys &out) override {
        (void) sql;
        (void) params;
        ++gExecuteCalls;
        affected = 1;
        out.clear();
        if (!gKeyRow.empty()) {
            out.rows = buildResultSet({gKeyRow});
        } else if (gInsertId != 0) {
            common::Row r;
            r.set("insert_id", static_cast<std::int64_t>(gInsertId));
            out.rows.setFields({"insert_id"});
            out.rows.addRow(std::move(r));
        }
        return Status::OK();
    }
    Status begin() override { tx_ = true; return Status::OK(); }
    Status begin(const common::TransactionOptions &) override { return begin(); }
    Status commit() override { tx_ = false; return Status::OK(); }
    Status rollback() override { tx_ = false; return Status::OK(); }
    void close() override { open_ = false; }
    bool isOpen() const override { return open_; }
    bool inTransaction() const override { return tx_; }
    bool allowsLiteralInterpolation() const override { return true; }

private:
    bool open_ = false;
    bool tx_ = false;
};

class MappingMockDriver : public driver::IDriver {
public:
    const char *name() const override { return "mmock"; }
    std::unique_ptr<core::IDatabaseConnection> createConnection() override {
        return std::make_unique<MappingMockConnection>();
    }
};

// 直连测试 fetchAs：不经过驱动，构造一个假游标。
class FakeCursor final : public core::ICursor {
public:
    explicit FakeCursor(std::vector<RowData> rows) : rows_(std::move(rows)) {}
    common::Status fetch(std::size_t n, common::ResultSet &out) override {
        const std::size_t take = (n == 0 || n > rows_.size() - pos_) ? rows_.size() - pos_ : n;
        std::vector<RowData> slice(rows_.begin() + static_cast<long>(pos_),
                                   rows_.begin() + static_cast<long>(pos_ + take));
        pos_ += take;
        out = buildResultSet(slice);
        return Status::OK();
    }
    common::Status fetchRow(common::Row &out, bool &ok) override {
        if (pos_ >= rows_.size()) {
            ok = false;
            return Status::error(common::ErrorCode::CursorClosed, "EOF");
        }
        out = buildResultSet({rows_[pos_++]}).rows().front();
        ok = true;
        return Status::OK();
    }
    common::Status close() override { open_ = false; return Status::OK(); }
    bool isOpen() const override { return open_; }
    bool hasNext() const override { return pos_ < rows_.size(); }
    std::uint64_t rowsFetched() const override { return static_cast<std::uint64_t>(pos_); }

private:
    std::vector<RowData> rows_;
    std::size_t pos_ = 0;
    bool open_ = true;
};

// ---------------------------------------------------------------------------
// 实体与映射声明（纯手写特化，无宏）
// ---------------------------------------------------------------------------
struct User {
    std::int64_t id = 0;
    std::string name;
    std::optional<std::string> email;
    common::Decimal balance{"0"};
    common::Timestamp createdAt{};
};

struct StrictUser {   // 多余列直接报错
    std::int64_t id = 0;
    std::string name;
};

struct StrictMissingUser {   // 缺列直接报错
    std::int64_t id = 0;
    std::string name;
    common::Decimal balance{"0"};
};

struct FlagRow { bool active = false; };
struct SmallRow { std::int32_t qty = 0; };
struct LossyAmountRow { double amount = 0; };
struct StrictAmountRow { double amount = 0; };
struct TextualTsRow { common::Timestamp ts{}; };
struct PlainTsRow { common::Timestamp ts{}; };
struct StrRow { std::string s; };

enum class UserState : std::int32_t { Active = 1, Locked = 2 };
struct EnumRow { UserState st = UserState::Active; };

struct UserId { std::int64_t v = 0; };          // 业务自定义类型：走 ValueConverter 特化
struct RefRow { UserId uid; };

struct NoPkRow { std::string name; };           // 无主键：updateAs 必须拒绝

namespace dbmw::mapping {

    template <> struct RowMapper<User> {
        static Mapping<User> describe() {
            return Mapping<User>()
                .field(&User::id, "id", FieldFlags::PrimaryKey | FieldFlags::Generated)
                .field(&User::name, "name")
                .field(&User::email, "email")
                .field(&User::balance, "balance")
                .field(&User::createdAt, "created_at");
        }
    };

    template <> struct RowMapper<StrictUser> {
        static Mapping<StrictUser> describe() {
            return Mapping<StrictUser>()
                .field(&StrictUser::id, "id")
                .field(&StrictUser::name, "name")
                .extraColumns(ExtraColumns::Error);
        }
    };

    template <> struct RowMapper<StrictMissingUser> {
        static Mapping<StrictMissingUser> describe() {
            return Mapping<StrictMissingUser>()
                .field(&StrictMissingUser::id, "id")
                .field(&StrictMissingUser::name, "name")
                .field(&StrictMissingUser::balance, "balance")
                .missingColumns(MissingColumns::Error);
        }
    };

    template <> struct RowMapper<FlagRow> {
        static Mapping<FlagRow> describe() { return Mapping<FlagRow>().field(&FlagRow::active, "active"); }
    };
    template <> struct RowMapper<SmallRow> {
        static Mapping<SmallRow> describe() { return Mapping<SmallRow>().field(&SmallRow::qty, "qty"); }
    };
    template <> struct RowMapper<LossyAmountRow> {
        static Mapping<LossyAmountRow> describe() {
            return Mapping<LossyAmountRow>().field(&LossyAmountRow::amount, "amount", FieldFlags::Lossy);
        }
    };
    template <> struct RowMapper<StrictAmountRow> {
        static Mapping<StrictAmountRow> describe() {
            return Mapping<StrictAmountRow>().field(&StrictAmountRow::amount, "amount");
        }
    };
    template <> struct RowMapper<TextualTsRow> {
        static Mapping<TextualTsRow> describe() {
            return Mapping<TextualTsRow>().field(&TextualTsRow::ts, "ts", FieldFlags::Textual);
        }
    };
    template <> struct RowMapper<PlainTsRow> {
        static Mapping<PlainTsRow> describe() { return Mapping<PlainTsRow>().field(&PlainTsRow::ts, "ts"); }
    };
    template <> struct RowMapper<StrRow> {
        static Mapping<StrRow> describe() { return Mapping<StrRow>().field(&StrRow::s, "s"); }
    };
    template <> struct RowMapper<EnumRow> {
        static Mapping<EnumRow> describe() { return Mapping<EnumRow>().field(&EnumRow::st, "st"); }
    };
    template <> struct RowMapper<RefRow> {
        static Mapping<RefRow> describe() { return Mapping<RefRow>().field(&RefRow::uid, "uid"); }
    };
    template <> struct RowMapper<NoPkRow> {
        static Mapping<NoPkRow> describe() { return Mapping<NoPkRow>().field(&NoPkRow::name, "name"); }
    };

    // M9：自定义类型接入
    template <> struct ValueConverter<UserId> {
        static Status fromValue(const common::Value &v, UserId &out, FieldFlags) {
            if (const auto p = std::get_if<std::int64_t>(&v)) { out.v = *p; return Status::OK(); }
            return mapError("UserId expects int64");
        }
        static common::Value toValue(const UserId &in) { return common::Value(in.v); }
    };

} // namespace dbmw::mapping

// ---------------------------------------------------------------------------
// 配置与夹具工具
// ---------------------------------------------------------------------------
struct CfgFlags {
    bool cache = false;
    bool interceptors = false;
};

static std::string buildConfig(const CfgFlags &f) {
    const std::string cache = f.cache
        ? R"("query_cache": { "enabled": true, "ttl_ms": 60000, "max_entries": 100 },)"
        : R"("query_cache": { "enabled": false },)";
    const std::string icp = f.interceptors
        ? R"("interceptors": { "enabled": true },)"
        : R"("interceptors": { "enabled": false },)";
    return R"({
  "default_datasource": "main",
  "heartbeat_interval_ms": 5000,
  "pool": { "enabled": true, "min": 0, "max": 4, "borrow_timeout_ms": 2000 },
  "retry": { "max_attempts": 1, "initial_backoff_ms": 10, "max_backoff_ms": 10, "retry_writes": false },
  )" + cache + icp + R"(
  "async": { "enabled": true, "threads": 2, "queue_size": 64 },
  "datasources": [
    { "name": "main", "type": "mmock", "host": "localhost" }
  ],
  "groups": []
}
)";
}

static std::string g_configPath;

static void applyConfig(const CfgFlags &f) {
    std::ofstream(g_configPath) << buildConfig(f);
}

static common::Timestamp ts(const std::string &s) {
    common::Timestamp t{};
    common::tryParseTimestamp(s, t);
    return t;
}

static RowData userRow(const std::int64_t id, const std::string &name,
                       const std::optional<std::string> &email, const std::string &bal) {
    RowData rd;
    rd.emplace_back("id", common::Value(id));
    rd.emplace_back("name", common::Value(name));
    if (email) rd.emplace_back("email", common::Value(*email));
    else rd.emplace_back("email", common::Value(nullptr));
    rd.emplace_back("balance", common::Value(common::Decimal{bal}));
    rd.emplace_back("created_at", common::Value(ts("2026-09-07 10:00:00")));
    return rd;
}

// M19：把 name 列脱敏为 ***
class MaskingInterceptor final : public core::ISqlInterceptor {
public:
    void onRoute(const std::string &, const std::string &, common::OperationType,
                 common::SqlContext &) override {}
    common::Status beforeExecution(const core::ExecutionView &) override { return Status::OK(); }
    void afterExecution(const core::ExecutionView &view) override {
        if (view.result == nullptr) return;
        for (auto &row : view.result->mutableRows())
            if (row.has("name")) row.set("name", common::Value(std::string("***")));
    }
    void onCompletion(const core::ExecutionView &) override {}
};

#if defined(DBMW_ENABLE_ASYNC_CORO)
// 协程段：实参一律用具名局部变量（GCC 13 ICE 规避，见 task.h 注释）
static async::Task<void> coroQueryBody(std::promise<EntityResult<User>> pr) {
    common::Params p;
    auto r = co_await async::queryAsAsync<User>("SELECT coro", p);
    pr.set_value(std::move(r));
}
#endif

// ---------------------------------------------------------------------------
int main() {
    g_configPath = (std::filesystem::temp_directory_path() / "dbmw_mapping_test.json").string();

    driver::DriverRegistry::instance().registerDriver(
        "mmock", [] { return std::make_unique<MappingMockDriver>(); });

    CfgFlags base;
    applyConfig(base);
    if (!DBMW::init(g_configPath).ok()) {
        std::cout << "init failed\n";
        return 1;
    }

    // =====================================================================
    std::cout << "== M1. 基本映射：全类型往返 ==\n";
    {
        gRows = {userRow(1, "alice", std::string("a@x.com"), "12.50"),
                 userRow(2, "bob", std::nullopt, "0.01")};
        const auto r = queryAs<User>("SELECT * FROM users");
        check(r.status.ok(), "queryAs 成功");
        check(r.items.size() == 2, "两行全部映射");
        if (r.items.size() == 2) {
            const auto &a = r.items[0];
            check(a.id == 1 && a.name == "alice", "整型与字符串字段正确");
            check(a.email.has_value() && *a.email == "a@x.com", "optional 有值时正确");
            check(a.balance.value == "12.50", "Decimal 原样保留（未转 double）");
            check(a.createdAt == ts("2026-09-07 10:00:00"), "Timestamp 正确");
            check(!r.items[1].email.has_value(), "NULL → nullopt");
        }
        const auto one = queryOneAs<User>("SELECT * FROM users WHERE id = ?",
                                          common::Params{common::Value(std::int64_t(1))});
        check(!one.status.ok() && one.status.code == common::ErrorCode::MappingError,
              "queryOneAs 在多行时报错（不是静默取第一行）");
    }

    // =====================================================================
    std::cout << "== M2/M3. NULL 语义 ==\n";
    {
        gRows = {userRow(1, "alice", std::nullopt, "1.00")};
        const auto r = queryAs<User>("SELECT 1");
        check(r.status.ok() && r.items.size() == 1 && !r.items[0].email.has_value(),
              "NULL 落进 optional → nullopt（合法）");

        // name 是 std::string（非 optional），给 NULL 必须报错
        RowData bad = userRow(1, "x", std::nullopt, "1.00");
        for (auto &kv : bad) if (kv.first == "name") kv.second = common::Value(nullptr);
        gRows = {bad};
        const auto r2 = queryAs<User>("SELECT 2");
        check(!r2.status.ok() && r2.status.code == common::ErrorCode::MappingError,
              "NULL 落进非 optional → MappingError");
        check(r2.items.empty(), "失败时 items 为空（I8：不返回半成品）");
        check(r2.status.message.find("name") != std::string::npos,
              "错误信息含列名");
    }

    // =====================================================================
    std::cout << "== M4/M5. 列匹配：缺列默认跳过、多余列可配 ==\n";
    {
        RowData missing = userRow(1, "alice", std::nullopt, "1.00");
        missing.erase(missing.begin() + 3); // 去掉 balance 列
        gRows = {missing};
        const auto r = queryAs<User>("SELECT 3");
        check(r.status.ok(), "缺列默认跳过（宽松模式，正常返回）");
        if (r.status.ok() && r.items.size() == 1) {
            const auto &u = r.items[0];
            check(u.id == 1 && u.name == "alice", "存在列正常映射");
            check(u.balance.value == "0", "缺失列保持默认构造值（未退化成 NULL 也不报错）");
        }

        // 严格：MissingColumns::Error 下缺列仍报错，并指出列名
        const auto strict = queryAs<StrictMissingUser>("SELECT 3b");
        check(!strict.status.ok() && strict.status.code == common::ErrorCode::MappingError,
              "MissingColumns::Error 下缺列仍报错");
        check(strict.status.message.find("balance") != std::string::npos,
              "错误信息指出缺失的列名");

        // 多余列：默认忽略
        RowData extra = userRow(1, "alice", std::nullopt, "1.00");
        extra.emplace_back("extra_col", common::Value(std::string("x")));
        gRows = {extra};
        check(queryAs<User>("SELECT 4").status.ok(),
              "多余列默认忽略（兼容 SELECT * / 联表）");
        check(!queryAs<StrictUser>("SELECT 4b").status.ok(),
              "ExtraColumns::Error 下多余列报错");
    }

    // =====================================================================
    std::cout << "== M6. 类型不符矩阵（严格模式全部报错）==\n";
    {
        gRows = {RowData{{"active", common::Value(true)}}};
        check(queryAs<FlagRow>("SELECT f1").status.ok(), "bool ← bool 通过");

        gRows = {RowData{{"active", common::Value(std::int64_t(1))}}};
        const auto b1 = queryAs<FlagRow>("SELECT f2");
        check(!b1.status.ok() && b1.status.code == common::ErrorCode::MappingError,
              "bool ← int64 报错（驱动返回整型 = 声明与列类型不符）");

        gRows = {RowData{{"qty", common::Value(std::int64_t(5))}}};
        check(queryAs<SmallRow>("SELECT f3").status.ok(), "int32 ← int64 范围内通过");

        gRows = {RowData{{"qty", common::Value(std::int64_t(5000000000LL))}}};
        const auto o1 = queryAs<SmallRow>("SELECT f4");
        check(!o1.status.ok() && o1.status.message.find("out of range") != std::string::npos,
              "整型溢出报错且消息含 out of range");

        gRows = {RowData{{"amount", common::Value(common::Decimal{"12.50"})}}};
        const auto d1 = queryAs<StrictAmountRow>("SELECT f5");
        check(!d1.status.ok() && d1.status.code == common::ErrorCode::MappingError,
              "double ← Decimal 默认拒绝（防丢精度）");

        gRows = {RowData{{"s", common::Value(common::Blob{1, 2, 3})}}};
        check(!queryAs<StrRow>("SELECT f6").status.ok(), "string ← Blob 报错");

        gRows = {RowData{{"ts", common::Value(std::string("2026-09-07 10:00:00"))}}};
        check(!queryAs<PlainTsRow>("SELECT f7").status.ok(),
              "Timestamp ← string 未声明 Textual 时报错");
    }

    // =====================================================================
    std::cout << "== M7. Lossy / Textual 显式放开 ==\n";
    {
        gRows = {RowData{{"amount", common::Value(common::Decimal{"12.50"})}}};
        const auto r = queryAs<LossyAmountRow>("SELECT l1");
        check(r.status.ok() && r.items.size() == 1 && r.items[0].amount == 12.5,
              "Lossy 下 double ← Decimal 放行");

        gRows = {RowData{{"ts", common::Value(std::string("2026-09-07 10:00:00"))}}};
        const auto r2 = queryAs<TextualTsRow>("SELECT l2");
        check(r2.status.ok() && r2.items.size() == 1 &&
              r2.items[0].ts == ts("2026-09-07 10:00:00"),
              "Textual 下 Timestamp ← string 解析通过");
    }

    // =====================================================================
    std::cout << "== M8. enum class ==\n";
    {
        gRows = {RowData{{"st", common::Value(std::int64_t(2))}}};
        const auto r = queryAs<EnumRow>("SELECT e1");
        check(r.status.ok() && r.items.size() == 1 && r.items[0].st == UserState::Locked,
              "enum ← int64 映射正确");

        gRows = {RowData{{"st", common::Value(std::int64_t(5000000000LL))}}};
        check(!queryAs<EnumRow>("SELECT e2").status.ok(),
              "enum 底层整型溢出报错（范围检查）");
    }

    // =====================================================================
    std::cout << "== M9. 自定义 ValueConverter ==\n";
    {
        gRows = {RowData{{"uid", common::Value(std::int64_t(5))}}};
        const auto r = queryAs<RefRow>("SELECT c1");
        check(r.status.ok() && r.items.size() == 1 && r.items[0].uid.v == 5,
              "自定义类型经 ValueConverter 特化接入");

        gRows = {RowData{{"uid", common::Value(std::string("nope"))}}};
        check(!queryAs<RefRow>("SELECT c2").status.ok(), "自定义转换器可拒绝不符类型");
    }

    // =====================================================================
    std::cout << "== M10/M11. 写方向：参数与 SQL 片段 ==\n";
    {
        User u;
        u.id = 7;
        u.name = "alice";
        u.email = std::string("a@x.com");
        u.balance = common::Decimal{"3.25"};
        u.createdAt = ts("2026-09-07 10:00:00");

        const auto writable = mapping::paramsOf(u);
        check(writable.size() == 4, "Writable 参数跳过 Generated 列（4 个）");
        check(mapping::paramsOf(u, mapping::WriteCols::All).size() == 5, "All 参数含主键");
        check(mapping::paramsOf(u, mapping::WriteCols::PrimaryKey).size() == 1, "PrimaryKey 只有 1 个");
        if (writable.size() == 4) {
            check(std::get<std::string>(writable[0]) == "alice", "参数顺序 = 声明顺序（name 第一）");
            check(std::get<common::Decimal>(writable[2]).value == "3.25", "Decimal 参数原样下发");
        }

        const std::string ins = mapping::insertSql<User>("users");
        check(ins.find("INSERT INTO \"users\"") == 0, "insertSql 表名走 quoteIdentifier");
        check(ins.find("\"id\"") == std::string::npos, "insertSql 跳过 Generated 列");
        check(std::count(ins.begin(), ins.end(), '?') == 4, "占位符数量 = 可写列数");

        const std::string upd = mapping::updateSql<User>("users");
        check(upd.rfind("UPDATE \"users\" SET", 0) == 0, "updateSql 形态正确");
        check(upd.find("WHERE") != std::string::npos, "updateSql 带 WHERE（由 PrimaryKey 推导）");
        check(upd.find("\"id\" = ?") != std::string::npos, "WHERE 使用主键列");

        const auto up = mapping::updateParamsOf(u);
        check(up.size() == 5 && std::get<std::int64_t>(up.back()) == 7,
              "updateParamsOf：SET 列在前、主键列在后");

        // 标识符转义：列名/表名带双引号应被翻倍
        const std::string evil = mapping::insertSql<User>("us\"ers");
        check(evil.find("\"us\"\"ers\"") != std::string::npos, "表名含引号时被正确转义");
    }

    // =====================================================================
    std::cout << "== M12. insertAs 生成键回填 ==\n";
    {
        gKeyRow.clear();
        gInsertId = 42;
        User u;
        u.name = "alice";
        u.balance = common::Decimal{"1.00"};
        const auto r = insertAs<User>("users", u);
        check(r.status.ok() && r.affected == 1, "insertAs 执行成功");
        check(u.id == 42, "MySQL 路径：insert_id 回填到 Generated 列");

        gInsertId = 0;
        gKeyRow = RowData{{"id", common::Value(std::int64_t(99))}};
        User u2;
        u2.name = "bob";
        const auto r2 = insertAs<User>("users", u2);
        check(r2.status.ok() && u2.id == 99, "PG/ODBC 路径：RETURNING 列按列名回填");
        gKeyRow.clear();
    }

    // =====================================================================
    std::cout << "== M13/M14. 无主键拒绝 / 批量 ==\n";
    {
        NoPkRow np;
        np.name = "x";
        const auto r = updateAs<NoPkRow>("t", np);
        check(!r.status.ok() && r.status.code == common::ErrorCode::MappingError,
              "无 PrimaryKey 时 updateAs 拒绝生成无条件 UPDATE");

        std::vector<User> batch(2);
        batch[0].name = "a";
        batch[1].name = "b";
        const auto br = insertBatchAs<User>("users", batch);
        check(br.status.ok(), "insertBatchAs 执行成功");
        check(br.batch.totalAffected() >= 1, "批量返回受影响行数");
        const auto pb = mapping::batchOf(batch);
        check(pb.size() == 2 && pb[0].size() == 4, "batchOf 每行一套可写参数");
    }

    // =====================================================================
    std::cout << "== M15. 流式 queryEachAs ==\n";
    {
        gRows = {userRow(1, "a", std::nullopt, "1.00"),
                 userRow(2, "b", std::nullopt, "2.00"),
                 userRow(3, "c", std::nullopt, "3.00")};
        std::vector<std::string> names;
        std::uint64_t rows = 0;
        const auto st = queryEachAs<User>("SELECT stream", common::Params{},
                                          [&](User &&u) { names.push_back(u.name); return true; }, rows);
        check(st.ok() && rows == 3, "queryEachAs 全量映射 3 行");
        check(names.size() == 3 && names[0] == "a" && names[2] == "c", "逐行映射内容正确");

        std::uint64_t rows2 = 0;
        int seen = 0;
        const auto st2 = queryEachAs<User>("SELECT stream2", common::Params{},
                                           [&](User &&) { return ++seen < 2; }, rows2);
        check(st2.ok() && rows2 == 2, "回调返回 false 提前终止（rows=2）");

        // 映射失败 → 立即停并以 MappingError 收尾
        RowData bad = userRow(1, "a", std::nullopt, "1.00");
        for (auto &kv : bad) if (kv.first == "name") kv.second = common::Value(std::int64_t(1));
        gRows = {bad, userRow(2, "b", std::nullopt, "2.00")};
        std::uint64_t rows3 = 0;
        const auto st3 = queryEachAs<User>("SELECT stream3", common::Params{},
                                           [](User &&) { return true; }, rows3);
        check(!st3.ok() && st3.code == common::ErrorCode::MappingError && rows3 == 0,
              "流式映射失败立即停止并报错");
    }

    // =====================================================================
    std::cout << "== M16. 游标 fetchAs ==\n";
    {
        FakeCursor cur({userRow(1, "a", std::nullopt, "1.00"), userRow(2, "b", std::nullopt, "2.00")});
        const auto r = fetchAs<User>(cur, 10);
        check(r.status.ok() && r.items.size() == 2, "fetchAs 映射两行");
        check(r.items.size() == 2 && r.items[1].name == "b", "游标结果内容正确");
    }

    // =====================================================================
    std::cout << "== M17. 事务内映射（Session 形态）==\n";
    {
        gRows = {userRow(5, "tx", std::string("t@x.com"), "9.99")};
        std::int64_t idSeen = 0;
        const auto st = DBMW::transaction([&](core::Session &s) {
            const auto r = queryAs<User>(s, "SELECT tx", common::Params{});
            if (!r.status.ok()) return r.status;
            if (!r.items.empty()) idSeen = r.items[0].id;
            return Status::OK();
        });
        check(st.ok() && idSeen == 5, "事务内 queryAs(Session&) 结果一致");
    }

    // =====================================================================
    std::cout << "== M18. 查询缓存：只缓存原始 ResultSet，命中后仍映射 ==\n";
    {
        CfgFlags cf;
        cf.cache = true;
        applyConfig(cf);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(), "热加载开启查询缓存");

        gRows = {userRow(1, "cached", std::nullopt, "1.00")};
        const int before = gQueryCalls.load();
        const auto r1 = queryAs<User>("SELECT cached");
        const int mid = gQueryCalls.load();
        const auto r2 = queryAs<User>("SELECT cached");
        const int after = gQueryCalls.load();

        check(r1.status.ok() && r2.status.ok(), "两次查询均成功");
        check(mid - before == 1, "首次查询真实下发驱动（1 次）");
        check(after - mid == 0, "第二次命中缓存，驱动零调用");
        check(r2.items.size() == 1 && r2.items[0].name == "cached",
              "缓存命中后仍执行映射（I1：缓存不存实体）");

        CfgFlags back;
        applyConfig(back);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(), "恢复基础配置");
    }

    // =====================================================================
    std::cout << "== M19. 脱敏顺序：映射在 afterExecution 之后 ==\n";
    {
        CfgFlags cf;
        cf.interceptors = true;
        applyConfig(cf);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(), "热加载开启拦截器");
        DBMW::addInterceptor(std::make_shared<MaskingInterceptor>());

        gRows = {userRow(1, "secret", std::nullopt, "1.00")};
        const auto r = queryAs<User>("SELECT masked");
        check(r.status.ok() && r.items.size() == 1, "脱敏后映射仍成功");
        check(r.items.size() == 1 && r.items[0].name == "***",
              "映射拿到的是脱敏后的值（I2：顺序正确）");

        DBMW::clearInterceptors();
        CfgFlags back;
        applyConfig(back);
        check(DBMW::reload(g_configPath, std::chrono::milliseconds(500)).ok(), "恢复基础配置");
    }

    // =====================================================================
    std::cout << "== M20. 异步三形态：回调 / future / 协程 ==\n";
    {
        gRows = {userRow(1, "async", std::string("a@x.com"), "5.00"),
                 userRow(2, "async2", std::nullopt, "6.00")};
        const auto callerTid = std::this_thread::get_id();

        // 回调式
        std::promise<EntityResult<User>> pr1;
        auto fut1 = pr1.get_future();
        std::thread::id cbTid{};
        common::Params p;
        auto h = async::queryAs<User>("SELECT async", p,
                                      [&](EntityResult<User> &&r) {
                                          cbTid = std::this_thread::get_id();
                                          pr1.set_value(std::move(r));
                                      });
        check(h.valid(), "回调式 queryAs 返回有效 Handle");
        auto o1 = fut1.get();
        check(o1.status.ok() && o1.items.size() == 2, "回调式映射 2 行");
        check(o1.items.size() == 2 && o1.items[0].name == "async", "回调式内容正确");
        check(cbTid != callerTid, "完成回调不在调用线程（异步语义不变）");

        // future 式
        common::Params p2;
        auto fut2 = async::queryAs<User>("SELECT async2", p2);
        auto o2 = fut2.get();
        check(o2.status.ok() && o2.items.size() == 2, "future 式映射结果一致");

        common::Params p3;
        auto fut3 = async::queryAs<User>("SELECT async3", p3);
        auto o3 = fut3.get();
        check(o3.items.size() == o1.items.size() &&
              o3.items[1].id == o1.items[1].id, "回调式与 future 式同源同结果");

#if defined(DBMW_ENABLE_ASYNC_CORO)
        std::promise<EntityResult<User>> prC;
        auto futC = prC.get_future();
        async::run(coroQueryBody(std::move(prC)));
        auto oC = futC.get();
        check(oC.status.ok() && oC.items.size() == 2, "协程式 queryAsAsync 映射成功");
        check(oC.items.size() == 2 && oC.items[0].name == "async", "协程式与同步结果一致");
#else
        check(true, "协程形态未启用（DBMW_ENABLE_ASYNC_CORO=OFF），跳过");
#endif
    }

    // =====================================================================
    std::cout << "== M21/M22. 异步映射失败与失败后可用性 ==\n";
    {
        RowData bad = userRow(1, "a", std::nullopt, "1.00");
        for (auto &kv : bad) if (kv.first == "name") kv.second = common::Value(std::int64_t(1));
        gRows = {bad};
        std::promise<EntityResult<User>> pr;
        auto fut = pr.get_future();
        common::Params p;
        async::queryAs<User>("SELECT bad", p, [&](EntityResult<User> &&r) { pr.set_value(std::move(r)); });
        auto o = fut.get();
        check(!o.status.ok() && o.status.code == common::ErrorCode::MappingError,
              "异步映射失败 → MappingError");
        check(o.items.empty(), "异步失败时 items 为空（I8）");

        gRows = {userRow(1, "ok", std::nullopt, "1.00")};
        const auto r2 = queryAs<User>("SELECT after-fail");
        check(r2.status.ok() && r2.items.size() == 1,
              "映射失败不影响连接与后续操作（I3：纯 CPU，无副作用）");
    }

    // =====================================================================
    std::cout << "== M23. 一致性矩阵：queryAs 与手工映射等价 ==\n";
    {
        gRows = {userRow(3, "same", std::string("s@x.com"), "7.77")};
        common::ResultSet rs;
        const auto st = DBMW::query("SELECT same", common::Params{}, rs);
        std::vector<User> manual;
        const auto ms = mapping::fromRows<User>(rs, manual);
        const auto viaFacade = queryAs<User>("SELECT same", common::Params{});
        check(st.ok() && ms.ok() && viaFacade.status.ok(), "两条路径均成功");
        check(manual.size() == viaFacade.items.size() && manual.size() == 1, "结果行数一致");
        check(manual.size() == 1 && viaFacade.items.size() == 1 &&
              manual[0].id == viaFacade.items[0].id &&
              manual[0].balance.value == viaFacade.items[0].balance.value,
              "字段级完全一致");
    }

    std::cout << "\n结果: " << g_passed << " passed, " << g_failed << " failed\n";
    DBMW::shutdown();
    return g_failed == 0 ? 0 : 1;
}
