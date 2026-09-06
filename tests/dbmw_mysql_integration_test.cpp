#include "dbmw/async/dbmw_async.h"
#include "dbmw/dbmw.h"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {
using dbmw::common::ErrorCode;
using dbmw::common::Params;
using dbmw::common::ResultSet;
using dbmw::common::Status;
using dbmw::common::Value;

int checks = 0;
void require(bool condition, const std::string &message) {
    ++checks;
    if (!condition) throw std::runtime_error(message);
}
void requireOk(const Status &status, const std::string &where) {
    require(status.ok(), where + " failed: [" + dbmw::common::errorCodeToString(status.code)
                         + "] " + status.message + " sqlstate=" + status.sqlState);
}
std::string env(const char *name, const std::string &fallback = {}) {
    const char *value = std::getenv(name);
    return value && *value ? value : fallback;
}
std::string quoteJson(const std::string &value) {
    std::string out;
    for (const char c: value) {
        if (c == '\\' || c == '"') out.push_back('\\');
        out.push_back(c);
    }
    return out;
}
std::int64_t asInt(const Value &v) { return std::get<std::int64_t>(v); }

struct Fixture {
    std::string table;
    std::string configPath;
    bool initialized = false;
    Fixture() {
        const auto stamp = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        table = "dbmw_it_" + std::to_string(static_cast<unsigned long long>(stamp));
        configPath = "/tmp/" + table + ".json";
    }
    ~Fixture() {
        if (initialized) {
            std::int64_t affected = 0;
            (void)dbmw::DBMW::execute("DROP TABLE IF EXISTS " + table, affected);
            dbmw::DBMW::shutdown(std::chrono::milliseconds(3000));
        }
        std::remove(configPath.c_str());
    }
    void start() {
        const auto host = env("DBMW_TEST_MYSQL_HOST", "127.0.0.1");
        const auto port = env("DBMW_TEST_MYSQL_PORT", "3306");
        const auto user = env("DBMW_TEST_MYSQL_USER", "root");
        const auto database = env("DBMW_TEST_MYSQL_DATABASE", "dbmw_test");
        require(!env("DBMW_TEST_MYSQL_PASSWORD").empty(),
                "DBMW_TEST_MYSQL_PASSWORD must be set");
        std::ofstream file(configPath);
        file << "{\"default_datasource\":\"mysql\","
                "\"pool\":{\"min\":0,\"max\":4,\"borrow_timeout_ms\":2000},"
                "\"prepared_cache\":{\"enabled\":true,\"max_per_connection\":1},"
                "\"cursor\":{\"enabled\":true,\"default_batch_size\":2,"
                "\"max_open_cursors\":1},"
                "\"observability\":{\"slow_sql\":{\"enabled\":true,"
                "\"threshold_ms\":0,\"aggregate_capacity\":100,"
                "\"recent_capacity\":100},\"pool_metrics\":{\"enabled\":true}},"
                "\"async\":{\"enabled\":true,\"threads\":2,\"queue_size\":64},"
                "\"datasources\":[{\"name\":\"mysql\",\"type\":\"mysql\","
                "\"host\":\"" << quoteJson(host) << "\",\"port\":" << port
             << ",\"user\":\"" << quoteJson(user)
             << "\",\"password_env\":\"DBMW_TEST_MYSQL_PASSWORD\","
                "\"database\":\"" << quoteJson(database)
             << "\",\"connection_timeout_ms\":5000}]}";
        file.close();
        requireOk(dbmw::DBMW::init(configPath), "init");
        initialized = true;
        std::int64_t affected = 0;
        requireOk(dbmw::DBMW::execute(
            "CREATE TABLE " + table + " (id BIGINT AUTO_INCREMENT PRIMARY KEY, "
            "name VARCHAR(100) NOT NULL UNIQUE, qty BIGINT NOT NULL, unsigned_value BIGINT "
            "UNSIGNED NOT NULL, amount DECIMAL(30,9), due_date DATE, local_time TIME(6), "
            "metadata JSON, payload BLOB, created_at DATETIME(6)) ENGINE=InnoDB", affected),
            "create table");
    }
};

void testTypesAndKeys(Fixture &f) {
    auto ds = dbmw::DBMW::dataSource();
    require(ds != nullptr, "default datasource missing");
    dbmw::common::GeneratedKeys keys;
    std::int64_t affected = 0;
    const dbmw::common::Blob blob{0, 1, 127, 128, 255};
    requireOk(ds->execute(
        "INSERT INTO " + f.table + " (name,qty,unsigned_value,amount,due_date,local_time,"
        "metadata,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
        Params{std::string("alpha"), std::int64_t(7),
               std::uint64_t{18446744073709551615ULL},
               dbmw::common::Decimal{"123456789012345678901.123456789"},
               dbmw::common::Date{"2026-09-06"}, dbmw::common::Time{"11:50:00.123456"},
               dbmw::common::Json{"{\"ok\":true}"}, blob,
               dbmw::common::Timestamp{std::chrono::system_clock::now()}},
        affected, keys), "insert with generated key");
    require(affected == 1 && keys.lastInsertId() > 0, "generated key missing");

    ResultSet rows;
    requireOk(dbmw::DBMW::query(
        "SELECT qty,unsigned_value,amount,due_date,local_time,metadata,payload,created_at "
        "FROM " + f.table + " WHERE id=?", Params{keys.lastInsertId()}, rows), "type query");
    require(rows.rowCount() == 1, "type query row count");
    const auto &row = rows.rows().front();
    require(asInt(row.at("qty")) == 7, "signed bigint mismatch");
    require(std::get<std::uint64_t>(row.at("unsigned_value")) ==
                18446744073709551615ULL, "unsigned bigint mismatch");
    require(std::get<dbmw::common::Decimal>(row.at("amount")).value ==
                "123456789012345678901.123456789", "decimal precision lost");
    require(std::get<dbmw::common::Date>(row.at("due_date")).value == "2026-09-06",
            "date mismatch");
    require(std::get<dbmw::common::Time>(row.at("local_time")).value == "11:50:00.123456",
            "time mismatch");
    require(std::holds_alternative<dbmw::common::Json>(row.at("metadata")), "json type lost");
    require(std::get<dbmw::common::Blob>(row.at("payload")) == blob, "blob mismatch");
    require(std::holds_alternative<dbmw::common::Timestamp>(row.at("created_at")),
            "datetime type lost");
}

void testTransactionsBatchCursorAndAsync(Fixture &f) {
    dbmw::common::ParamBatch batch{{std::string("b1"), std::int64_t(1)},
                                   {std::string("b2"), std::int64_t(2)}};
    dbmw::common::BatchResult batchResult;
    requireOk(dbmw::DBMW::executeBatch(
        "INSERT INTO " + f.table + " (name,qty,unsigned_value,created_at) "
        "VALUES (?,?,1,NOW(6))", batch, batchResult), "batch");
    require(batchResult.totalAffected() == 2, "batch affected mismatch");

    const auto rollback = dbmw::DBMW::transaction([&](dbmw::core::Session &session) {
        std::int64_t affected = 0;
        auto st = session.execute("INSERT INTO " + f.table
            + " (name,qty,unsigned_value,created_at) VALUES ('rollback',1,1,NOW(6))", affected);
        if (!st.ok()) return st;
        return Status::error(ErrorCode::TxError, "intentional rollback");
    });
    require(rollback.code == ErrorCode::TxError, "rollback status mismatch");
    ResultSet rolled;
    requireOk(dbmw::DBMW::query("SELECT COUNT(*) n FROM " + f.table
                                + " WHERE name='rollback'", rolled), "verify rollback");
    require(asInt(rolled.rows()[0].at("n")) == 0, "transaction was committed");

    requireOk(dbmw::DBMW::withSession([&](dbmw::core::Session &session) {
        dbmw::core::PreparedStatementHandle first, second;
        auto st = session.prepare("SELECT id FROM " + f.table + " WHERE name=?",
                                  {std::string()}, first);
        if (!st.ok()) return st;
        st = session.prepare("SELECT qty FROM " + f.table + " WHERE name=?",
                             {std::string()}, second);
        if (!st.ok()) return st;
        ResultSet out;
        const auto evicted = session.executePrepared(first, {std::string("alpha")}, out);
        return evicted.code == ErrorCode::QueryError ? Status::OK()
            : Status::error(ErrorCode::QueryError, "evicted handle unexpectedly executed");
    }), "prepared LRU safety");

    std::uint64_t delivered = 0;
    int callbacks = 0;
    requireOk(dbmw::DBMW::queryEach("SELECT id FROM " + f.table + " ORDER BY id", {},
        [&](const dbmw::common::Row &) { return ++callbacks < 2; }, delivered), "queryEach");
    require(delivered == 2, "queryEach early stop mismatch");

    dbmw::core::CursorOptions options;
    options.batch_size = 2;
    std::unique_ptr<dbmw::core::Cursor> cursor;
    requireOk(dbmw::DBMW::openCursor("SELECT id FROM " + f.table + " ORDER BY id", {},
                                     options, cursor), "open cursor");
    ResultSet streamed;
    while (cursor->hasNext()) requireOk(cursor->fetch(2, streamed), "cursor fetch");
    require(streamed.rowCount() >= 3, "cursor missed rows");
    requireOk(cursor->close(), "cursor close");

    auto future = dbmw::async::query("SELECT COUNT(*) n FROM " + f.table);
    const auto asyncRows = future.get();
    requireOk(asyncRows.status, "async query");
    require(asInt(asyncRows.rows.rows()[0].at("n")) >= 3, "async count mismatch");

    std::int64_t affected = 0;
    const auto duplicate = dbmw::DBMW::execute(
        "INSERT INTO " + f.table + " (name,qty,unsigned_value,created_at) "
        "VALUES ('alpha',1,1,NOW(6))", affected);
    require(duplicate.code == ErrorCode::ConstraintViolation, "duplicate not classified");
    dbmw::core::ConnectionPool::Stats pool;
    require(dbmw::DBMW::poolStats(pool) && pool.borrowRequests > 0, "pool metrics empty");
    require(!dbmw::DBMW::slowSqlStats().empty(), "slow SQL metrics empty");
}
} // namespace

int main() {
    Fixture fixture;
    try {
        fixture.start();
        testTypesAndKeys(fixture);
        testTransactionsBatchCursorAndAsync(fixture);
        std::cout << "MySQL integration test passed (" << checks << " checks)\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "MySQL integration test failed after " << checks
                  << " checks: " << error.what() << '\n';
        return 1;
    }
}
