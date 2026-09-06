#include "dbmw/async/dbmw_async.h"
#include "dbmw/dbmw.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <future>
#include <iostream>
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
std::string jsonEscape(const std::string &value) {
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
        const auto host = env("DBMW_TEST_ODBC_HOST", "127.0.0.1");
        const auto port = env("DBMW_TEST_ODBC_PORT", "1433");
        const auto user = env("DBMW_TEST_ODBC_USER", "sa");
        const auto password = env("DBMW_TEST_ODBC_PASSWORD");
        const auto database = env("DBMW_TEST_ODBC_DATABASE", "master");
        const auto driver = env("DBMW_TEST_ODBC_DRIVER", "FreeTDS");
        require(!password.empty(), "DBMW_TEST_ODBC_PASSWORD must be set");
        const std::string connection = "DRIVER={" + driver + "};SERVER=" + host
            + ";PORT=" + port + ";DATABASE=" + database + ";UID=" + user + ";PWD="
            + password + ";TDS_Version=7.4;";
        std::ofstream file(configPath);
        file << "{\"default_datasource\":\"odbc\","
                "\"pool\":{\"min\":0,\"max\":4,\"borrow_timeout_ms\":3000},"
                "\"prepared_cache\":{\"enabled\":true,\"max_per_connection\":1},"
                "\"cursor\":{\"enabled\":true,\"default_batch_size\":2,"
                "\"max_open_cursors\":1,\"allow_scrollable\":false},"
                "\"observability\":{\"slow_sql\":{\"enabled\":true,"
                "\"threshold_ms\":0,\"aggregate_capacity\":100,"
                "\"recent_capacity\":100},\"pool_metrics\":{\"enabled\":true}},"
                "\"async\":{\"enabled\":true,\"threads\":2,\"queue_size\":64},"
                "\"datasources\":[{\"name\":\"odbc\",\"type\":\"odbc\","
                "\"database\":\"" << jsonEscape(database) << "\","
                "\"connection_timeout_ms\":5000,\"extra\":{"
                "\"connection_string\":\"" << jsonEscape(connection) << "\","
                "\"savepoint_style\":\"sqlserver\"}}]}";
        file.close();
        requireOk(dbmw::DBMW::init(configPath), "init");
        initialized = true;
        std::int64_t affected = 0;
        requireOk(dbmw::DBMW::execute(
            "CREATE TABLE " + table + " (id BIGINT IDENTITY(1,1) PRIMARY KEY,"
            "name VARCHAR(100) NOT NULL UNIQUE,qty BIGINT NOT NULL,amount DECIMAL(30,9),"
            "due_date DATE,local_time TIME(6),external_id UNIQUEIDENTIFIER,"
            "payload VARBINARY(MAX),created_at DATETIME2(6))", affected), "create table");
    }
};

void testTypesKeysAndErrors(Fixture &f) {
    auto ds = dbmw::DBMW::dataSource();
    require(ds != nullptr, "default datasource missing");
    dbmw::common::GeneratedKeys keys;
    std::int64_t affected = 0;
    const dbmw::common::Blob blob{0, 1, 127, 128, 255};
    requireOk(ds->execute(
        "INSERT INTO " + f.table + " (name,qty,amount,due_date,local_time,external_id,"
        "payload,created_at) OUTPUT INSERTED.id VALUES (?,?,?,?,?,?,?,?)",
        Params{std::string("alpha"), std::int64_t(7),
               dbmw::common::Decimal{"123456789012345678901.123456789"},
               dbmw::common::Date{"2026-09-06"}, dbmw::common::Time{"11:50:00.123456"},
               dbmw::common::Uuid{"550e8400-e29b-41d4-a716-446655440000"}, blob,
               dbmw::common::Timestamp{std::chrono::system_clock::now()}},
        affected, keys), "insert output key");
    require(!keys.empty() && keys.lastInsertId() > 0, "OUTPUT generated key missing");

    ResultSet rows;
    requireOk(dbmw::DBMW::query(
        "SELECT qty,amount,due_date,local_time,external_id,payload,created_at FROM "
        + f.table + " WHERE id=?", Params{keys.lastInsertId()}, rows), "type query");
    require(rows.rowCount() == 1 && asInt(rows.rows()[0].at("qty")) == 7,
            "bigint round trip failed");
    const auto &row = rows.rows()[0];
    require(std::get<dbmw::common::Decimal>(row.at("amount")).value ==
                "123456789012345678901.123456789", "decimal precision lost");
    require(std::get<dbmw::common::Date>(row.at("due_date")).value == "2026-09-06",
            "date round trip failed");
    require(std::get<dbmw::common::Time>(row.at("local_time")).value.find("11:50:00.123456") == 0,
            "time round trip failed");
    require(std::holds_alternative<dbmw::common::Uuid>(row.at("external_id")),
            "GUID type lost");
    require(std::get<dbmw::common::Blob>(row.at("payload")) == blob, "binary mismatch");
    require(std::holds_alternative<dbmw::common::Timestamp>(row.at("created_at")),
            "datetime2 type lost");

    const auto duplicate = dbmw::DBMW::execute(
        "INSERT INTO " + f.table + " (name,qty) VALUES ('alpha',1)", affected);
    require(duplicate.code == ErrorCode::ConstraintViolation,
            "unique violation not classified: " + duplicate.sqlState);
}

void testTransactionsPreparedBatchCursorAsync(Fixture &f) {
    dbmw::common::ParamBatch batch{{std::string("b1"), std::int64_t(1)},
                                   {std::string("b2"), std::int64_t(2)}};
    dbmw::common::BatchResult result;
    requireOk(dbmw::DBMW::executeBatch(
        "INSERT INTO " + f.table + " (name,qty) VALUES (?,?)", batch, result), "batch");
    require(result.totalAffected() == 2, "batch affected mismatch");

    const auto rolled = dbmw::DBMW::transaction([&](dbmw::core::Session &session) {
        std::int64_t affected = 0;
        auto st = session.execute("INSERT INTO " + f.table
                                  + " (name,qty) VALUES ('rollback',1)", affected);
        if (!st.ok()) return st;
        return Status::error(ErrorCode::TxError, "intentional rollback");
    });
    require(rolled.code == ErrorCode::TxError, "transaction status mismatch");
    ResultSet count;
    requireOk(dbmw::DBMW::query("SELECT COUNT(*) n FROM " + f.table
                                + " WHERE name='rollback'", count), "verify rollback");
    require(asInt(count.rows()[0].at("n")) == 0, "rollback row persisted");

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

    const auto asyncRows = dbmw::async::query("SELECT COUNT(*) n FROM " + f.table).get();
    requireOk(asyncRows.status, "async query");
    require(asInt(asyncRows.rows.rows()[0].at("n")) >= 3, "async count mismatch");
    dbmw::core::ConnectionPool::Stats pool;
    require(dbmw::DBMW::poolStats(pool) && pool.borrowRequests > 0, "pool metrics empty");
    require(!dbmw::DBMW::slowSqlStats().empty(), "slow SQL metrics empty");
}
} // namespace

int main() {
    Fixture fixture;
    try {
        fixture.start();
        testTypesKeysAndErrors(fixture);
        testTransactionsPreparedBatchCursorAsync(fixture);
        std::cout << "ODBC SQL Server integration test passed (" << checks << " checks)\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "ODBC SQL Server integration test failed after " << checks
                  << " checks: " << error.what() << '\n';
        return 1;
    }
}
