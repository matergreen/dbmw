#include "dbmw/async/dbmw_async.h"
#include "dbmw/dbmw.h"

#include <atomic>
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
#include <thread>

namespace {

using dbmw::common::ErrorCode;
using dbmw::common::Params;
using dbmw::common::ResultSet;
using dbmw::common::Status;
using dbmw::common::Value;

int gChecks = 0;

void require(bool condition, const std::string &message) {
    ++gChecks;
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
    std::ostringstream out;
    for (const unsigned char c : value) {
        switch (c) {
            case '\\': out << "\\\\"; break;
            case '"': out << "\\\""; break;
            case '\n': out << "\\n"; break;
            case '\r': out << "\\r"; break;
            case '\t': out << "\\t"; break;
            default:
                if (c < 0x20) {
                    const char *hex = "0123456789abcdef";
                    out << "\\u00" << hex[c >> 4] << hex[c & 0x0f];
                } else {
                    out << static_cast<char>(c);
                }
        }
    }
    return out.str();
}

std::int64_t asInt(const Value &value) {
    if (const auto *v = std::get_if<std::int64_t>(&value)) return *v;
    throw std::runtime_error("expected int64 result value");
}

const std::string &asString(const Value &value) {
    if (const auto *v = std::get_if<std::string>(&value)) return *v;
    throw std::runtime_error("expected string result value");
}

bool existsByName(const std::string &table, const std::string &name) {
    ResultSet rows;
    requireOk(dbmw::DBMW::query("SELECT count(*) AS n FROM " + table + " WHERE name = ?",
                                Params{std::string(name)}, rows),
              "count by name");
    return asInt(rows.rows().at(0).at("n")) != 0;
}

struct Fixture {
    std::string schema;
    std::string table;
    std::string configPath;
    bool initialized = false;

    Fixture() {
        const auto stamp = std::chrono::high_resolution_clock::now().time_since_epoch().count();
        schema = "dbmw_it_" + std::to_string(static_cast<unsigned long long>(stamp));
        table = schema + ".items";
        configPath = "/tmp/" + schema + ".json";
    }

    ~Fixture() {
        if (initialized) {
            std::int64_t affected = 0;
            (void)dbmw::DBMW::execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE", affected);
            dbmw::DBMW::shutdown(std::chrono::milliseconds(3000));
        }
        std::remove(configPath.c_str());
    }

    void start() {
        const std::string host = env("DBMW_TEST_PG_HOST", "127.0.0.1");
        const std::string port = env("DBMW_TEST_PG_PORT", "5432");
        const std::string user = env("DBMW_TEST_PG_USER", "postgres");
        const std::string database = env("DBMW_TEST_PG_DATABASE", "postgres");
        require(!env("DBMW_TEST_PG_PASSWORD").empty(),
                "DBMW_TEST_PG_PASSWORD must be set for the integration test");

        auto dataSource = [&](const std::string &name, int maxRows) {
            std::ostringstream out;
            out << "{\"name\":\"" << name << "\",\"type\":\"postgres\",";
            out << "\"host\":\"" << jsonEscape(host) << "\",\"port\":" << port << ',';
            out << "\"user\":\"" << jsonEscape(user) << "\",";
            out << "\"password_env\":\"DBMW_TEST_PG_PASSWORD\",";
            out << "\"database\":\"" << jsonEscape(database) << "\",";
            out << "\"connection_timeout_ms\":3000,\"query_timeout_ms\":0,";
            out << "\"max_result_rows\":" << maxRows << '}';
            return out.str();
        };

        std::ofstream file(configPath);
        require(static_cast<bool>(file), "cannot create temporary integration config");
        file << "{\n"
             << "\"default_datasource\":\"pg\",\n"
             << "\"heartbeat_interval_ms\":1000,\n"
             << "\"pool\":{\"enabled\":true,\"min\":0,\"max\":4,"
                "\"borrow_timeout_ms\":1000,\"validation_interval_ms\":0},\n"
             << "\"retry\":{\"max_attempts\":1,\"retry_writes\":false},\n"
             << "\"circuit_breaker\":{\"failure_threshold\":0},\n"
             << "\"prepared_cache\":{\"enabled\":true,\"max_per_connection\":8},\n"
             << "\"cursor\":{\"enabled\":true,\"default_batch_size\":2,"
                "\"max_open_cursors\":1,\"allow_scrollable\":false},\n"
             << "\"query_cache\":{\"enabled\":true,\"ttl_ms\":60000,"
                "\"max_entries\":100},\n"
             << "\"observability\":{\"sql_log\":{\"enabled\":false},"
                "\"slow_sql\":{\"enabled\":true,\"threshold_ms\":0,"
                "\"aggregate_capacity\":100,\"recent_capacity\":100,"
                "\"retain_rendered_sql\":false},"
                "\"pool_metrics\":{\"enabled\":true}},\n"
             << "\"async\":{\"enabled\":true,\"threads\":2,\"queue_size\":64},\n"
             << "\"datasources\":[" << dataSource("pg", 0) << ','
             << dataSource("limited", 2) << "],\n\"groups\":[]\n}\n";
        file.close();

        requireOk(dbmw::DBMW::init(configPath), "DBMW::init");
        initialized = true;
        std::int64_t affected = 0;
        requireOk(dbmw::DBMW::execute("CREATE SCHEMA " + schema, affected), "create schema");
        requireOk(dbmw::DBMW::execute(
            "CREATE TABLE " + table + " ("
            "id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE, qty BIGINT NOT NULL, "
            "price DOUBLE PRECISION NOT NULL, active BOOLEAN NOT NULL, payload BYTEA, "
            "created_at TIMESTAMPTZ NOT NULL)", affected), "create table");
    }
};

void testConnectivityAndTypes(Fixture &f) {
    ResultSet version;
    requireOk(dbmw::DBMW::query("SELECT current_database() AS db, version() AS version", version),
              "server identity query");
    require(version.rowCount() == 1, "server identity query returned wrong row count");
    require(!asString(version.rows()[0].at("version")).empty(), "PostgreSQL version is empty");

    auto ds = dbmw::DBMW::dataSource();
    require(ds != nullptr, "default datasource is missing");
    const dbmw::common::Timestamp timestamp = std::chrono::system_clock::now();
    const dbmw::common::Blob blob{0x00, 0x01, 0x7f, 0x80, 0xff};
    dbmw::common::GeneratedKeys keys;
    std::int64_t affected = 0;
    requireOk(ds->execute(
        "INSERT INTO " + f.table
        + " (name, qty, price, active, payload, created_at) VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
        Params{std::string("alpha"), std::int64_t(7), 12.5, true, blob, timestamp},
        affected, keys), "insert with RETURNING");
    require(affected == 1 && keys.lastInsertId() > 0, "generated key or affected rows is wrong");

    ResultSet rows;
    requireOk(dbmw::DBMW::query(
        "SELECT name, qty, price, active, payload, created_at FROM " + f.table + " WHERE id = ?",
        Params{keys.lastInsertId()}, rows), "type round trip");
    require(rows.rowCount() == 1, "type round trip returned wrong row count");
    const auto &row = rows.rows()[0];
    require(asString(row.at("name")) == "alpha", "text round trip failed");
    require(asInt(row.at("qty")) == 7, "bigint round trip failed");
    require(std::abs(std::get<double>(row.at("price")) - 12.5) < 0.0001,
            "double round trip failed");
    require(std::get<bool>(row.at("active")), "boolean round trip failed");
    require(std::get<dbmw::common::Blob>(row.at("payload")) == blob, "bytea round trip failed");
    require(std::holds_alternative<dbmw::common::Timestamp>(row.at("created_at")),
            "timestamptz was not mapped to Timestamp");

    ResultSet nullRow;
    requireOk(dbmw::DBMW::query("SELECT ?::text AS value", Params{nullptr}, nullRow),
              "NULL binding");
    require(std::holds_alternative<std::nullptr_t>(nullRow.rows()[0].at("value")),
            "NULL binding round trip failed");
}

void testBatchPreparedAndStreaming(Fixture &f) {
    dbmw::common::ParamBatch batch{
        Params{std::string("batch-1"), std::int64_t(1)},
        Params{std::string("batch-2"), std::int64_t(2)},
        Params{std::string("batch-3"), std::int64_t(3)}};
    dbmw::common::BatchResult result;
    requireOk(dbmw::DBMW::executeBatch(
        "INSERT INTO " + f.table
        + " (name, qty, price, active, created_at) VALUES (?, ?, 1.0, true, now())",
        batch, result), "batch insert");
    require(result.affected.size() == 3 && result.totalAffected() == 3,
            "batch affected rows are wrong");

    requireOk(dbmw::DBMW::withSession([&](dbmw::core::Session &session) {
        dbmw::core::PreparedStatementHandle prepared;
        auto st = session.prepare("SELECT qty FROM " + f.table + " WHERE name = ?",
                                  Params{std::string("sample")}, prepared);
        if (!st.ok()) return st;
        if (!prepared.valid()) return Status::error(ErrorCode::QueryError, "invalid prepared handle");
        for (int i = 1; i <= 3; ++i) {
            ResultSet rows;
            st = session.executePrepared(prepared, Params{std::string("batch-") + std::to_string(i)}, rows);
            if (!st.ok()) return st;
            if (rows.rowCount() != 1 || asInt(rows.rows()[0].at("qty")) != i)
                return Status::error(ErrorCode::QueryError, "prepared result mismatch");
        }
        return Status::OK();
    }), "explicit prepared statement");

    std::uint64_t streamedRows = 0;
    int callbacks = 0;
    requireOk(dbmw::DBMW::queryEach(
        "SELECT id FROM " + f.table + " ORDER BY id", {},
        [&](const dbmw::common::Row &) { return ++callbacks < 2; }, streamedRows),
        "queryEach early stop");
    require(callbacks == 2 && streamedRows == 2, "queryEach early stop count is wrong");
}

void testTransactions(Fixture &f) {
    requireOk(dbmw::DBMW::transaction([&](dbmw::core::Session &session) {
        std::int64_t affected = 0;
        auto st = session.execute(
            "INSERT INTO " + f.table
            + " (name, qty, price, active, created_at) VALUES ('committed', 1, 1, true, now())",
            affected);
        if (!st.ok()) return st;
        st = session.savepoint("before_temp");
        if (!st.ok()) return st;
        st = session.execute(
            "INSERT INTO " + f.table
            + " (name, qty, price, active, created_at) VALUES ('savepoint-temp', 1, 1, true, now())",
            affected);
        if (!st.ok()) return st;
        st = session.rollbackToSavepoint("before_temp");
        if (!st.ok()) return st;
        return session.releaseSavepoint("before_temp");
    }), "transaction commit/savepoint");
    require(existsByName(f.table, "committed"), "committed row is missing");
    require(!existsByName(f.table, "savepoint-temp"), "savepoint rollback did not roll back row");

    const Status rollback = dbmw::DBMW::transaction([&](dbmw::core::Session &session) {
        std::int64_t affected = 0;
        const auto st = session.execute(
            "INSERT INTO " + f.table
            + " (name, qty, price, active, created_at) VALUES ('rolled-back', 1, 1, true, now())",
            affected);
        if (!st.ok()) return st;
        return Status::error(ErrorCode::TxError, "intentional rollback");
    });
    require(rollback.code == ErrorCode::TxError, "transaction did not propagate callback error");
    require(!existsByName(f.table, "rolled-back"), "failed transaction was committed");

    dbmw::common::TransactionOptions readOnly;
    readOnly.readOnly = true;
    requireOk(dbmw::DBMW::transaction(readOnly, [&](dbmw::core::Session &session) {
        ResultSet rows;
        return session.query("SELECT count(*) AS n FROM " + f.table, rows);
    }), "read-only transaction");

    dbmw::common::TransactionOptions timeout;
    timeout.timeout = std::chrono::milliseconds(100);
    const auto started = std::chrono::steady_clock::now();
    const Status timed = dbmw::DBMW::transaction(timeout, [](dbmw::core::Session &session) {
        ResultSet rows;
        return session.query("SELECT pg_sleep(2)", rows);
    });
    require(timed.code == ErrorCode::QueryTimeout,
            "transaction timeout was not classified as QueryTimeout: " + timed.message);
    require(std::chrono::steady_clock::now() - started < std::chrono::seconds(2),
            "transaction timeout did not interrupt pg_sleep promptly");
}

void testErrorsLimitsAndCursor(Fixture &f) {
    std::int64_t affected = 0;
    const Status duplicate = dbmw::DBMW::execute(
        "INSERT INTO " + f.table
        + " (name, qty, price, active, created_at) VALUES (?, 1, 1, true, now())",
        Params{std::string("alpha")}, affected);
    require(duplicate.code == ErrorCode::ConstraintViolation,
            "unique violation was not classified as ConstraintViolation");
    require(duplicate.sqlState == "23505", "unique violation SQLSTATE was not preserved");

    ResultSet limited;
    const Status limit = dbmw::DBMW::query(
        "limited", "SELECT id FROM " + f.table + " ORDER BY id", {}, limited);
    require(limit.code == ErrorCode::QueryError, "max_result_rows did not reject oversized result");

    dbmw::core::CursorOptions options;
    options.batch_size = 2;
    std::unique_ptr<dbmw::core::Cursor> cursor;
    requireOk(dbmw::DBMW::openCursor("SELECT id FROM " + f.table + " ORDER BY id", {}, options, cursor),
              "open cursor");
    std::unique_ptr<dbmw::core::Cursor> second;
    const Status cursorLimit = dbmw::DBMW::openCursor(
        "SELECT id FROM " + f.table + " ORDER BY id", {}, options, second);
    require(cursorLimit.code == ErrorCode::CursorLimit, "max_open_cursors was not enforced");

    ResultSet streamed;
    while (cursor->isOpen()) requireOk(cursor->fetch(2, streamed), "cursor fetch");
    require(streamed.rowCount() >= 5, "cursor did not stream all rows");
    requireOk(cursor->close(), "cursor close");
    dbmw::core::ConnectionPool::Stats stats;
    require(dbmw::DBMW::poolStats(stats), "pool stats unavailable");
    require(stats.borrowed == 0, "cursor close did not immediately return its connection");

    std::unique_ptr<dbmw::core::Cursor> reopened;
    requireOk(dbmw::DBMW::openCursor(
        "SELECT id FROM " + f.table + " ORDER BY id", {}, options, reopened),
        "reopen cursor after close");
    requireOk(reopened->close(), "close reopened cursor");
}

void testCacheAsyncAndObservability(Fixture &f) {
    ResultSet before;
    requireOk(dbmw::DBMW::query("SELECT qty FROM " + f.table + " WHERE name = ?",
                                Params{std::string("alpha")}, before), "cached read before write");
    std::int64_t affected = 0;
    requireOk(dbmw::DBMW::execute("UPDATE " + f.table + " SET qty = ? WHERE name = ?",
                                  Params{std::int64_t(99), std::string("alpha")}, affected),
              "cache invalidating write");
    ResultSet after;
    requireOk(dbmw::DBMW::query("SELECT qty FROM " + f.table + " WHERE name = ?",
                                Params{std::string("alpha")}, after), "cached read after write");
    require(asInt(after.rows()[0].at("qty")) == 99, "write did not invalidate query cache");

    auto future = dbmw::async::query("SELECT count(*) AS n FROM " + f.table);
    auto asyncRows = future.get();
    requireOk(asyncRows.status, "async future query");
    require(asInt(asyncRows.rows.rows()[0].at("n")) >= 5, "async query returned wrong count");

    std::promise<dbmw::async::QueryResult> completion;
    dbmw::async::Options options;
    options.timeout = std::chrono::milliseconds(100);
    const auto started = std::chrono::steady_clock::now();
    auto handle = dbmw::async::query(
        "SELECT pg_sleep(2)",
        [&](dbmw::async::QueryResult &&result) { completion.set_value(std::move(result)); },
        options);
    require(handle.valid(), "async timeout handle is invalid");
    auto timed = completion.get_future().get();
    const auto elapsed = std::chrono::steady_clock::now() - started;
    require(timed.status.code == ErrorCode::QueryTimeout,
            "async pg_sleep was not classified as QueryTimeout: " + timed.status.message);
    require(elapsed < std::chrono::seconds(2), "async cancellation did not interrupt pg_sleep promptly");

    dbmw::core::ConnectionPool::Stats stats;
    require(dbmw::DBMW::poolStats(stats), "pool stats unavailable after async test");
    require(stats.borrowRequests > 0 && stats.borrowSuccesses > 0 && stats.connectionsCreated > 0,
            "pool counters were not populated");
    require(!dbmw::DBMW::slowSqlStats(100).empty(), "slow SQL aggregates are empty");
    require(!dbmw::DBMW::recentSlowSql(100).empty(), "recent slow SQL records are empty");
}

} // namespace

int main() {
    Fixture fixture;
    try {
        fixture.start();
        testConnectivityAndTypes(fixture);
        testBatchPreparedAndStreaming(fixture);
        testTransactions(fixture);
        testErrorsLimitsAndCursor(fixture);
        testCacheAsyncAndObservability(fixture);
        std::cout << "PostgreSQL integration test passed (" << gChecks << " checks)\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "PostgreSQL integration test failed after " << gChecks
                  << " checks: " << error.what() << '\n';
        return 1;
    }
}
