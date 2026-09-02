#include "dbmw/driver/mysql_driver.h"
#include "dbmw/driver/driver_registry.h"

#include <cstring>
#include <algorithm>
#include <string>
#include <memory>
#include <utility>
#include <vector>

#ifdef DBMW_ENABLE_MYSQL
#include <mysql/mysql.h>
#endif


namespace dbmw::driver {
    namespace {
        bool validSavepointName(const std::string &name) {
            if (name.empty() || !(std::isalpha(static_cast<unsigned char>(name[0])) ||
                                  name[0] == '_')) return false;
            return std::all_of(name.begin() + 1, name.end(), [](const char c) {
                return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
            });
        }

        common::Status notConnected(const char *where) {
            return common::Status::error(common::ErrorCode::NotConnected,
                                         std::string("MySQL: not connected (") + where + ")");
        }

#ifdef DBMW_ENABLE_MYSQL
        // 结果列的首取缓冲大小；超出的列会按真实长度二次读取。
        constexpr std::size_t kInitialColBytes = 4096;

        // 语句句柄的 RAII 包装，保证任何退出路径都释放。
        class StmtGuard {
        public:
            explicit StmtGuard(MYSQL_STMT *s = nullptr) : s_(s) {}

            ~StmtGuard() { if (s_) mysql_stmt_close(s_); }

            StmtGuard(const StmtGuard &) = delete;

            StmtGuard &operator=(const StmtGuard &) = delete;

            StmtGuard(StmtGuard &&other) noexcept : s_(other.s_) { other.s_ = nullptr; }

            StmtGuard &operator=(StmtGuard &&other) noexcept {
                if (this != &other) {
                    if (s_) mysql_stmt_close(s_);
                    s_ = other.s_;
                    other.s_ = nullptr;
                }
                return *this;
            }

            MYSQL_STMT *get() const { return s_; }

        private:
            MYSQL_STMT *s_;
        };

        class ActiveMysqlOperation {
        public:
            ActiveMysqlOperation(std::mutex &mutex, unsigned long &slot,
                                 const unsigned long threadId)
                : mutex_(mutex), slot_(slot), threadId_(threadId) {
                std::lock_guard<std::mutex> lock(mutex_);
                slot_ = threadId_;
            }
            ~ActiveMysqlOperation() {
                std::lock_guard<std::mutex> lock(mutex_);
                if (slot_ == threadId_) slot_ = 0;
            }
            ActiveMysqlOperation(const ActiveMysqlOperation &) = delete;
            ActiveMysqlOperation &operator=(const ActiveMysqlOperation &) = delete;

        private:
            std::mutex &mutex_;
            unsigned long &slot_;
            unsigned long threadId_;
        };

        // 绑定参数的后备存储。MYSQL_BIND 只存指针，
        // 因此真正的缓冲区必须活到 mysql_stmt_execute 返回之后。
        struct ParamStorage {
            std::vector<MYSQL_BIND> bind;
            std::vector<std::vector<char> > strBuf;
            MysqlBoolArray isNull;
            std::vector<long long> intBuf;
            std::vector<double> dblBuf;
            std::vector<unsigned long> len;
        };

        void setStringParam(ParamStorage &st, std::size_t i, const char *data, std::size_t size) {
            st.strBuf[i].assign(data, data + size);
            // 空串也要保证 buffer 是合法地址；真实长度仍为 0。
            if (st.strBuf[i].empty()) st.strBuf[i].push_back('\0');
            st.len[i] = static_cast<unsigned long>(size);
            st.bind[i].buffer = st.strBuf[i].data();
            st.bind[i].buffer_length = st.len[i];
        }

        // 将 MySQL 列类型转换为 dbmw 的通用 Value。
        // 数值/小数尽量解析为 int64/double，日期时间解析为 Timestamp，
        // 二进制列为 Blob，其余一律作为字符串，NULL 由调用方处理。
        common::Value fieldToValue(enum_field_types type, const char *data, unsigned long len) {
            using common::Value;
            switch (type) {
                case MYSQL_TYPE_TINY:
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_INT24:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_LONGLONG: {
                    try { return Value{std::stoll(std::string(data, len))}; } catch (...) {
                        return Value{std::string(data, len)};
                    }
                }
                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL: {
                    try { return Value{std::stod(std::string(data, len))}; } catch (...) {
                        return Value{std::string(data, len)};
                    }
                }
                case MYSQL_TYPE_DATE:
                case MYSQL_TYPE_NEWDATE:
                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_TIMESTAMP: {
                    const std::string s(data, len);
                    common::Timestamp ts{};
                    if (common::tryParseTimestamp(s, ts)) return Value{ts};
                    return Value{s}; // 零日期等无法解析的值退化为字符串，不丢数据
                }
                case MYSQL_TYPE_TINY_BLOB:
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_LONG_BLOB: {
                    common::Blob b(len);
                    if (len > 0) std::memcpy(b.data(), data, len);
                    return Value{std::move(b)};
                }
                default:
                    return Value{std::string(data, len)};
            }
        }
#endif
    } // namespace

#ifdef DBMW_ENABLE_MYSQL
    // 结果集行数上限。query() 会把整个结果集物化进内存，
    // 一条漏写 LIMIT 的查询就足以吃爆进程。超限时直接失败并提示改用
    // queryEach() 流式消费。limit <= 0 表示不限制。
    //
    // queryEach 不受此约束：它本就逐行交付给回调，不会全量驻留内存。
    //
    // 位置要求：必须定义在首个使用点 MySQLConnection::query() 之前。此前它位于
    // 文件后半段的匿名命名空间里（晚于 query()），导致 "not declared in this scope"。
    // 放在驱动宏内，避免禁用 MySQL 时触发 unused-function 警告。
    namespace {
        common::Status rowLimitExceeded(std::uint64_t rows, int limit) {
            if (limit <= 0 || rows <= static_cast<std::uint64_t>(limit))
                return common::Status::OK();
            return common::Status::error(
                common::ErrorCode::QueryError,
                "result set exceeded max_result_rows (" + std::to_string(rows)
                + " > " + std::to_string(limit)
                + "); use queryEach() to stream the result instead");
        }
    } // namespace
#endif

    // ---------------------------------------------------------------------------
    common::Status MySQLConnection::connect(const config::DataSourceConfig &cfg) {
        cfg_ = cfg;
#ifdef DBMW_ENABLE_MYSQL
        close(); // 清理可能的残留句柄

        m_ = mysql_init(nullptr);
        if (!m_) {
            return common::Status::error(common::ErrorCode::ConnectionFailed,
                                         "MySQL: mysql_init failed (out of memory)");
        }

        // 连接超时（秒）
        if (cfg.connection_timeout_ms > 0) {
            unsigned int t = static_cast<unsigned int>(cfg.connection_timeout_ms / 1000);
            mysql_options(m_, MYSQL_OPT_CONNECT_TIMEOUT, &t);
        }
        // 读写超时（秒）；0 表示沿用服务端默认值
        const int ioTimeoutMs = cfg.query_timeout_ms > 0
            ? cfg.query_timeout_ms : cfg.socket_timeout_ms;
        if (ioTimeoutMs > 0) {
            unsigned int t = static_cast<unsigned int>(std::max(1, (ioTimeoutMs + 999) / 1000));
            mysql_options(m_, MYSQL_OPT_READ_TIMEOUT, &t);
            mysql_options(m_, MYSQL_OPT_WRITE_TIMEOUT, &t);
        }
        // 字符集（extra["charset"]，如 utf8mb4）
        auto it = cfg.extra.find("charset");
        if (it != cfg.extra.end()) {
            mysql_options(m_, MYSQL_SET_CHARSET_NAME, it->second.c_str());
        }
        if (cfg.tls_enabled) {
            mysql_ssl_set(m_,
                          cfg.tls_key.empty() ? nullptr : cfg.tls_key.c_str(),
                          cfg.tls_cert.empty() ? nullptr : cfg.tls_cert.c_str(),
                          cfg.tls_ca.empty() ? nullptr : cfg.tls_ca.c_str(),
                          nullptr, nullptr);
#if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 80000 && !defined(MARIADB_VERSION_ID)
            const mysql_ssl_mode mode = cfg.tls_verify_peer
                ? SSL_MODE_VERIFY_IDENTITY : SSL_MODE_REQUIRED;
            mysql_options(m_, MYSQL_OPT_SSL_MODE, &mode);
#else
            MysqlBool verify = cfg.tls_verify_peer ? 1 : 0;
            mysql_options(m_, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &verify);
#endif
        }

        const char *host = cfg.host.empty() ? nullptr : cfg.host.c_str();
        const char *user = cfg.user.empty() ? nullptr : cfg.user.c_str();
        const char *pass = cfg.password.empty() ? nullptr : cfg.password.c_str();
        const char *db = cfg.database.empty() ? nullptr : cfg.database.c_str();

        if (!mysql_real_connect(m_, host, user, pass, db,
                                cfg.port, nullptr, 0)) {
            std::string err = "MySQL connect failed: ";
            err += mysql_error(m_);
            const auto native = static_cast<std::int64_t>(mysql_errno(m_));
            const std::string state = mysql_sqlstate(m_) ? mysql_sqlstate(m_) : "";
            mysql_close(m_);
            m_ = nullptr;
            open_ = false;
            return common::Status::databaseError(common::ErrorCode::ConnectionFailed,
                                                 cfg.redact(std::move(err)), state, native);
        }

        open_ = true;
        return common::Status::OK();
#else
        open_ = false;
        return common::Status::error(common::ErrorCode::DriverDisabled,
                                     "MySQL driver not built. Rebuild with -DDBMW_ENABLE_MYSQL=ON");
#endif
    }

    common::Status MySQLConnection::ping() {
#ifdef DBMW_ENABLE_MYSQL
        if (!open_ || !m_) return notConnected("ping");
        if (mysql_ping(m_) == 0) return common::Status::OK();
        return lastError("mysql_ping");
#else
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::query(const std::string &sql, common::ResultSet &out) {
#ifdef DBMW_ENABLE_MYSQL
        if (!open_ || !m_) return notConnected("query");
        ActiveMysqlOperation active(operationMtx_, activeThreadId_, mysql_thread_id(m_));
        if (mysql_real_query(m_, sql.data(), sql.size()) != 0)
            return lastError("mysql_real_query");

        // 取结果集。无结果集的语句（DDL/DML）走 execute，这里容错返回 ok。
        std::unique_ptr<MYSQL_RES, void(*)(MYSQL_RES *)> res(
            mysql_store_result(m_),
            [](MYSQL_RES *r) { if (r) mysql_free_result(r); });
        if (!res) {
            if (mysql_field_count(m_) == 0) return common::Status::OK();
            return lastError("mysql_store_result");
        }

        // mysql_store_result 已把整个结果集拉进内存，这里能直接看到总行数：
        // 超限就一行都不转换，尽早止损并提示改用流式接口。
        if (const auto st = rowLimitExceeded(mysql_num_rows(res.get()),
                                             cfg_.max_result_rows); !st.ok())
            return st;

        unsigned int nfields = mysql_num_fields(res.get());
        MYSQL_FIELD *fields = mysql_fetch_fields(res.get());

        // 记录 SELECT 列表中的列顺序（Row 内部是有序 map，本身不带顺序信息）。
        std::vector<std::string> names;
        names.reserve(nfields);
        for (unsigned int i = 0; i < nfields; ++i) names.emplace_back(fields[i].name);
        out.setFields(std::move(names));

        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res.get())) != nullptr) {
            unsigned long *lengths = mysql_fetch_lengths(res.get());
            common::Row r;
            for (unsigned int i = 0; i < nfields; ++i) {
                const char *colName = fields[i].name;
                if (row[i] == nullptr) {
                    r.set(colName, nullptr); // SQL NULL
                    continue;
                }
                r.set(colName, fieldToValue(fields[i].type, row[i], lengths[i]));
            }
            out.addRow(std::move(r));
        }
        return common::Status::OK();
#else
        (void) sql;
        (void) out;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::execute(const std::string &sql, std::int64_t &affected) {
#ifdef DBMW_ENABLE_MYSQL
        affected = 0;
        if (!open_ || !m_) return notConnected("execute");
        ActiveMysqlOperation active(operationMtx_, activeThreadId_, mysql_thread_id(m_));
        if (mysql_real_query(m_, sql.data(), sql.size()) != 0)
            return lastError("mysql_real_query");
        affected = static_cast<std::int64_t>(mysql_affected_rows(m_));
        return common::Status::OK();
#else
        (void) sql;
        affected = 0;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

#ifdef DBMW_ENABLE_MYSQL
    namespace {
        // 绑定参数并执行前的公共部分：init -> prepare -> 参数数量校验 -> bind_param
        // 把 params 绑定到已 prepare 好的 stmt（不负责 init / prepare / 关闭）。
        // 供 executePrepared 复用连接缓存里的 MYSQL_STMT*，也供 prepareAndBind 复用。
        common::Status bindParamsOnly(MYSQL_STMT *stmt, const common::Params &params,
                                      ParamStorage &st) {
            const unsigned long want = mysql_stmt_param_count(stmt);
            if (want != params.size()) {
                return common::Status::error(
                    common::ErrorCode::QueryError,
                    "parameter mismatch: statement expects " + std::to_string(want)
                    + " parameter(s) but " + std::to_string(params.size()) + " supplied");
            }
            if (want == 0) return common::Status::OK();

            const std::size_t n = params.size();
            st.bind.resize(n);
            std::memset(st.bind.data(), 0, n * sizeof(MYSQL_BIND));
            st.strBuf.resize(n);
            st.isNull = std::make_unique<MysqlBool[]>(n);
            st.intBuf.assign(n, 0);
            st.dblBuf.assign(n, 0.0);
            st.len.assign(n, 0);

            for (std::size_t i = 0; i < n; ++i) {
                const auto &v = params[i];
                MYSQL_BIND &b = st.bind[i];
                b.length = &st.len[i];
                b.is_null = &st.isNull[i];

                if (std::holds_alternative<std::nullptr_t>(v)) {
                    b.buffer_type = MYSQL_TYPE_NULL;
                    st.isNull[i] = 1;
                } else if (const auto *x = std::get_if<bool>(&v)) {
                    b.buffer_type = MYSQL_TYPE_LONGLONG;
                    st.intBuf[i] = *x ? 1 : 0;
                    b.buffer = &st.intBuf[i];
                } else if (const auto *x = std::get_if<std::int64_t>(&v)) {
                    b.buffer_type = MYSQL_TYPE_LONGLONG;
                    st.intBuf[i] = *x;
                    b.buffer = &st.intBuf[i];
                } else if (const auto *x = std::get_if<double>(&v)) {
                    b.buffer_type = MYSQL_TYPE_DOUBLE;
                    st.dblBuf[i] = *x;
                    b.buffer = &st.dblBuf[i];
                } else if (const auto *x = std::get_if<common::Timestamp>(&v)) {
                    b.buffer_type = MYSQL_TYPE_STRING;
                    const std::string s = common::timestampToStringMs(*x);
                    setStringParam(st, i, s.data(), s.size());
                } else if (const auto *x = std::get_if<common::Blob>(&v)) {
                    b.buffer_type = MYSQL_TYPE_BLOB;
                    st.strBuf[i].resize(x->size());
                    if (!x->empty()) std::memcpy(st.strBuf[i].data(), x->data(), x->size());
                    if (st.strBuf[i].empty()) st.strBuf[i].push_back('\0');
                    st.len[i] = static_cast<unsigned long>(x->size());
                    b.buffer = st.strBuf[i].data();
                    b.buffer_length = st.len[i];
                } else if (const auto *x = std::get_if<std::string>(&v)) {
                    b.buffer_type = MYSQL_TYPE_STRING;
                    setStringParam(st, i, x->data(), x->size());
                } else {
                    b.buffer_type = MYSQL_TYPE_NULL;
                    st.isNull[i] = 1;
                }
            }

            if (mysql_stmt_bind_param(stmt, st.bind.data()) != 0) {
                return common::Status::error(common::ErrorCode::QueryError,
                                             std::string("mysql_stmt_bind_param: ") + mysql_stmt_error(stmt));
            }
            return common::Status::OK();
        }

        common::Status prepareAndBind(MYSQL *m, const std::string &sql,
                                      const common::Params &params,
                                      StmtGuard &guard, ParamStorage &st) {
            MYSQL_STMT *stmt = mysql_stmt_init(m);
            if (!stmt) {
                return common::Status::error(common::ErrorCode::QueryError,
                                             "MySQL: mysql_stmt_init failed (out of memory)");
            }
            guard = StmtGuard(stmt);

            if (mysql_stmt_prepare(stmt, sql.data(), static_cast<unsigned long>(sql.size())) != 0) {
                return common::Status::error(common::ErrorCode::QueryError,
                                             std::string("mysql_stmt_prepare: ") + mysql_stmt_error(stmt));
            }

            return bindParamsOnly(stmt, params, st);
        }

        common::Status fetchPreparedInternal(MYSQL_STMT *stmt, common::ResultSet *out,
                                             const common::RowCallback *callback,
                                             std::uint64_t *delivered,
                                             int maxRows = 0) {
            std::unique_ptr<MYSQL_RES, void(*)(MYSQL_RES *)> meta(
                mysql_stmt_result_metadata(stmt),
                [](MYSQL_RES *r) { if (r) mysql_free_result(r); });
            if (!meta) return common::Status::OK(); // 无结果集

            const unsigned int nfields = mysql_num_fields(meta.get());
            MYSQL_FIELD *fields = mysql_fetch_fields(meta.get());
            if (nfields == 0) return common::Status::OK();

            std::vector<std::vector<char> > buf(nfields);
            std::vector<unsigned long> len(nfields);
            MysqlBoolArray isNull = std::make_unique<MysqlBool[]>(nfields);
            std::vector<MYSQL_BIND> bind(nfields);
            std::memset(bind.data(), 0, nfields * sizeof(MYSQL_BIND));

            for (unsigned int i = 0; i < nfields; ++i) {
                buf[i].resize(kInitialColBytes);
                // 统一按字符串取出，再依据元数据里的真实类型做转换。
                bind[i].buffer_type = MYSQL_TYPE_STRING;
                bind[i].buffer = buf[i].data();
                bind[i].buffer_length = kInitialColBytes;
                bind[i].length = &len[i];
                bind[i].is_null = &isNull[i];
            }

            if (mysql_stmt_bind_result(stmt, bind.data()) != 0) {
                return common::Status::error(common::ErrorCode::QueryError,
                                             std::string("mysql_stmt_bind_result: ") + mysql_stmt_error(stmt));
            }

            std::vector<std::string> names;
            names.reserve(nfields);
            for (unsigned int i = 0; i < nfields; ++i) names.emplace_back(fields[i].name);
            if (out) out->setFields(names);
            if (delivered) *delivered = 0;
            std::uint64_t fetched = 0;

            for (;;) {
                const int rc = mysql_stmt_fetch(stmt);
                if (rc == MYSQL_NO_DATA) break;
                if (rc != 0 && rc != MYSQL_DATA_TRUNCATED) {
                    return common::Status::error(common::ErrorCode::QueryError,
                                                 std::string("mysql_stmt_fetch: ") + mysql_stmt_error(stmt));
                }

                // 列数据超过首取缓冲：按真实长度重新读取该列。
                if (rc == MYSQL_DATA_TRUNCATED) {
                    for (unsigned int i = 0; i < nfields; ++i) {
                        if (len[i] <= buf[i].size()) continue;
                        buf[i].resize(static_cast<std::size_t>(len[i]) + 1);
                        bind[i].buffer = buf[i].data();
                        bind[i].buffer_length = len[i];
                        MYSQL_BIND one = bind[i];
                        if (mysql_stmt_fetch_column(stmt, &one, i, 0) != 0) {
                            return common::Status::error(
                                common::ErrorCode::QueryError,
                                std::string("mysql_stmt_fetch_column: ") + mysql_stmt_error(stmt));
                        }
                    }
                }

                common::Row r;
                for (unsigned int i = 0; i < nfields; ++i) {
                    const char *colName = fields[i].name;
                    if (isNull[i]) {
                        r.set(colName, nullptr);
                        continue;
                    }
                    r.set(colName, fieldToValue(fields[i].type, buf[i].data(), len[i]));
                }
                if (out) {
                    // 边取边判：prepared 路径逐行 fetch，无法预知总行数，
                    // 因此一旦越过上限立刻止损，不再往 ResultSet 里堆。
                    ++fetched;
                    if (maxRows > 0 && fetched > static_cast<std::uint64_t>(maxRows))
                        return rowLimitExceeded(fetched, maxRows);
                    out->addRow(std::move(r));
                } else if (callback) {
                    if (delivered) ++*delivered;
                    if (*callback && !(*callback)(r)) break;
                }
            }
            return common::Status::OK();
        }

        common::Status fetchPrepared(MYSQL_STMT *stmt, common::ResultSet &out,
                                     int maxRows = 0) {
            return fetchPreparedInternal(stmt, &out, nullptr, nullptr, maxRows);
        }

        common::Status fetchPreparedEach(MYSQL_STMT *stmt,
                                         const common::RowCallback &callback,
                                         std::uint64_t &rows) {
            return fetchPreparedInternal(stmt, nullptr, &callback, &rows);
        }
    } // namespace

    // MySQL 流式游标：复用 prepare+bind+execute，但不调用 mysql_stmt_store_result，
    // 按批从服务端取行（mysql_stmt_fetch）。连接被钉住直到游标关闭——这与游标
    // “借→钉住→取 N 次→显式关→还”的语义天然契合。无事务要求。
    class MyCursor : public core::ICursor {
    public:
        MyCursor(MySQLConnection &owner, StmtGuard guard, ParamStorage storage,
                 std::size_t batchSize)
            : owner_(owner), guard_(std::move(guard)), storage_(std::move(storage)),
              batchSize_(batchSize) {}

        ~MyCursor() override { reset(); }

        common::Status setupResult() {
            meta_ = mysql_stmt_result_metadata(guard_.get());
            if (!meta_) { eof_ = true; return common::Status::OK(); } // 无结果集（DML）
            nfields_ = mysql_num_fields(meta_);
            fields_.reserve(nfields_);
            types_.reserve(nfields_);
            MYSQL_FIELD *f = mysql_fetch_fields(meta_);
            for (unsigned int i = 0; i < nfields_; ++i) {
                fields_.emplace_back(f[i].name);
                types_.push_back(f[i].type);
            }
            buf_.assign(nfields_, std::vector<char>(kInitialColBytes));
            len_.assign(nfields_, 0);
            isNull_ = std::make_unique<MysqlBool[]>(nfields_);
            bind_.resize(nfields_);
            std::memset(bind_.data(), 0, nfields_ * sizeof(MYSQL_BIND));
            for (unsigned int i = 0; i < nfields_; ++i) {
                bind_[i].buffer_type = MYSQL_TYPE_STRING;
                bind_[i].buffer = buf_[i].data();
                bind_[i].buffer_length = static_cast<unsigned long>(kInitialColBytes);
                bind_[i].length = &len_[i];
                bind_[i].is_null = &isNull_[i];
            }
            if (mysql_stmt_bind_result(guard_.get(), bind_.data()) != 0)
                return owner_.lastError("mysql_stmt_bind_result(openCursor)");
            open_ = true;
            return common::Status::OK();
        }

        common::Status fetch(std::size_t n, common::ResultSet &out) override {
            if (!open_ || eof_) return common::Status::OK();
            // 置 activeThreadId_，使 mysql_kill 取消能在取行进行中生效（与 query 一致）。
            ActiveMysqlOperation active(owner_.operationMtx_, owner_.activeThreadId_,
                                        mysql_thread_id(owner_.m_));
            if (!fieldsSet_) { out.setFields(fields_); fieldsSet_ = true; }
            const std::size_t want = (n == 0) ? batchSize_ : n;
            for (std::size_t i = 0; i < want; ++i) {
                const int rc = mysql_stmt_fetch(guard_.get());
                if (rc == MYSQL_NO_DATA) { eof_ = true; break; }
                if (rc != 0 && rc != MYSQL_DATA_TRUNCATED)
                    return owner_.lastError("mysql_stmt_fetch(openCursor)");
                if (rc == MYSQL_DATA_TRUNCATED) {
                    for (unsigned int c = 0; c < nfields_; ++c) {
                        if (len_[c] <= buf_[c].size()) continue;
                        buf_[c].resize(static_cast<std::size_t>(len_[c]) + 1);
                        bind_[c].buffer = buf_[c].data();
                        bind_[c].buffer_length = len_[c];
                        MYSQL_BIND one = bind_[c];
                        if (mysql_stmt_fetch_column(guard_.get(), &one, c, 0) != 0)
                            return owner_.lastError("mysql_stmt_fetch_column(openCursor)");
                    }
                }
                common::Row row;
                for (unsigned int c = 0; c < nfields_; ++c) {
                    const char *colName = fields_[c].c_str();
                    if (isNull_[c]) { row.set(colName, nullptr); continue; }
                    row.set(colName, fieldToValue(types_[c], buf_[c].data(), len_[c]));
                }
                out.addRow(std::move(row));
                ++rowsFetched_;
            }
            return common::Status::OK();
        }

        common::Status fetchRow(common::Row &outRow, bool &ok) override {
            ok = false;
            if (!open_ || eof_) return common::Status::OK();
            common::ResultSet tmp;
            const auto st = fetch(1, tmp);
            if (!st.ok()) return st;
            if (tmp.empty()) return common::Status::OK();
            outRow = std::move(tmp.rows()[0]);
            ok = true;
            return common::Status::OK();
        }

        common::Status close() override {
            reset();
            return common::Status::OK();
        }

        [[nodiscard]] bool isOpen() const override { return open_; }
        [[nodiscard]] bool hasNext() const override { return open_ && !eof_; }
        [[nodiscard]] std::uint64_t rowsFetched() const override { return rowsFetched_; }

    private:
        void reset() {
            if (meta_) { mysql_free_result(meta_); meta_ = nullptr; }
            guard_ = StmtGuard(nullptr); // 释放语句句柄（关闭游标）
            open_ = false;
        }

        MySQLConnection &owner_;
        StmtGuard guard_;
        ParamStorage storage_; // 保留参数缓冲存活（执行后仍可能被驱动引用）
        std::size_t batchSize_;
        MYSQL_RES *meta_ = nullptr;
        unsigned int nfields_ = 0;
        std::vector<std::string> fields_;
        std::vector<enum_field_types> types_;
        std::vector<std::vector<char>> buf_;
        std::vector<unsigned long> len_;
        MysqlBoolArray isNull_;
        std::vector<MYSQL_BIND> bind_;
        bool open_ = false;
        bool eof_ = false;
        bool fieldsSet_ = false;
        std::uint64_t rowsFetched_ = 0;
    };

#endif

    common::Status MySQLConnection::query(const std::string &sql, const common::Params &params,
                                          common::ResultSet &out) {
#ifdef DBMW_ENABLE_MYSQL
        if (!open_ || !m_) return notConnected("query");
        ActiveMysqlOperation active(operationMtx_, activeThreadId_, mysql_thread_id(m_));

        StmtGuard guard(nullptr);
        ParamStorage st;
        if (const auto s = prepareAndBind(m_, sql, params, guard, st); !s.ok()) return s;

        if (mysql_stmt_execute(guard.get()) != 0) return stmtError("mysql_stmt_execute", guard.get());
        return fetchPrepared(guard.get(), out, cfg_.max_result_rows);
#else
        (void) sql;
        (void) params;
        (void) out;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::execute(const std::string &sql, const common::Params &params,
                                            std::int64_t &affected) {
#ifdef DBMW_ENABLE_MYSQL
        affected = 0;
        if (!open_ || !m_) return notConnected("execute");
        ActiveMysqlOperation active(operationMtx_, activeThreadId_, mysql_thread_id(m_));

        StmtGuard guard(nullptr);
        ParamStorage st;
        if (const auto s = prepareAndBind(m_, sql, params, guard, st); !s.ok()) return s;

        if (mysql_stmt_execute(guard.get()) != 0) return stmtError("mysql_stmt_execute", guard.get());
        affected = static_cast<std::int64_t>(mysql_stmt_affected_rows(guard.get()));
        return common::Status::OK();
#else
        (void) sql;
        (void) params;
        affected = 0;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::execute(const std::string &sql, std::int64_t &affected,
                                           common::GeneratedKeys &out) {
#ifdef DBMW_ENABLE_MYSQL
        out.clear();
        if (const auto s = execute(sql, affected); !s.ok()) return s;
        const auto id = mysql_insert_id(m_);
        if (id != 0) {
            common::Row r;
            r.set("insert_id", static_cast<std::int64_t>(id));
            out.rows.addRow(std::move(r));
        }
        return common::Status::OK();
#else
        (void) sql; (void) affected; out.clear();
        return common::Status::error(common::ErrorCode::NotSupported, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::execute(const std::string &sql, const common::Params &params,
                                           std::int64_t &affected, common::GeneratedKeys &out) {
#ifdef DBMW_ENABLE_MYSQL
        out.clear();
        if (!open_ || !m_) return notConnected("execute");
        ActiveMysqlOperation active(operationMtx_, activeThreadId_, mysql_thread_id(m_));
        StmtGuard guard(nullptr);
        ParamStorage st;
        if (const auto s = prepareAndBind(m_, sql, params, guard, st); !s.ok()) return s;
        if (mysql_stmt_execute(guard.get()) != 0)
            return stmtError("mysql_stmt_execute", guard.get());
        affected = static_cast<std::int64_t>(mysql_stmt_affected_rows(guard.get()));
        const auto id = mysql_insert_id(m_);
        if (id != 0) {
            common::Row r;
            r.set("insert_id", static_cast<std::int64_t>(id));
            out.rows.addRow(std::move(r));
        }
        return common::Status::OK();
#else
        (void) sql; (void) params; (void) affected; out.clear();
        return common::Status::error(common::ErrorCode::NotSupported, "MySQL driver disabled");
#endif
    }

    bool MySQLConnection::supportsPrepared() const {
#ifdef DBMW_ENABLE_MYSQL
        return true;
#else
        return false;
#endif
    }

    common::Status MySQLConnection::prepare(const std::string &sql,
                                           const common::Params &typesSample,
                                           core::PreparedStatementHandle &out) {
#ifdef DBMW_ENABLE_MYSQL
        out = core::PreparedStatementHandle{};
        if (!open_ || !m_) return notConnected("prepare");
        // key = SQL + 参数类型签名。PG / MySQL 在 prepare 阶段就需要参数类型，
        // 类型序列不同必须视为不同语句（见 common::paramTypeSignature）。
        const std::string key = sql + common::paramTypeSignature(typesSample);
        if (const auto it = preparedCache_.find(key); it != preparedCache_.end()) {
            // 命中本连接缓存：移到 LRU 末尾（最近使用）。
            preparedLru_.remove(key);
            preparedLru_.push_back(key);
            out = it->second;
            return common::Status::OK();
        }
        MYSQL_STMT *stmt = mysql_stmt_init(m_);
        if (!stmt)
            return common::Status::error(common::ErrorCode::QueryError,
                                         "MySQL: mysql_stmt_init failed (out of memory)");
        if (mysql_stmt_prepare(stmt, sql.data(), static_cast<unsigned long>(sql.size())) != 0) {
            const auto msg = std::string("mysql_stmt_prepare: ") + mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return common::Status::error(common::ErrorCode::QueryError, std::move(msg));
        }
        const auto id = ++preparedSeq_;
        core::PreparedStatementHandle h =
            core::PreparedStatementHandle::make(id, static_cast<void *>(stmt));
        preparedCache_[key] = h;
        preparedLru_.push_back(key);
        // 超出每连接上限时按 LRU 淘汰（等价物 = mysql_stmt_close）。
        // 注意：被淘汰的句柄若仍被上层持有会悬空——这是 LRU 的固有取舍，
        // 默认 max_per_connection=0（不限制）时不会触发；显式设上限即表示接受。
        if (preparedLimit_ > 0) {
            while (preparedCache_.size() > static_cast<std::size_t>(preparedLimit_)) {
                const std::string oldKey = preparedLru_.front();
                preparedLru_.pop_front();
                if (const auto oit = preparedCache_.find(oldKey); oit != preparedCache_.end()) {
                    if (MYSQL_STMT *s = static_cast<MYSQL_STMT *>(oit->second.native()))
                        mysql_stmt_close(s);
                    preparedCache_.erase(oit);
                }
            }
        }
        out = h;
        return common::Status::OK();
#else
        (void) sql; (void) typesSample; out = core::PreparedStatementHandle{};
        return common::Status::error(common::ErrorCode::NotSupported, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::executePrepared(const core::PreparedStatementHandle &h,
                                                   const common::Params &params,
                                                   common::ResultSet &out) {
#ifdef DBMW_ENABLE_MYSQL
        if (!open_ || !m_) return notConnected("executePrepared");
        MYSQL_STMT *stmt = static_cast<MYSQL_STMT *>(h.native());
        if (!stmt)
            return common::Status::error(common::ErrorCode::QueryError,
                                         "MySQL: invalid prepared handle (null statement)");
        // 复用连接缓存里的语句句柄：只重新绑定本次参数并执行，不重新 prepare。
        ActiveMysqlOperation active(operationMtx_, activeThreadId_, mysql_thread_id(m_));
        ParamStorage st;
        if (const auto s = bindParamsOnly(stmt, params, st); !s.ok()) return s;
        if (mysql_stmt_execute(stmt) != 0) return stmtError("mysql_stmt_execute(prepared)", stmt);
        return fetchPrepared(stmt, out, cfg_.max_result_rows);
#else
        (void) h; (void) params; (void) out;
        return common::Status::error(common::ErrorCode::NotSupported, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::executePrepared(const core::PreparedStatementHandle &h,
                                                   const common::Params &params,
                                                   std::int64_t &affected) {
#ifdef DBMW_ENABLE_MYSQL
        affected = 0;
        if (!open_ || !m_) return notConnected("executePrepared");
        MYSQL_STMT *stmt = static_cast<MYSQL_STMT *>(h.native());
        if (!stmt)
            return common::Status::error(common::ErrorCode::QueryError,
                                         "MySQL: invalid prepared handle (null statement)");
        ActiveMysqlOperation active(operationMtx_, activeThreadId_, mysql_thread_id(m_));
        ParamStorage st;
        if (const auto s = bindParamsOnly(stmt, params, st); !s.ok()) return s;
        if (mysql_stmt_execute(stmt) != 0) return stmtError("mysql_stmt_execute(prepared)", stmt);
        affected = static_cast<std::int64_t>(mysql_stmt_affected_rows(stmt));
        return common::Status::OK();
#else
        (void) h; (void) params; affected = 0;
        return common::Status::error(common::ErrorCode::NotSupported, "MySQL driver disabled");
#endif
    }

    void MySQLConnection::closeAllPrepared() {
#ifdef DBMW_ENABLE_MYSQL
        for (auto &kv : preparedCache_) {
            if (MYSQL_STMT *s = static_cast<MYSQL_STMT *>(kv.second.native()))
                mysql_stmt_close(s);
        }
        preparedCache_.clear();
        preparedLru_.clear();
#endif
    }

    void MySQLConnection::setPreparedCacheLimit(int maxPerConnection) {
#ifdef DBMW_ENABLE_MYSQL
        preparedLimit_ = maxPerConnection;
#else
        (void) maxPerConnection;
#endif
    }

    common::Status MySQLConnection::queryEach(const std::string &sql,
                                              const common::Params &params,
                                              const common::RowCallback &callback,
                                              std::uint64_t &rows) {
#ifdef DBMW_ENABLE_MYSQL
        rows = 0;
        if (!open_ || !m_) return notConnected("stream");
        ActiveMysqlOperation active(operationMtx_, activeThreadId_, mysql_thread_id(m_));
        StmtGuard guard(nullptr);
        ParamStorage storage;
        if (const auto status = prepareAndBind(m_, sql, params, guard, storage); !status.ok())
            return status;
        if (mysql_stmt_execute(guard.get()) != 0)
            return stmtError("mysql_stmt_execute(stream)", guard.get());
        // 未调用 mysql_stmt_store_result：mysql_stmt_fetch 按行从服务端消费。
        return fetchPreparedEach(guard.get(), callback, rows);
#else
        (void) sql; (void) params; (void) callback; rows = 0;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::openCursor(const std::string &sql, const common::Params &params,
                                              const core::CursorOptions &opts,
                                              std::unique_ptr<core::ICursor> &out) {
#ifdef DBMW_ENABLE_MYSQL
        out.reset();
        if (!open_ || !m_) return notConnected("openCursor");
        ActiveMysqlOperation active(operationMtx_, activeThreadId_, mysql_thread_id(m_));
        StmtGuard guard(nullptr);
        ParamStorage storage;
        if (const auto s = prepareAndBind(m_, sql, params, guard, storage); !s.ok()) return s;
        if (mysql_stmt_execute(guard.get()) != 0)
            return stmtError("mysql_stmt_execute(openCursor)", guard.get());
        // 不调用 mysql_stmt_store_result：保持流式，按批从服务端取行。
        auto cur = std::make_unique<MyCursor>(*this, std::move(guard), std::move(storage),
                                              opts.batch_size > 0 ? opts.batch_size : 256);
        const auto st = cur->setupResult();
        if (!st.ok()) return st;
        out = std::move(cur);
        return common::Status::OK();
#else
        (void) sql; (void) params; (void) opts;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    std::string MySQLConnection::escapeLiteral(const common::Value &v) const {
#ifdef DBMW_ENABLE_MYSQL
        // 字符串交给 mysql_real_escape_string：它感知连接字符集，
        // 也能正确处理服务端开启 NO_BACKSLASH_ESCAPES 的情况。
        if (const auto *s = std::get_if<std::string>(&v)) {
            if (!m_ || !open_) return common::escapeLiteralGeneric(v);
            std::vector<char> buf(s->size() * 2 + 1, '\0');
            const unsigned long n = mysql_real_escape_string(
                m_, buf.data(), s->data(), static_cast<unsigned long>(s->size()));
            std::string out;
            out.reserve(static_cast<std::size_t>(n) + 2);
            out.push_back('\'');
            out.append(buf.data(), static_cast<std::size_t>(n));
            out.push_back('\'');
            return out;
        }
        // 二进制：MySQL 支持标准 SQL 的 X'..' 写法。
        return common::escapeLiteralGeneric(v);
#else
        return common::escapeLiteralGeneric(v);
#endif
    }

    common::Status MySQLConnection::begin() {
#ifdef DBMW_ENABLE_MYSQL
        if (!open_ || !m_) return notConnected("begin");
        // MySQL 的 START TRANSACTION 会隐式 COMMIT 当前事务。若不挡住重复 begin，
        // 调用方以为还在同一个事务里，前半段其实已被静默提交——这是丢数据级别的坑。
        // PostgreSQL / ODBC 驱动同样在这里返回 TxError，三个驱动必须行为一致。
        if (txOpen_)
            return common::Status::error(common::ErrorCode::TxError,
                                         "MySQL: a transaction is already open on this connection");
        if (mysql_query(m_, "START TRANSACTION") != 0) return lastError("START TRANSACTION");
        txOpen_ = true;
        return common::Status::OK();
#else
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::begin(const common::TransactionOptions &options) {
#ifdef DBMW_ENABLE_MYSQL
        if (!open_ || !m_) return notConnected("begin");
        // 先挡住重入，再发 SET TRANSACTION：后者在事务已开启时语义不确定（MySQL
        // 会把它作用于"下一个"事务），静默改变隔离级别比直接报错更危险。
        if (txOpen_)
            return common::Status::error(common::ErrorCode::TxError,
                                         "MySQL: a transaction is already open on this connection");
        const char *level = nullptr;
        switch (options.isolation) {
            case common::IsolationLevel::Default: break;
            case common::IsolationLevel::ReadUncommitted: level = "READ UNCOMMITTED"; break;
            case common::IsolationLevel::ReadCommitted: level = "READ COMMITTED"; break;
            case common::IsolationLevel::RepeatableRead: level = "REPEATABLE READ"; break;
            case common::IsolationLevel::Serializable: level = "SERIALIZABLE"; break;
        }
        if (level) {
            const std::string sql = std::string("SET TRANSACTION ISOLATION LEVEL ") + level;
            if (mysql_query(m_, sql.c_str()) != 0) return lastError("SET TRANSACTION ISOLATION");
        }
        if (options.readOnly && mysql_query(m_, "SET TRANSACTION READ ONLY") != 0)
            return lastError("SET TRANSACTION READ ONLY");
        return begin();
#else
        (void) options;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::commit() {
#ifdef DBMW_ENABLE_MYSQL
        if (!open_ || !m_) return notConnected("commit");
        if (mysql_query(m_, "COMMIT") != 0) return lastError("COMMIT");
        txOpen_ = false;
        return common::Status::OK();
#else
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::rollback() {
#ifdef DBMW_ENABLE_MYSQL
        if (!open_ || !m_) return notConnected("rollback");
        if (mysql_query(m_, "ROLLBACK") != 0) return lastError("ROLLBACK");
        txOpen_ = false;
        return common::Status::OK();
#else
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::savepoint(const std::string &name) {
#ifdef DBMW_ENABLE_MYSQL
        if (!txOpen_ || !validSavepointName(name))
            return common::Status::error(common::ErrorCode::TxError,
                                         "MySQL: invalid savepoint or no active transaction");
        if (mysql_query(m_, ("SAVEPOINT " + name).c_str()) != 0) return lastError("SAVEPOINT");
        return common::Status::OK();
#else
        (void) name;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::releaseSavepoint(const std::string &name) {
#ifdef DBMW_ENABLE_MYSQL
        if (!txOpen_ || !validSavepointName(name))
            return common::Status::error(common::ErrorCode::TxError,
                                         "MySQL: invalid savepoint or no active transaction");
        if (mysql_query(m_, ("RELEASE SAVEPOINT " + name).c_str()) != 0)
            return lastError("RELEASE SAVEPOINT");
        return common::Status::OK();
#else
        (void) name;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::rollbackToSavepoint(const std::string &name) {
#ifdef DBMW_ENABLE_MYSQL
        if (!txOpen_ || !validSavepointName(name))
            return common::Status::error(common::ErrorCode::TxError,
                                         "MySQL: invalid savepoint or no active transaction");
        if (mysql_query(m_, ("ROLLBACK TO SAVEPOINT " + name).c_str()) != 0)
            return lastError("ROLLBACK TO SAVEPOINT");
        return common::Status::OK();
#else
        (void) name;
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    void MySQLConnection::close() {
#ifdef DBMW_ENABLE_MYSQL
        closeAllPrepared(); // 释放本连接上所有预编译句柄（连接即将归还/销毁）
        if (m_) {
            mysql_close(m_);
            m_ = nullptr;
        }
#endif
        open_ = false;
        txOpen_ = false;
    }

    common::Status MySQLConnection::cancel() {
#ifdef DBMW_ENABLE_MYSQL
        std::lock_guard<std::mutex> lock(operationMtx_);
        if (!m_ || !open_ || activeThreadId_ == 0)
            return common::Status::error(common::ErrorCode::NotConnected,
                                         "MySQL: no active query to cancel");

        // libmysqlclient 没有可在同一连接上并发调用的 cancel API。
        // 使用一条短生命周期控制连接终止当前 server thread；原连接随后会在
        // 归池前/再次借出时被 ping 判死并重建。
        MySQLConnection control;
        if (const auto status = control.connect(cfg_); !status.ok())
            return common::Status::error(common::ErrorCode::Cancelled,
                                         "MySQL cancel control connection failed: " + status.message);
        if (mysql_kill(control.m_, activeThreadId_) != 0)
            return common::Status::databaseError(
                common::ErrorCode::Cancelled,
                std::string("MySQL mysql_kill: ") + mysql_error(control.m_),
                mysql_sqlstate(control.m_) ? mysql_sqlstate(control.m_) : "",
                static_cast<std::int64_t>(mysql_errno(control.m_)));
        return common::Status::OK();
#else
        return common::Status::error(common::ErrorCode::DriverDisabled, "MySQL driver disabled");
#endif
    }

    common::Status MySQLConnection::lastError(const char *where) {
#ifdef DBMW_ENABLE_MYSQL
        std::string msg = where;
        msg += ": ";
        msg += mysql_error(m_);
        return common::Status::databaseError(
            common::ErrorCode::QueryError, std::move(msg),
            mysql_sqlstate(m_) ? mysql_sqlstate(m_) : "",
            static_cast<std::int64_t>(mysql_errno(m_)));
#else
        (void) where;
        return common::Status::error(common::ErrorCode::QueryError, "MySQL error");
#endif
    }

#ifdef DBMW_ENABLE_MYSQL
    common::Status MySQLConnection::stmtError(const char *where, MYSQL_STMT *stmt) {
        std::string msg = where;
        msg += ": ";
        msg += stmt ? mysql_stmt_error(stmt) : "(null statement)";
        return common::Status::databaseError(
            common::ErrorCode::QueryError, std::move(msg),
            stmt && mysql_stmt_sqlstate(stmt) ? mysql_stmt_sqlstate(stmt) : "",
            stmt ? static_cast<std::int64_t>(mysql_stmt_errno(stmt)) : 0);
    }
#endif

    void registerMySQLDriver() {
        DriverRegistry::instance().registerDriver("mysql",
                                                  []() { return std::make_unique<MySQLDriver>(); });
    }
} // namespace dbmw::driver
