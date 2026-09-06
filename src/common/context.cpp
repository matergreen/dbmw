#include "dbmw/common/context.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <vector>

namespace dbmw::common {

    // ------------------------------------------------------------------------
    // 线程局部的栈存储
    // ------------------------------------------------------------------------
    //
    // 栈用 std::vector 而非 std::stack：
    //  ① `current()` 需要 O(1) 访问栈顶；
    //  ② 调试/诊断打印需要底层访问。
    //
    // 栈深上限与头文件 kMaxDepth 对齐。超限后构造 ContextScope 仍合法但不再压栈，
    // 由 `entered_` 成员负责对称——析构时若未入栈则不动栈，确保 LIFO 语义严格。
    //
    // Thread_local 静态存放于 ContextScope 类静态成员函数，绑死类作用域，无
    // 顺序依赖问题（C++17 inline 静态函数 + thread_local 是 well-defined 的）。
    namespace {
        bool isHex(char c) noexcept {
            return (c >= '0' && c <= '9') ||
                   (c >= 'a' && c <= 'f') ||
                   (c >= 'A' && c <= 'F');
        }

        bool copyHex(const std::string &s, std::size_t pos, std::size_t len,
                     std::string &out) {
            if (pos + len > s.size()) return false;
            out.resize(len);
            for (std::size_t i = 0; i < len; ++i) {
                char c = s[pos + i];
                if (!isHex(c)) return false;
                out[i] = c;
            }
            return true;
        }

        constexpr char kHex[] = "0123456789abcdef";
    } // namespace

    std::vector<SqlContext> &ContextScope::stack() {
        static thread_local std::vector<SqlContext> s;
        return s;
    }

    const SqlContext &ContextScope::defaultInstance() {
        static const SqlContext kDefault{};
        return kDefault;
    }

    // ------------------------------------------------------------------------
    // ContextScope
    // ------------------------------------------------------------------------
    ContextScope::ContextScope(SqlContext ctx) : entered_(false) {
        auto &s = stack();
        if (s.size() < kMaxDepth) {
            s.push_back(std::move(ctx));
            entered_ = true;
            return;
        }
        // 溢出路径：一次性 stderr 警告，后续构造静默 noop。
        // 用 std::atomic + exchange 保证多线程下只警告一次。
        static std::atomic<bool> warned{false};
        if (!warned.exchange(true)) {
            std::fprintf(stderr,
                "dbmw: common::ContextScope 栈超过 %zu 层，"
                "后续构造不再压栈（疑似拦截器内递归调 SQL）。\n",
                kMaxDepth);
        }
    }

    ContextScope::~ContextScope() {
        if (entered_) {
            stack().pop_back();
        }
    }

    const SqlContext &ContextScope::current() noexcept {
        auto &s = stack();
        if (s.empty()) return defaultInstance();
        return s.back();
    }

    std::size_t ContextScope::depth() noexcept {
        return stack().size();
    }

    // ------------------------------------------------------------------------
    // nextSpanId
    // ------------------------------------------------------------------------
    //
    // 用进程级原子序列生成 64-bit 数，再写成 16 hex。所有线程从同一序列
    // 领取编号，避免各线程局部计数区间重叠。
    // 真正生产环境应使用 OpenTelemetry 的 spanId 随机实现（M3 集成时再升级）。
    std::string nextSpanId() {
        const auto &ctx = ContextScope::current();
        if (ctx.traceId.empty() && ctx.spanId.empty()) {
            return {}; // 无业务上下文时不发"幽灵 span"
        }
        // 每个 span 都从同一个进程级序列领取唯一编号。旧实现只给每个线程
        // 分配相邻的起点，线程 A 的第 2 个值会与线程 B 的第 1 个值碰撞。
        static std::atomic<std::uint64_t> globalSeq{0};
        std::uint64_t seq = globalSeq.fetch_add(1, std::memory_order_relaxed) + 1;
        std::array<char, 16> buf{};
        for (int i = 15; i >= 0; --i) {
            buf[i] = kHex[seq & 0x0F];
            seq >>= 4;
        }
        return std::string(buf.data(), buf.size());
    }

    // ------------------------------------------------------------------------
    // W3C traceparent
    // ------------------------------------------------------------------------
    //
    // 形态：00-<32hex traceId>-<16hex spanId>-<2hex flags>
    // 长度固定 = 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55
    //
    // flags 字段不暴露给 SqlContext——保留位（W3C versioned 演进用）。
    // 始终把 sampled flag 写 1（与 dbmw 默认"全量采样可观测"一致）。
    bool parseTraceparent(const std::string &header, SqlContext &out) {
        constexpr std::size_t kTotalLen = 55;
        if (header.size() != kTotalLen) return false;
        if (header[0] != '0' || header[1] != '0') return false;
        if (header[2] != '-' || header[35] != '-' || header[52] != '-') return false;

        SqlContext tmp;
        if (!copyHex(header, 3, 32, tmp.traceId)) return false;
        if (!copyHex(header, 36, 16, tmp.spanId)) return false;
        // flags：当前版本固定为两个十六进制字符。未知位可以保留，但格式
        // 仍必须合法；否则会把损坏的传播头当成可信 trace。
        std::string flagsBuf;
        if (!copyHex(header, 53, 2, flagsBuf)) return false;

        // W3C 明确禁止全 0 trace-id。
        bool allZero = true;
        for (char c : tmp.traceId) {
            if (c != '0') { allZero = false; break; }
        }
        if (allZero) return false;

        // W3C 同样禁止全 0 parent-id。
        allZero = true;
        for (char c : tmp.spanId) {
            if (c != '0') { allZero = false; break; }
        }
        if (allZero) return false;

        out = std::move(tmp);
        return true;
    }

    std::string formatTraceparent(const SqlContext &ctx) {
        if (ctx.traceId.size() != 32 || ctx.spanId.size() != 16) return {};
        if (!std::all_of(ctx.traceId.begin(), ctx.traceId.end(), isHex) ||
            !std::all_of(ctx.spanId.begin(), ctx.spanId.end(), isHex)) return {};
        if (std::all_of(ctx.traceId.begin(), ctx.traceId.end(), [](char c) { return c == '0'; }) ||
            std::all_of(ctx.spanId.begin(), ctx.spanId.end(), [](char c) { return c == '0'; })) return {};
        std::string out;
        out.reserve(55);
        out.append("00-");
        out.append(ctx.traceId);                  // 32 hex
        out.append("-");
        out.append(ctx.spanId);
        out.append("-01");                         // sampled
        return out;
    }

} // namespace dbmw::common
