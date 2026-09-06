// M1 SPI 上下文层单测：覆盖 SqlContext / ContextScope / parseTraceparent /
// formatTraceparent / nextSpanId。纯公共层测试，不依赖驱动与执行器。
#include "dbmw/common/context.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace dbmw::common;

static int g_failed = 0;
static int g_passed = 0;

static void check(bool cond, const std::string &name) {
    if (cond) { ++g_passed; std::cout << "  [PASS] " << name << "\n"; }
    else { ++g_failed; std::cout << "  [FAIL] " << name << "\n"; }
}

static bool isLowerHex16(const std::string &s) {
    if (s.size() != 16) return false;
    for (char c : s) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    }
    return true;
}

int main() {
    std::cout << "== M1 上下文：SqlContext::empty() 状态切换 ==\n";
    {
        SqlContext c;
        check(c.empty(), "默认构造全空，empty() 为 true");

        c.traceId = std::string(32, 'a');
        check(!c.empty(), "填 traceId 后 empty() 为 false");
        c.traceId.clear();
        check(c.empty(), "清回 traceId 后 empty() 恢复 true");

        c.tenantId = "tenant-1";
        check(!c.empty(), "只填 tenantId 也判 non-empty");
    }

    std::cout << "== M1 上下文：ContextScope LIFO 与 current() ==\n";
    {
        // 没有任何 Scope 时，current() 是 defaultInstance（全部字段默认）。
        check(ContextScope::current().empty(), "无 Scope 时 current() 走 defaultInstance");
        check(ContextScope::depth() == 0, "空线程下栈深 = 0");

        {
            SqlContext a;
            a.traceId = "t-a";
            a.tenantId = "ta";
            ContextScope s1(a);
            check(ContextScope::depth() == 1, "第一次 push 后 depth = 1");
            check(ContextScope::current().traceId == "t-a", "current() 取栈顶 traceId");

            {
                SqlContext b;
                b.traceId = "t-b";
                b.spanId = "sp-b";
                b.targetDataSource = "ds-b";
                ContextScope s2(b);
                check(ContextScope::depth() == 2, "嵌套 push 后 depth = 2");
                check(ContextScope::current().traceId == "t-b" &&
                      ContextScope::current().spanId == "sp-b" &&
                      ContextScope::current().targetDataSource == "ds-b",
                      "current() 取栈顶 s2 的全部字段");
            }
            check(ContextScope::depth() == 1, "s2 析构后回到 depth = 1");
            check(ContextScope::current().traceId == "t-a",
                  "LIFO 严格：s2 出栈后 s1 重新可见");
            // LIFO 不变量：s2 出栈时不能动 s1 之后的栈层；这里等同于 s1 还在。
        }
        check(ContextScope::depth() == 0, "s1 析构后栈完全清空");
        check(ContextScope::current().empty(), "栈空 current() 再次走 defaultInstance");
    }

    std::cout << "== M1 上下文：异常 RAII 仍弹出栈 ==\n";
    {
        check(ContextScope::depth() == 0, "进入异常测试前栈空");
        try {
            SqlContext a; a.traceId = "throw-trace";
            ContextScope s(a);
            check(ContextScope::depth() == 1, "异常路径中 push 也成功");
            throw std::runtime_error("boom");
        } catch (const std::runtime_error &) {
            // 吞掉，下面验证 RAII 是否正确出栈
        }
        check(ContextScope::depth() == 0, "RAII 保证异常路径栈深归零");
        check(ContextScope::current().empty(), "异常路径不污染 current()");
    }

    std::cout << "== M1 上下文：线程局部性 ==\n";
    {
        SqlContext main;
        main.traceId = "main-thread-trace";
        ContextScope s(main);
        check(ContextScope::current().traceId == "main-thread-trace", "主线程 traceId");

        std::atomic<bool> workerOk{false};
        std::string workerObserved;
        std::thread t([&] {
            // worker 自己就是空栈：current() 应为 defaultInstance，与主线程无关。
            workerObserved = ContextScope::current().traceId;
            workerOk.store(ContextScope::current().empty());
            // worker 自己 push 一层不影响主线程
            SqlContext c; c.traceId = "worker-trace";
            ContextScope ws(c);
        });
        t.join();
        check(workerObserved.empty(), "worker 看到的是 defaultInstance（不共享主线程栈）");
        check(workerOk.load(), "worker 中 current().empty() = true");
        check(ContextScope::current().traceId == "main-thread-trace",
              "主线程栈不受 worker 影响（LIFO 不变量 + 线程局部）");
    }

    std::cout << "== M1 上下文：parseTraceparent 合法/非法形态 ==\n";
    {
        SqlContext out;
        // 标准 55 字节格式
        check(parseTraceparent(
                  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                  out),
              "W3C 标准 traceparent 解析成功");
        check(out.traceId == "0af7651916cd43dd8448eb211c80319c" &&
              out.spanId == "b7ad6b7169203331",
              "traceId/spanId 字段填回正确");

        // 长度不对
        SqlContext bad;
        check(!parseTraceparent("00-0af7651916cd43dd8448eb211c80319c", bad),
              "长度不足 55 字节拒绝");
        check(!parseTraceparent(
                  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extra",
                  bad),
              "长度超出 55 字节拒绝");

        // 版本号不对
        check(!parseTraceparent(
                  "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                  bad),
              "version != 00 拒绝");

        // W3C 禁止全 0 trace-id
        check(!parseTraceparent(
                  "00-00000000000000000000000000000000-b7ad6b7169203331-01",
                  bad),
              "全 0 trace-id 拒绝");

        // 非 hex 字符
        check(!parseTraceparent(
                  "00-0af7651916cd43dd8448eb211c80319g-b7ad6b7169203331-01",
                  bad),
              "traceId 含 g 拒绝");
        check(!parseTraceparent(
                  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b716920333z-01",
                  bad),
              "spanId 含 z 拒绝");

        // flags 错误不影响主字段有效性（设计上 flags 容许扩展）
        SqlContext withBadFlags;
        check(parseTraceparent(
                  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-zz",
                  withBadFlags),
              "flags 非 hex 不影响 traceId/spanId 解析");
        check(withBadFlags.traceId == "0af7651916cd43dd8448eb211c80319c",
              "flags 错误时 traceId 仍被填充");
    }

    std::cout << "== M1 上下文：formatTraceparent 与 parse 往返 ==\n";
    {
        SqlContext ctx;
        check(formatTraceparent(ctx).empty(),
              "traceId 为空时 formatTraceparent 返回空串");

        ctx.traceId = "0af7651916cd43dd8448eb211c80319c";
        ctx.spanId = "b7ad6b7169203331";
        const auto formatted = formatTraceparent(ctx);
        check(formatted.size() == 55, "格式输出固定 55 字节");
        check(formatted.substr(0, 3) == "00-", "前缀固定 00-");
        check(formatted.substr(35, 1) == "-" && formatted.substr(52, 1) == "-",
              "分隔符位置正确");
        SqlContext roundTrip;
        check(parseTraceparent(formatted, roundTrip),
              "format 输出可直接被 parse 吃回");
        check(roundTrip.traceId == ctx.traceId &&
              roundTrip.spanId == ctx.spanId,
              "traceId/spanId 往返无损");

        // spanId 长度不对时补 16 个 0
        ctx.spanId = "short";
        const auto padded = formatTraceparent(ctx);
        SqlContext parsed;
        check(parseTraceparent(padded, parsed) && parsed.spanId == "0000000000000000",
              "spanId 长度无效时占位 16 个 0");
    }

    std::cout << "== M1 上下文：nextSpanId 形态与跨调用差异 ==\n";
    {
        SqlContext empty;
        check(nextSpanId().empty(),
              "无调用方上下文时 nextSpanId 返回空串（不发幽灵 span）");

        SqlContext ctx; ctx.traceId = "0af7651916cd43dd8448eb211c80319c";
        ContextScope scope(ctx);
        const auto first = nextSpanId();
        const auto second = nextSpanId();
        const auto third = nextSpanId();
        check(isLowerHex16(first), "nextSpanId 第一次输出 16 小写 hex");
        check(isLowerHex16(second) && isLowerHex16(third),
              "连续两次输出也是 16 小写 hex");
        std::set<std::string> unique{first, second, third};
        check(unique.size() == 3, "三次生成互不相同（线程内计数器递增）");
    }

    std::cout << "\n----------------------------------------\n";
    std::cout << "通过 " << g_passed << " 项，失败 " << g_failed << " 项\n";
    return g_failed == 0 ? 0 : 1;
}
