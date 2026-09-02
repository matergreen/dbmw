#ifndef DBMW_CORE_RATE_LIMITER_H
#define DBMW_CORE_RATE_LIMITER_H

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>


namespace dbmw::core {
    // 令牌桶：时间驱动补充，取令牌加锁（单桶竞争，足够承载单数据源 QPS）。
    class TokenBucket {
    public:
        TokenBucket(double ratePerSec, double burst)
            : tokens_(burst), rate_(ratePerSec), burst_(burst), last_(clock::now()) {}

        // 取 1 个令牌；不足返回 false（被限流）。
        bool tryAcquire() {
            std::lock_guard<std::mutex> lk(mtx_);
            const auto now = clock::now();
            const double elapsed = std::chrono::duration<double>(now - last_).count();
            last_ = now;
            tokens_ += elapsed * rate_;
            if (tokens_ > burst_) tokens_ = burst_;
            if (tokens_ >= 1.0) {
                tokens_ -= 1.0;
                return true;
            }
            return false;
        }

    private:
        using clock = std::chrono::steady_clock;
        std::mutex mtx_;
        double tokens_;
        double rate_;
        double burst_;
        clock::time_point last_;
    };

    // 限流器：全局桶（每数据源总 QPS）+ 可选指纹桶（单 SQL 热点保护）。
    //
    // 限流是"快速失败"——acquire 返回 false 即被限流，调用方应立即失败或本地排队，
    // 不要把连接池打满后雪崩。指纹桶带容量上限，超出退化为只限全局。
    class RateLimiter {
    public:
        // globalQps/perFpQps 为每秒令牌；burst 为桶容量（<=0 时等于对应 qps）。
        RateLimiter(double globalQps, double perFpQps, int burst, std::string fpMode)
            : perFpQps_(perFpQps), fpMode_(std::move(fpMode)) {
            if (globalQps > 0) {
                const double b = burst > 0 ? static_cast<double>(burst) : globalQps;
                global_ = std::make_shared<TokenBucket>(globalQps, b);
            }
        }

        // 是否启用了按指纹限流（DataSource 据此决定是否计算指纹，省开销）。
        bool usesFingerprint() const {
            return fpMode_ != "off" && perFpQps_ > 0;
        }

        // 返回 false 表示被限流。fp==0 时不走指纹桶。
        bool acquire(std::uint64_t fp) {
            if (!global_) return true; // 未启用全局限流 -> 放行
            if (!global_->tryAcquire()) return false;
            if (fp != 0 && fpMode_ != "off" && perFpQps_ > 0) {
                std::shared_ptr<TokenBucket> bucket;
                {
                    std::lock_guard<std::mutex> lk(mapMtx_);
                    auto it = fpBuckets_.find(fp);
                    if (it == fpBuckets_.end()) {
                        if (fpBuckets_.size() >= kFpCap) return true; // 退化：只限全局
                        bucket = std::make_shared<TokenBucket>(perFpQps_, perFpQps_);
                        fpBuckets_[fp] = bucket;
                    } else {
                        bucket = it->second;
                    }
                }
                if (!bucket->tryAcquire()) return false;
            }
            return true;
        }

    private:
        static constexpr std::size_t kFpCap = 1024;
        std::shared_ptr<TokenBucket> global_;
        double perFpQps_;
        std::string fpMode_;
        std::mutex mapMtx_;
        std::unordered_map<std::uint64_t, std::shared_ptr<TokenBucket>> fpBuckets_;
    };
} // namespace dbmw::core

#endif // DBMW_CORE_RATE_LIMITER_H
