// Sharded in-memory map with the append-only log as its source of truth on restart.
#pragma once

#include <array>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kvlog/log.hpp"

namespace kvlog {

class Store {
public:
    static constexpr std::size_t kShards = 64;

    // Without a log the store is memory-only (used by tests and the "no persistence" benchmark).
    explicit Store(LogWriter* log = nullptr) : log_(log) {}

    // Startup order: recover into a log-less store, then open the writer (after any torn tail is truncated)
    // and attach it. Must be called before any client thread exists.
    void attach_log(LogWriter* log) { log_ = log; }
    LogWriter* log() const { return log_; }

    std::optional<std::string> get(std::string_view key) const {
        const Shard& s = shard(key);
        std::shared_lock lk(s.mu);
        auto it = s.map.find(std::string(key));
        if (it == s.map.end()) return std::nullopt;
        return it->second;
    }

    bool exists(std::string_view key) const {
        const Shard& s = shard(key);
        std::shared_lock lk(s.mu);
        return s.map.count(std::string(key)) != 0;
    }

    // Applies and logs the write; returns the log sequence number to wait on (0 without a log).
    // The caller must wait_durable(seq) before acknowledging; set() does both.
    std::uint64_t set_nowait(std::string_view key, std::string_view value) {
        const std::string rec = log_ ? encode_record(Op::Set, key, value) : std::string();  // encode outside the lock
        Shard& s = shard(key);
#ifdef KVLOG_BROKEN_ORDERING
        // Deliberately wrong (test-only build): log after releasing the shard lock, so two writers to one key
        // can reach memory and the log in different orders. test_store_broken must FAIL with this defined.
        {
            std::unique_lock lk(s.mu);
            s.map.insert_or_assign(std::string(key), std::string(value));
        }
        std::this_thread::yield();
        return log_ ? log_->append(rec) : 0;
#else
        std::unique_lock lk(s.mu);
        // Logging inside the shard lock keeps the log order of writes to one key equal to the order they
        // were applied, so replaying the log rebuilds exactly this map. The fsync happens after unlocking.
        const std::uint64_t seq = log_ ? log_->append(rec) : 0;
        s.map.insert_or_assign(std::string(key), std::string(value));
        return seq;
#endif
    }

    // Returns {existed, seq}. Deleting a missing key writes nothing.
    std::pair<bool, std::uint64_t> del_nowait(std::string_view key) {
        Shard& s = shard(key);
        std::unique_lock lk(s.mu);
        auto it = s.map.find(std::string(key));
        if (it == s.map.end()) return {false, 0};
        const std::uint64_t seq = log_ ? log_->append(encode_record(Op::Del, key)) : 0;
        s.map.erase(it);
        return {true, seq};
    }

    // Returns once the write is applied and, under SyncPolicy::Always, durable.
    void set(std::string_view key, std::string_view value) {
        const std::uint64_t seq = set_nowait(key, value);
        if (log_) log_->wait_durable(seq);
    }

    bool del(std::string_view key) {
        const auto [existed, seq] = del_nowait(key);
        if (existed && log_) log_->wait_durable(seq);
        return existed;
    }

    std::size_t size() const {
        std::size_t n = 0;
        for (const auto& s : shards_) {
            std::shared_lock lk(s.mu);
            n += s.map.size();
        }
        return n;
    }

    // Used during recovery only, before the server accepts connections: applies without logging.
    void apply(const Record& r) {
        Shard& s = shard(r.key);
        std::unique_lock lk(s.mu);
        if (r.op == Op::Set) s.map.insert_or_assign(std::string(r.key), std::string(r.value));
        else s.map.erase(std::string(r.key));
    }

    // Rewrites the log as one SET per live key. Holds every shard lock (in a fixed order), so reads and
    // writes pause for the duration. See README "Design decisions".
    void compact() {
        if (!log_) return;
        std::vector<std::unique_lock<std::shared_mutex>> locks;
        locks.reserve(kShards);
        for (auto& s : shards_) locks.emplace_back(s.mu);
        log_->rewrite([this](const std::function<void(std::string_view)>& emit) {
            for (const auto& s : shards_)
                for (const auto& [k, v] : s.map) emit(encode_record(Op::Set, k, v));
        });
    }

    // Visits every pair (tests compare a recovered store with the live one).
    void for_each(const std::function<void(const std::string&, const std::string&)>& fn) const {
        for (const auto& s : shards_) {
            std::shared_lock lk(s.mu);
            for (const auto& [k, v] : s.map) fn(k, v);
        }
    }

private:
    struct Shard {
        mutable std::shared_mutex mu;
        std::unordered_map<std::string, std::string> map;
    };

    Shard& shard(std::string_view key) { return shards_[std::hash<std::string_view>{}(key) % kShards]; }
    const Shard& shard(std::string_view key) const { return shards_[std::hash<std::string_view>{}(key) % kShards]; }

    std::array<Shard, kShards> shards_;
    LogWriter* log_;
};

}  // namespace kvlog
