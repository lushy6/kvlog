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

    // Returns once the write is applied and, under SyncPolicy::Always, durable.
    void set(std::string_view key, std::string_view value) {
        std::uint64_t seq = 0;
        {
            Shard& s = shard(key);
            std::unique_lock lk(s.mu);
            // Logging inside the shard lock keeps the log order of writes to one key equal to the order they
            // were applied, so replaying the log rebuilds exactly this map. The fsync happens after unlocking.
            if (log_) seq = log_->append(encode_record(Op::Set, key, value));
            s.map.insert_or_assign(std::string(key), std::string(value));
        }
        if (log_) log_->wait_durable(seq);
    }

    // Returns true if the key existed. Deleting a missing key writes nothing.
    bool del(std::string_view key) {
        std::uint64_t seq = 0;
        bool existed;
        {
            Shard& s = shard(key);
            std::unique_lock lk(s.mu);
            auto it = s.map.find(std::string(key));
            existed = it != s.map.end();
            if (existed) {
                if (log_) seq = log_->append(encode_record(Op::Del, key));
                s.map.erase(it);
            }
        }
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

    // Rewrites the log as one SET per live key. Holds every shard lock (in a fixed order), so writes pause
    // for the duration; reads of other shards are also blocked. See README "Compaction".
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
