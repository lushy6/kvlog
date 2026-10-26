#include <atomic>
#include <barrier>
#include <filesystem>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "check.hpp"
#include "kvlog/store.hpp"

using namespace kvlog;
namespace fs = std::filesystem;

static std::map<std::string, std::string> contents(const Store& s) {
    std::map<std::string, std::string> m;
    s.for_each([&](const std::string& k, const std::string& v) { m[k] = v; });
    return m;
}

static std::map<std::string, std::string> recovered(const fs::path& p) {
    Store fresh;
    recover(p, [&](const Record& r) { fresh.apply(r); });
    return contents(fresh);
}

// Many threads hammer a few hot keys. If the log order for one key ever differed from the order the
// writes were applied in memory, replaying the log would produce a different map. Comparing only at the
// end is too weak (threads finish at different times, so their last writes rarely race), so all threads
// stop at a barrier `checkpoints` times and the replayed log is compared with memory at each stop.
static void replay_reproduces_memory_under_contention(SyncPolicy policy, int threads, int ops, int checkpoints) {
    const fs::path p = "contention.aof";
    fs::remove(p);
    std::map<std::string, std::string> live;
    std::uint64_t fsyncs = 0;
    int mismatches = 0;
    {
        LogWriter log(p, policy);
        Store store(&log);
        std::barrier sync(threads, [&]() noexcept {
            log.size();  // flushes buffered records so the file holds everything appended so far
            if (recovered(p) != contents(store)) ++mismatches;
        });
        std::vector<std::thread> pool;
        for (int t = 0; t < threads; ++t) {
            pool.emplace_back([&, t] {
                std::mt19937 rng(static_cast<unsigned>(t) * 7919u);
                std::uniform_int_distribution<int> key(0, 7), action(0, 9);
                for (int i = 0; i < ops; ++i) {
                    const std::string k = "hot" + std::to_string(key(rng));
                    if (action(rng) == 0) store.del(k);
                    else store.set(k, std::to_string(t) + ":" + std::to_string(i));
                    if ((i + 1) % (ops / checkpoints) == 0) sync.arrive_and_wait();
                }
            });
        }
        for (auto& th : pool) th.join();
        live = contents(store);
        fsyncs = log.fsyncs();
    }
    CHECK(!live.empty());
    CHECK(mismatches == 0);
    if (mismatches) std::fprintf(stderr, "  %d of %d checkpoints: replayed log != memory\n", mismatches, checkpoints);
    CHECK(recovered(p) == live);
    if (policy == SyncPolicy::Always) {
        // Group commit: every write waited for an fsync, but one fsync can cover many writers' records.
        CHECK(fsyncs > 0);
        CHECK(fsyncs < static_cast<std::uint64_t>(threads) * static_cast<std::uint64_t>(ops));
        std::printf("  fsync=always: %llu fsyncs for %d writes\n", static_cast<unsigned long long>(fsyncs), threads * ops);
    }
    fs::remove(p);
}

[[maybe_unused]] static void compaction_during_writes_loses_nothing() {  // unused in the ORDERING_ONLY build
    const fs::path p = "compact.aof";
    fs::remove(p);
    std::map<std::string, std::string> live;
    std::uintmax_t size_after = 0;
    {
        LogWriter log(p, SyncPolicy::Never);
        Store store(&log);
        std::atomic<bool> done{false};
        std::vector<std::thread> writers;
        for (int t = 0; t < 4; ++t) {
            writers.emplace_back([&, t] {
                for (int i = 0; i < 20000; ++i) {
                    store.set("k" + std::to_string(i % 500), std::to_string(t * 100000 + i));
                    if (i % 7 == 0) store.del("k" + std::to_string((i * 3) % 500));
                }
            });
        }
        std::thread compactor([&] {
            while (!done) { store.compact(); std::this_thread::yield(); }
        });
        for (auto& w : writers) w.join();
        done = true;
        compactor.join();
        store.compact();
        live = contents(store);
        size_after = log.size();
    }
    CHECK(recovered(p) == live);
    CHECK(size_after > 0);
    fs::remove(p);
}

[[maybe_unused]] static void delete_of_missing_key_writes_nothing() {  // unused in the ORDERING_ONLY build
    const fs::path p = "del.aof";
    fs::remove(p);
    {
        LogWriter log(p, SyncPolicy::Never);
        Store store(&log);
        CHECK(!store.del("nope"));
        store.set("a", "1");
        CHECK(store.del("a"));
        CHECK(!store.exists("a"));
    }
    RecoveryReport rep = recover(p, [](const Record&) {});
    CHECK(rep.records == 2);  // one SET and one DEL, nothing for the missing key
    fs::remove(p);
}

int main() {
    replay_reproduces_memory_under_contention(SyncPolicy::Never, 8, 20000, 100);
#ifndef ORDERING_ONLY
    replay_reproduces_memory_under_contention(SyncPolicy::Always, 8, 300, 10);
    compaction_during_writes_loses_nothing();
    delete_of_missing_key_writes_nothing();
#endif
    return finish("test_store");
}
