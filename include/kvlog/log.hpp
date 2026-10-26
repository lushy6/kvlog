// Append-only log: every SET and DEL is a checksummed record, replayed on startup.
//
// Record layout (little-endian):
//   u32 payload_len | u32 crc32(payload) | payload
//   payload = u8 op (1 = SET, 2 = DEL) | u32 key_len | key | [u32 value_len | value]   (value only for SET)
//
// A crash can leave a partially written last record. Recovery replays records in order and stops at the
// first one that is short or fails its checksum, then truncates the file there, so the next append starts
// from a clean boundary. Everything before that point was written in full and is kept.
#pragma once

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace kvlog {

enum class Op : std::uint8_t { Set = 1, Del = 2 };
enum class SyncPolicy { Always, EverySecond, Never };

// ---------- encoding ----------

inline std::uint32_t crc32(std::string_view data) {
    static const auto table = [] {
        std::array<std::uint32_t, 256> t{};
        for (std::uint32_t i = 0; i < 256; ++i) {
            std::uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            t[i] = c;
        }
        return t;
    }();
    std::uint32_t c = 0xFFFFFFFFu;
    for (unsigned char b : data) c = table[(c ^ b) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

inline void put_u32(std::string& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i) out += static_cast<char>((v >> (8 * i)) & 0xFF);
}

inline std::uint32_t get_u32(std::string_view s, std::size_t at) {
    std::uint32_t v = 0;
    for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(static_cast<unsigned char>(s[at + i])) << (8 * i);
    return v;
}

inline std::string encode_record(Op op, std::string_view key, std::string_view value = {}) {
    std::string payload;
    payload += static_cast<char>(op);
    put_u32(payload, static_cast<std::uint32_t>(key.size()));
    payload += key;
    if (op == Op::Set) {
        put_u32(payload, static_cast<std::uint32_t>(value.size()));
        payload += value;
    }
    std::string rec;
    rec.reserve(8 + payload.size());
    put_u32(rec, static_cast<std::uint32_t>(payload.size()));
    put_u32(rec, crc32(payload));
    rec += payload;
    return rec;
}

struct Record {
    Op op;
    std::string_view key;
    std::string_view value;
};

// Decodes one record at `at`. Returns bytes consumed, or 0 if the bytes there are not a complete, valid record.
inline std::size_t decode_record(std::string_view data, std::size_t at, Record& out) {
    if (data.size() - at < 8) return 0;
    const std::uint32_t len = get_u32(data, at);
    const std::uint32_t crc = get_u32(data, at + 4);
    if (len < 5 || data.size() - at - 8 < len) return 0;
    const std::string_view p = data.substr(at + 8, len);
    if (crc32(p) != crc) return 0;
    const auto op = static_cast<Op>(static_cast<unsigned char>(p[0]));
    const std::uint32_t klen = get_u32(p, 1);
    if (5ull + klen > len) return 0;
    out.op = op;
    out.key = p.substr(5, klen);
    if (op == Op::Set) {
        if (9ull + klen > len) return 0;
        const std::uint32_t vlen = get_u32(p, 5 + klen);
        if (9ull + klen + vlen != len) return 0;
        out.value = p.substr(9 + klen, vlen);
    } else if (op == Op::Del) {
        if (5ull + klen != len) return 0;
        out.value = {};
    } else {
        return 0;
    }
    return 8 + len;
}

struct RecoveryReport {
    std::size_t records = 0;
    std::uintmax_t valid_bytes = 0;
    std::uintmax_t truncated_bytes = 0;
};

// Replays the log at `path` through `apply` and truncates any torn or corrupt tail.
inline RecoveryReport recover(const std::filesystem::path& path, const std::function<void(const Record&)>& apply) {
    RecoveryReport report;
    if (!std::filesystem::exists(path)) return report;
    std::string data;
    {
        std::FILE* f = std::fopen(path.string().c_str(), "rb");
        if (!f) throw std::runtime_error("cannot open log for recovery: " + path.string());
        char buf[1 << 16];
        std::size_t n;
        while ((n = std::fread(buf, 1, sizeof buf, f)) > 0) data.append(buf, n);
        std::fclose(f);
    }
    std::size_t at = 0;
    Record r{};
    while (at < data.size()) {
        const std::size_t used = decode_record(data, at, r);
        if (used == 0) break;
        apply(r);
        at += used;
        ++report.records;
    }
    report.valid_bytes = at;
    report.truncated_bytes = data.size() - at;
    if (report.truncated_bytes) std::filesystem::resize_file(path, at);
    return report;
}

// ---------- writer ----------

inline void sync_file(std::FILE* f) {
    std::fflush(f);
#ifdef _WIN32
    _commit(_fileno(f));
#else
    ::fsync(::fileno(f));
#endif
}

// Thread-safe appender with group commit: under SyncPolicy::Always, a caller waits until an fsync covering
// its own record has completed, and one fsync covers every record appended before it started.
class LogWriter {
public:
    LogWriter(std::filesystem::path path, SyncPolicy policy) : path_(std::move(path)), policy_(policy) {
        open();
        if (policy_ == SyncPolicy::EverySecond) syncer_ = std::thread([this] { sync_loop(); });
    }
    ~LogWriter() {
        {
            std::lock_guard lk(write_mu_);
            stop_ = true;
        }
        stop_cv_.notify_all();
        if (syncer_.joinable()) syncer_.join();
        std::lock_guard lk(write_mu_);
        if (file_) {
            sync_file(file_);
            std::fclose(file_);
        }
    }
    LogWriter(const LogWriter&) = delete;
    LogWriter& operator=(const LogWriter&) = delete;

    // Appends to the OS buffer and returns a sequence number to pass to wait_durable().
    std::uint64_t append(std::string_view record) {
        std::lock_guard lk(write_mu_);
        if (std::fwrite(record.data(), 1, record.size(), file_) != record.size())
            throw std::runtime_error("log write failed");
        return ++written_;
    }

    // Under SyncPolicy::Always, block until record `seq` is on stable storage. Otherwise a no-op.
    void wait_durable(std::uint64_t seq) {
        if (policy_ != SyncPolicy::Always) return;
        std::lock_guard sync_lk(sync_mu_);
        if (synced_ >= seq) return;  // someone else's fsync already covered it
        std::uint64_t upto;
        {
            std::lock_guard lk(write_mu_);
            std::fflush(file_);
            upto = written_;
        }
        // fsync without holding write_mu_, so other writers keep appending while the disk works.
#ifdef _WIN32
        _commit(_fileno(file_));
#else
        ::fsync(::fileno(file_));
#endif
        synced_ = upto;
        ++fsyncs_;
    }

    // Rewrites the log as `snapshot` (records produced by the caller), atomically replacing the old file.
    // The caller must stop all writers first (the store holds every shard lock while this runs).
    void rewrite(const std::function<void(const std::function<void(std::string_view)>&)>& snapshot) {
        std::lock_guard sync_lk(sync_mu_);
        std::lock_guard lk(write_mu_);
        const auto tmp = std::filesystem::path(path_).concat(".rewrite");
        std::FILE* out = std::fopen(tmp.string().c_str(), "wb");
        if (!out) throw std::runtime_error("cannot create " + tmp.string());
        snapshot([&](std::string_view rec) {
            if (std::fwrite(rec.data(), 1, rec.size(), out) != rec.size()) throw std::runtime_error("rewrite failed");
        });
        sync_file(out);
        std::fclose(out);
        sync_file(file_);
        std::fclose(file_);
        file_ = nullptr;
        std::filesystem::rename(tmp, path_);  // atomic replace; a crash before this keeps the old, complete log
        open();
        synced_ = written_;
    }

    std::uint64_t fsyncs() const { return fsyncs_; }
    // Bytes written so far, including any still in the stdio buffer (flushed first).
    std::uintmax_t size() {
        std::lock_guard lk(write_mu_);
        if (file_) std::fflush(file_);
        return std::filesystem::exists(path_) ? std::filesystem::file_size(path_) : 0;
    }

private:
    void open() {
        file_ = std::fopen(path_.string().c_str(), "ab");
        if (!file_) throw std::runtime_error("cannot open log " + path_.string());
        std::setvbuf(file_, nullptr, _IOFBF, 1 << 16);
    }

    void sync_loop() {
        std::unique_lock lk(write_mu_);
        while (!stop_) {
            stop_cv_.wait_for(lk, std::chrono::seconds(1), [this] { return stop_; });
            if (file_) sync_file(file_);
        }
    }

    std::filesystem::path path_;
    SyncPolicy policy_;
    std::FILE* file_ = nullptr;
    std::mutex write_mu_;
    std::mutex sync_mu_;
    std::uint64_t written_ = 0;
    std::uint64_t synced_ = 0;
    std::uint64_t fsyncs_ = 0;
    bool stop_ = false;
    std::condition_variable stop_cv_;
    std::thread syncer_;
};

}  // namespace kvlog
