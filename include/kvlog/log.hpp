// Append-only log: every SET and DEL is a checksummed record, replayed on startup.
//
// Record layout (little-endian):
//   u32 payload_len | u32 crc32(payload) | payload
//   payload = u8 op (1 = SET, 2 = DEL) | u32 key_len | key | [u32 value_len | value]   (value only for SET)
//
// A crash can leave a partially written last record. Recovery replays records in order and stops at the
// first one that is short or fails its checksum. If nothing valid follows it, that is a torn tail: the file
// is truncated there. If a valid record DOES follow it, the damage is in the middle of the log, and recovery
// refuses to start rather than silently deleting the later records.
//
// Any failed write, flush or fsync puts the writer into a sticky failed state: every later append throws.
// Nothing is acknowledged after a write error, and nothing is appended after a possibly torn record.
#pragma once

#include <array>
#include <atomic>
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
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace kvlog {

enum class Op : std::uint8_t { Set = 1, Del = 2 };
enum class SyncPolicy { Always, EverySecond, Never };

struct LogFailed : std::runtime_error {
    using std::runtime_error::runtime_error;
};

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

inline std::string read_file(const std::filesystem::path& path) {
    std::string data;
    std::FILE* f = std::fopen(path.string().c_str(), "rb");
    if (!f) throw std::runtime_error("cannot open " + path.string());
    char buf[1 << 16];
    std::size_t n;
    while ((n = std::fread(buf, 1, sizeof buf, f)) > 0) data.append(buf, n);
    const bool failed = std::ferror(f) != 0;
    std::fclose(f);
    if (failed) throw std::runtime_error("read error on " + path.string() + " (refusing to treat it as a torn tail)");
    return data;
}

// Counts valid records without changing anything. `bad_at` is where replay would stop.
inline RecoveryReport scan(std::string_view data, const std::function<void(const Record&)>& apply = {}) {
    RecoveryReport r;
    std::size_t at = 0;
    Record rec{};
    while (at < data.size()) {
        const std::size_t used = decode_record(data, at, rec);
        if (used == 0) break;
        if (apply) apply(rec);
        at += used;
        ++r.records;
    }
    r.valid_bytes = at;
    r.truncated_bytes = data.size() - at;
    return r;
}

// True if any complete, checksummed record starts after `from`: the damage is not just a torn tail.
inline bool valid_record_after(std::string_view data, std::size_t from) {
    Record rec{};
    for (std::size_t at = from + 1; at + 8 <= data.size(); ++at) {
        const std::uint32_t len = get_u32(data, at);
        if (len < 5 || data.size() - at - 8 < len) continue;  // cheap filter before the checksum
        if (decode_record(data, at, rec)) return true;
    }
    return false;
}

// Replays the log at `path` through `apply` and truncates a torn tail. Throws on mid-log corruption.
inline RecoveryReport recover(const std::filesystem::path& path, const std::function<void(const Record&)>& apply) {
    if (!std::filesystem::exists(path)) return {};
    const std::string data = read_file(path);
    const RecoveryReport report = scan(data, apply);
    if (report.truncated_bytes) {
        if (valid_record_after(data, report.valid_bytes))
            throw std::runtime_error("log is corrupt at byte " + std::to_string(report.valid_bytes) +
                                     " but valid records follow; refusing to truncate. Inspect or move " +
                                     path.string() + " before restarting.");
        std::filesystem::resize_file(path, report.valid_bytes);
    }
    return report;
}

// ---------- durable file operations ----------

inline bool flush_and_sync(std::FILE* f) {
    if (std::fflush(f) != 0) return false;
#ifdef _WIN32
    return _commit(_fileno(f)) == 0;
#else
    return ::fsync(::fileno(f)) == 0;
#endif
}

inline bool sync_fd_of(std::FILE* f) {
#ifdef _WIN32
    return _commit(_fileno(f)) == 0;
#else
    return ::fsync(::fileno(f)) == 0;
#endif
}

// Makes a rename or file creation in `dir` survive power loss (POSIX needs the directory fsynced).
inline void sync_directory(const std::filesystem::path& dir) {
#ifndef _WIN32
    const int fd = ::open(dir.empty() ? "." : dir.c_str(), O_RDONLY);
    if (fd < 0) throw LogFailed("cannot open directory for fsync: " + dir.string());
    const int rc = ::fsync(fd);
    ::close(fd);
    if (rc != 0) throw LogFailed("directory fsync failed: " + dir.string());
#else
    (void)dir;  // MoveFileExW with MOVEFILE_WRITE_THROUGH below covers the rename on Windows
#endif
}

inline void durable_replace(const std::filesystem::path& from, const std::filesystem::path& to) {
#ifdef _WIN32
    if (!MoveFileExW(from.wstring().c_str(), to.wstring().c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH))
        throw std::runtime_error("rename failed: " + from.string() + " -> " + to.string());
#else
    std::filesystem::rename(from, to);
    sync_directory(to.parent_path());
#endif
}

// ---------- writer ----------

// Thread-safe appender with group commit: under SyncPolicy::Always, a caller waits until an fsync covering
// its own record has completed, and one fsync covers every record appended before it started.
//
// Lock order: sync_mu_ before write_mu_ (and callers' shard locks before write_mu_).
class LogWriter {
public:
    LogWriter(std::filesystem::path path, SyncPolicy policy) : path_(std::move(path)), policy_(policy) {
        const bool created = !std::filesystem::exists(path_);
        open();
        if (created) sync_directory(path_.parent_path());
        if (policy_ == SyncPolicy::EverySecond) syncer_ = std::thread([this] { sync_loop(); });
    }
    ~LogWriter() {
        {
            std::lock_guard lk(stop_mu_);
            stop_ = true;
        }
        stop_cv_.notify_all();
        if (syncer_.joinable()) syncer_.join();
        std::lock_guard lk(write_mu_);
        if (file_) {
            flush_and_sync(file_);
            std::fclose(file_);
        }
    }
    LogWriter(const LogWriter&) = delete;
    LogWriter& operator=(const LogWriter&) = delete;

    // Appends to the stdio buffer and returns a sequence number for wait_durable(). Throws once failed.
    std::uint64_t append(std::string_view record) {
        std::lock_guard lk(write_mu_);
        check_ok();
        if (std::fwrite(record.data(), 1, record.size(), file_) != record.size()) fail("log write failed");
        return ++written_;
    }

    // Hands everything appended so far to the OS (no fsync). The server calls this before sending replies,
    // so a killed process never loses an acknowledged write, whatever the fsync policy.
    void flush_to_os() {
        std::lock_guard lk(write_mu_);
        check_ok();
        if (std::fflush(file_) != 0) fail("log flush failed");
    }

    // Under SyncPolicy::Always, block until record `seq` is on stable storage. Otherwise only flush_to_os().
    void wait_durable(std::uint64_t seq) {
        if (policy_ != SyncPolicy::Always) {
            flush_to_os();
            return;
        }
        std::lock_guard sync_lk(sync_mu_);
        check_ok();
        if (synced_ >= seq) return;  // someone else's fsync already covered it
        std::uint64_t upto;
        {
            std::lock_guard lk(write_mu_);
            check_ok();
            if (std::fflush(file_) != 0) fail("log flush failed");
            upto = written_;
        }
        // fsync without write_mu_, so other writers keep appending while the disk works. file_ only changes
        // in rewrite(), which needs sync_mu_ (held here).
        if (!sync_fd_of(file_)) {
            std::lock_guard lk(write_mu_);
            fail("log fsync failed");
        }
        synced_ = upto;
        ++fsyncs_;
    }

    // Rewrites the log as `snapshot` (records produced by the caller), then atomically replaces the old file.
    // The caller must stop all writers first (the store holds every shard lock while this runs).
    // On any error the old log stays in place and in use, and the error is thrown.
    void rewrite(const std::function<void(const std::function<void(std::string_view)>&)>& snapshot) {
        std::lock_guard sync_lk(sync_mu_);
        std::lock_guard lk(write_mu_);
        check_ok();
        const auto tmp = std::filesystem::path(path_).concat(".rewrite");
        std::size_t emitted = 0;
        {
            std::FILE* out = std::fopen(tmp.string().c_str(), "wb");
            if (!out) throw std::runtime_error("cannot create " + tmp.string());
            bool ok = true;
            try {
                snapshot([&](std::string_view rec) {
                    if (std::fwrite(rec.data(), 1, rec.size(), out) != rec.size()) throw std::runtime_error("rewrite write failed");
                    ++emitted;
                });
                ok = flush_and_sync(out);
            } catch (...) {
                std::fclose(out);
                std::filesystem::remove(tmp);
                throw;
            }
            if (std::fclose(out) != 0) ok = false;
            if (!ok) {
                std::filesystem::remove(tmp);
                throw std::runtime_error("rewrite flush/fsync failed; old log kept");
            }
        }
        // Read the new file back: it must decode completely, with every record we wrote.
        const RecoveryReport check = scan(read_file(tmp));
        if (check.truncated_bytes != 0 || check.records != emitted) {
            std::filesystem::remove(tmp);
            throw std::runtime_error("rewrite verification failed; old log kept");
        }
        if (!flush_and_sync(file_)) fail("log fsync failed before rewrite");
        std::fclose(file_);  // Windows cannot replace a file that is open
        file_ = nullptr;
        try {
            durable_replace(tmp, path_);
        } catch (...) {
            open();  // the old, complete log is still at path_; keep using it
            std::filesystem::remove(tmp);
            throw;
        }
        open();
        synced_ = written_;
    }

    std::uint64_t fsyncs() const { return fsyncs_.load(); }
    bool failed() const { return failed_.load(); }

    // Bytes written so far, including any still in the stdio buffer (flushed first).
    std::uintmax_t size() {
        std::lock_guard lk(write_mu_);
        if (file_) std::fflush(file_);
        return std::filesystem::exists(path_) ? std::filesystem::file_size(path_) : 0;
    }

private:
    void open() {
        file_ = std::fopen(path_.string().c_str(), "ab");
        if (!file_) {
            failed_ = true;
            throw LogFailed("cannot open log " + path_.string());
        }
        std::setvbuf(file_, nullptr, _IOFBF, 1 << 16);
    }

    // Caller holds write_mu_.
    void check_ok() const {
        if (failed_) throw LogFailed("log is in a failed state after an earlier write error; writes are disabled");
    }
    [[noreturn]] void fail(const char* what) {
        failed_ = true;
        throw LogFailed(what);
    }

    void sync_loop() {
        for (;;) {
            {
                std::unique_lock lk(stop_mu_);
                if (stop_cv_.wait_for(lk, std::chrono::seconds(1), [this] { return stop_; })) return;
            }
            // Flush under write_mu_ (fast), fsync under sync_mu_ only, so appends are never blocked by the disk.
            std::lock_guard sync_lk(sync_mu_);
            {
                std::lock_guard lk(write_mu_);
                if (failed_ || !file_) continue;
                if (std::fflush(file_) != 0) { failed_ = true; continue; }
            }
            if (!sync_fd_of(file_)) failed_ = true;
        }
    }

    std::filesystem::path path_;
    SyncPolicy policy_;
    std::FILE* file_ = nullptr;
    std::mutex write_mu_;
    std::mutex sync_mu_;
    std::uint64_t written_ = 0;
    std::uint64_t synced_ = 0;
    std::atomic<std::uint64_t> fsyncs_{0};
    std::atomic<bool> failed_{false};
    std::mutex stop_mu_;
    bool stop_ = false;
    std::condition_variable stop_cv_;
    std::thread syncer_;
};

}  // namespace kvlog
