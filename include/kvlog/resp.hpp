// RESP (the Redis wire protocol), the subset clients use for commands: arrays of bulk strings.
// The parser is incremental: feed it bytes as they arrive, take complete commands out.
#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kvlog {

using Command = std::vector<std::string>;

class RespParser {
public:
    static constexpr std::size_t kMaxArgs = 1024;
    static constexpr std::size_t kMaxBulk = 64u << 20;      // 64 MiB per argument
    static constexpr std::size_t kMaxBuffered = 256u << 20; // per connection; Redis's equivalent is client-query-buffer-limit

    void feed(std::string_view bytes) {
        if (!error_.empty()) return;
        if (buffered() + bytes.size() > kMaxBuffered) {
            error_ = "query buffer limit exceeded";
            return;
        }
        buf_.append(bytes);
    }

    // A complete command, or nullopt if more bytes are needed. Sets error() on malformed input;
    // after an error the connection should reply with it and close (the stream cannot be resynchronised).
    //
    // Two passes: first find every argument's offset without copying anything, and only when the whole
    // command is buffered copy the arguments out. A large command arriving in small pieces therefore costs
    // a header re-scan per read, not a copy of everything received so far.
    std::optional<Command> next() {
        if (!error_.empty()) return std::nullopt;
        std::size_t pos = pos_;
        long long n = 0;
        if (!read_header(pos, '*', n)) return std::nullopt;
        if (n < 1 || static_cast<std::size_t>(n) > kMaxArgs) return fail("invalid multibulk length");
        spans_.clear();
        for (long long i = 0; i < n; ++i) {
            long long len = 0;
            if (!read_header(pos, '$', len)) return std::nullopt;
            if (len < 0 || static_cast<std::size_t>(len) > kMaxBulk) return fail("invalid bulk length");
            const auto ulen = static_cast<std::size_t>(len);
            if (buf_.size() - pos < ulen + 2) return std::nullopt;
            if (buf_[pos + ulen] != '\r' || buf_[pos + ulen + 1] != '\n') return fail("bulk string not terminated by CRLF");
            spans_.emplace_back(pos, ulen);
            pos += ulen + 2;
        }
        Command cmd;
        cmd.reserve(spans_.size());
        for (auto [at, len] : spans_) cmd.emplace_back(buf_, at, len);
        pos_ = pos;
        if (pos_ > 64 * 1024 && pos_ * 2 > buf_.size()) {  // drop consumed bytes now and then, not on every command
            buf_.erase(0, pos_);
            pos_ = 0;
        }
        return cmd;
    }

    const std::string& error() const { return error_; }
    std::size_t buffered() const { return buf_.size() - pos_; }

private:
    // Reads "<prefix><integer>\r\n" at pos. False if incomplete (or on error, with error_ set).
    bool read_header(std::size_t& pos, char prefix, long long& value) {
        if (pos >= buf_.size()) return false;
        if (buf_[pos] != prefix) { fail(prefix == '*' ? "expected '*' (inline commands are not supported)" : "expected '$'"); return false; }
        const std::size_t eol = buf_.find("\r\n", pos + 1);
        if (eol == std::string::npos) {
            if (buf_.size() - pos > 32) fail("header line too long");
            return false;
        }
        std::size_t i = pos + 1;
        const bool neg = i < eol && buf_[i] == '-';
        if (neg) ++i;
        // At most 18 digits: always fits in long long, so no overflow (lengths are capped far lower anyway).
        if (i == eol || eol - i > 18) { fail("bad length"); return false; }
        long long v = 0;
        for (; i < eol; ++i) {
            const char c = buf_[i];
            if (c < '0' || c > '9') { fail("bad length"); return false; }
            v = v * 10 + (c - '0');
        }
        value = neg ? -v : v;
        pos = eol + 2;
        return true;
    }

    std::nullopt_t fail(const char* why) {
        error_ = why;
        return std::nullopt;
    }

    std::string buf_;
    std::size_t pos_ = 0;
    std::string error_;
    std::vector<std::pair<std::size_t, std::size_t>> spans_;
};

// ---------- replies ----------

inline void reply_simple(std::string& out, std::string_view s) { out += '+'; out += s; out += "\r\n"; }
inline void reply_error(std::string& out, std::string_view s) { out += '-'; out += s; out += "\r\n"; }
inline void reply_int(std::string& out, long long v) { out += ':'; out += std::to_string(v); out += "\r\n"; }
inline void reply_null(std::string& out) { out += "$-1\r\n"; }
inline void reply_bulk(std::string& out, std::string_view s) {
    out += '$';
    out += std::to_string(s.size());
    out += "\r\n";
    out += s;
    out += "\r\n";
}

inline std::string encode_command(const Command& args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& a : args) reply_bulk(out, a);
    return out;
}

}  // namespace kvlog
