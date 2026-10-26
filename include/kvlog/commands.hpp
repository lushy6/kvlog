// Command dispatch: RESP command in, RESP reply appended. Kept separate from sockets so it is unit-testable.
//
// Writes do not wait for durability here. dispatch() records the highest log sequence number in `Batch`, and
// the caller waits once for the whole batch before sending any reply (one fsync per pipeline, not per command).
#pragma once

#include <algorithm>
#include <atomic>
#include <cctype>
#include <string>

#include "kvlog/resp.hpp"
#include "kvlog/store.hpp"

namespace kvlog {

struct Stats {
    std::atomic<std::uint64_t> commands{0};
    std::atomic<std::uint64_t> connections{0};
};

struct Batch {
    std::uint64_t max_seq = 0;  // highest log sequence written by this batch; 0 = nothing to wait for
    bool wrote = false;
};

inline std::string upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return s;
}

// Client-supplied text in an error line: no CR/LF (it would forge extra replies), bounded length.
inline std::string printable(std::string_view s) {
    std::string out(s.substr(0, 128));
    std::replace_if(out.begin(), out.end(), [](char c) { return c == '\r' || c == '\n'; }, ' ');
    return out;
}

// Returns false when the connection should close after sending `out` (QUIT).
inline bool dispatch(Store& store, const Command& cmd, std::string& out, Batch& batch, Stats* stats = nullptr) {
    if (stats) ++stats->commands;
    const std::string name = upper(cmd[0]);
    const auto arity_error = [&] { reply_error(out, "ERR wrong number of arguments for '" + printable(cmd[0]) + "' command"); };
    const auto wrote = [&](std::uint64_t seq) {
        batch.wrote = true;
        batch.max_seq = std::max(batch.max_seq, seq);
    };
    if (name == "GET") {
        if (cmd.size() != 2) { arity_error(); return true; }
        if (auto v = store.get(cmd[1])) reply_bulk(out, *v);
        else reply_null(out);
    } else if (name == "SET") {
        if (cmd.size() != 3) { arity_error(); return true; }
        wrote(store.set_nowait(cmd[1], cmd[2]));
        reply_simple(out, "OK");
    } else if (name == "DEL") {
        if (cmd.size() < 2) { arity_error(); return true; }
        long long n = 0;
        for (std::size_t i = 1; i < cmd.size(); ++i) {
            const auto [existed, seq] = store.del_nowait(cmd[i]);
            if (existed) { ++n; wrote(seq); }
        }
        reply_int(out, n);
    } else if (name == "EXISTS") {
        if (cmd.size() < 2) { arity_error(); return true; }
        long long n = 0;
        for (std::size_t i = 1; i < cmd.size(); ++i) n += store.exists(cmd[i]) ? 1 : 0;
        reply_int(out, n);
    } else if (name == "PING") {
        if (cmd.size() == 2) reply_bulk(out, cmd[1]);
        else reply_simple(out, "PONG");
    } else if (name == "DBSIZE") {
        reply_int(out, static_cast<long long>(store.size()));
    } else if (name == "COMPACT") {
        store.compact();
        reply_simple(out, "OK");
    } else if (name == "QUIT") {
        reply_simple(out, "OK");
        return false;
    } else if (name == "COMMAND" || name == "CONFIG") {
        out += "*0\r\n";  // clients probe these on connect and expect an array; "nothing to report"
    } else if (name == "CLIENT") {
        reply_simple(out, "OK");
    } else {
        reply_error(out, "ERR unknown command '" + printable(cmd[0]) + "'");
    }
    return true;
}

// Convenience for tests: dispatch one command and wait for its durability immediately.
inline bool dispatch_now(Store& store, const Command& cmd, std::string& out) {
    Batch b;
    const bool keep = dispatch(store, cmd, out, b);
    if (b.wrote && store.log()) store.log()->wait_durable(b.max_seq);
    return keep;
}

}  // namespace kvlog
