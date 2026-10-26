// Command dispatch: RESP command in, RESP reply appended. Kept separate from sockets so it is unit-testable.
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

inline std::string upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return s;
}

// Returns false when the connection should close after sending `out` (QUIT).
inline bool dispatch(Store& store, const Command& cmd, std::string& out, Stats* stats = nullptr) {
    if (stats) ++stats->commands;
    const std::string name = upper(cmd[0]);
    const auto arity = [&](std::size_t n) {
        if (cmd.size() == n) return true;
        reply_error(out, "ERR wrong number of arguments for '" + cmd[0] + "' command");
        return false;
    };
    if (name == "GET") {
        if (!arity(2)) return true;
        if (auto v = store.get(cmd[1])) reply_bulk(out, *v);
        else reply_null(out);
    } else if (name == "SET") {
        if (!arity(3)) return true;
        store.set(cmd[1], cmd[2]);
        reply_simple(out, "OK");
    } else if (name == "DEL") {
        if (cmd.size() < 2) { reply_error(out, "ERR wrong number of arguments for 'del' command"); return true; }
        long long n = 0;
        for (std::size_t i = 1; i < cmd.size(); ++i) n += store.del(cmd[i]) ? 1 : 0;
        reply_int(out, n);
    } else if (name == "EXISTS") {
        if (cmd.size() < 2) { reply_error(out, "ERR wrong number of arguments for 'exists' command"); return true; }
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
    } else if (name == "CONFIG" || name == "CLIENT" || name == "COMMAND") {
        // Benchmark tools probe these on connect; answer politely instead of failing the run.
        reply_simple(out, "OK");
    } else {
        reply_error(out, "ERR unknown command '" + cmd[0] + "'");
    }
    return true;
}

}  // namespace kvlog
