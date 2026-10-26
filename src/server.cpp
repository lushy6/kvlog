// kvlog-server: a small Redis-protocol key-value server backed by an append-only log.
//
//   kvlog-server [--port 6380] [--host 127.0.0.1] [--log kvlog.aof] [--fsync always|everysec|no] [--no-log]
//
// One thread per connection; the store is sharded so connections only contend on the same shard.
// Replies for a batch of pipelined commands are sent only after the batch's writes are durable under the
// chosen policy: fsynced for `always`, handed to the OS for `everysec` and `no` (like Redis, which writes the
// AOF to the kernel before replying in every mode).
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <new>
#include <string>
#include <thread>

#include "kvlog/commands.hpp"
#include "kvlog/log.hpp"
#include "kvlog/net.hpp"
#include "kvlog/resp.hpp"
#include "kvlog/store.hpp"

using namespace kvlog;

namespace {

constexpr std::size_t kReplyFlushBytes = 1u << 20;  // send replies early once this much is pending

struct Options {
    std::string host = "127.0.0.1";
    std::uint16_t port = 6380;
    std::string log = "kvlog.aof";
    SyncPolicy sync = SyncPolicy::EverySecond;
    bool persist = true;
    bool unsafe_reply_before_write = false;  // test-only: the crash test's negative control
    int max_connections = 1024;
};

int parse_int(const std::string& flag, const std::string& v, int lo, int hi) {
    std::size_t used = 0;
    int n = 0;
    try { n = std::stoi(v, &used); } catch (...) { used = 0; }
    if (used != v.size() || n < lo || n > hi)
        throw std::runtime_error(flag + " must be an integer from " + std::to_string(lo) + " to " + std::to_string(hi));
    return n;
}

Options parse_args(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
            return argv[++i];
        };
        if (a == "--port") o.port = static_cast<std::uint16_t>(parse_int(a, value(), 1, 65535));
        else if (a == "--host") o.host = value();
        else if (a == "--log") o.log = value();
        else if (a == "--no-log") o.persist = false;
        else if (a == "--max-connections") o.max_connections = parse_int(a, value(), 1, 100000);
        else if (a == "--unsafe-reply-before-write") o.unsafe_reply_before_write = true;
        else if (a == "--fsync") {
            const std::string v = value();
            if (v == "always") o.sync = SyncPolicy::Always;
            else if (v == "everysec") o.sync = SyncPolicy::EverySecond;
            else if (v == "no") o.sync = SyncPolicy::Never;
            else throw std::runtime_error("--fsync must be always, everysec or no");
        } else if (a == "--help" || a == "-h") {
            std::puts("kvlog-server [--port 6380] [--host 127.0.0.1] [--log kvlog.aof] [--fsync always|everysec|no] [--no-log]");
            std::exit(0);
        } else {
            throw std::runtime_error("unknown option " + a);
        }
    }
    return o;
}

struct Server {
    Store& store;
    Stats& stats;
    std::atomic<int>& live;
    bool unsafe_reply_before_write;
};

// Makes the batch's writes durable (per policy), then sends. False if the connection must close.
bool commit_and_send(const Server& srv, socket_t client, Batch& batch, std::string& out) {
    if (batch.wrote && srv.store.log() && !srv.unsafe_reply_before_write) {
        try {
            srv.store.log()->wait_durable(batch.max_seq);
        } catch (const LogFailed& e) {
            // The writes may not be durable: never acknowledge them. Replace the whole reply with an error.
            out.clear();
            reply_error(out, std::string("ERR ") + e.what());
            net::send_all(client, out);
            return false;
        }
    }
    batch = Batch{};
    const bool sent = out.empty() || net::send_all(client, out);
    out.clear();
    return sent;
}

void serve(Server srv, socket_t client) {
    RespParser parser;
    std::string out;
    Batch batch;
    char buf[64 * 1024];
    bool open = true;
    try {
        while (open) {
            const int n = net::recv_some(client, buf, sizeof buf);
            if (n == 0) break;
            parser.feed(std::string_view(buf, static_cast<std::size_t>(n)));
            // Answer every complete command in this read together: pipelined clients get batched replies and
            // one durability wait. Large replies are sent early so memory stays bounded.
            while (open) {
                auto cmd = parser.next();
                if (!cmd) break;
                try {
                    open = dispatch(srv.store, *cmd, out, batch, &srv.stats);
                } catch (const LogFailed& e) {
                    reply_error(out, std::string("ERR ") + e.what());
                } catch (const std::bad_alloc&) {
                    throw;
                } catch (const std::exception& e) {
                    reply_error(out, std::string("ERR ") + e.what());
                }
                if (out.size() >= kReplyFlushBytes && !commit_and_send(srv, client, batch, out)) open = false;
            }
            if (!parser.error().empty()) {
                reply_error(out, "ERR Protocol error: " + parser.error());
                open = false;
            }
            if (!commit_and_send(srv, client, batch, out)) break;
        }
    } catch (const std::exception& e) {
        // Out of memory or similar: drop this client, keep the server running.
        std::fprintf(stderr, "connection dropped: %s\n", e.what());
    }
    close_socket(client);
    --srv.live;
}

}  // namespace

int main(int argc, char** argv) {
#ifndef _WIN32
    std::signal(SIGPIPE, SIG_IGN);  // belt and braces with MSG_NOSIGNAL
#endif
    try {
        const Options opt = parse_args(argc, argv);
        static net::Init sockets;

        // The store and log live for the whole process and are never destroyed: connection threads are
        // detached, so tearing them down while a thread might still use them would be a use-after-free.
        static Store store;
        static LogWriter* log = nullptr;
        if (opt.persist) {
            // Recovery runs before the writer opens the file, so a torn tail is truncated first.
            const auto t0 = std::chrono::steady_clock::now();
            const RecoveryReport r = recover(opt.log, [&](const Record& rec) { store.apply(rec); });
            const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
            std::fprintf(stderr, "recovered %zu records (%zu keys) from %s in %.1f ms; truncated %ju bad tail bytes\n",
                         r.records, store.size(), opt.log.c_str(), ms, r.truncated_bytes);
            log = new LogWriter(opt.log, opt.sync);
            store.attach_log(log);
        }

        static Stats stats;
        static std::atomic<int> live{0};
        const socket_t listener = net::listen_on(opt.host, opt.port);
        std::fprintf(stderr, "kvlog listening on %s:%u (log: %s, fsync: %s%s)\n", opt.host.c_str(), opt.port,
                     opt.persist ? opt.log.c_str() : "off",
                     opt.sync == SyncPolicy::Always ? "always" : opt.sync == SyncPolicy::EverySecond ? "everysec" : "no",
                     opt.unsafe_reply_before_write ? ", UNSAFE reply-before-write" : "");
        std::fflush(stderr);
        const Server srv{store, stats, live, opt.unsafe_reply_before_write};
        for (;;) {
            socket_t client = ::accept(listener, nullptr, nullptr);
            if (client == kBadSocket) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));  // e.g. out of descriptors: don't spin
                continue;
            }
            if (live.load() >= opt.max_connections) {
                net::send_all(client, "-ERR max number of clients reached\r\n");
                close_socket(client);
                continue;
            }
            net::no_delay(client);
            ++live;
            ++stats.connections;
            try {
                std::thread(serve, srv, client).detach();
            } catch (const std::system_error&) {
                --live;
                net::send_all(client, "-ERR server cannot start a thread for this connection\r\n");
                close_socket(client);
            }
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "kvlog-server: %s\n", e.what());
        std::fflush(stderr);
        std::_Exit(1);  // skip static destructors (see above)
    }
}
