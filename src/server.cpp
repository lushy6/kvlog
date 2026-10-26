// kvlog-server: a small Redis-protocol key-value server backed by an append-only log.
//
//   kvlog-server [--port 6380] [--host 127.0.0.1] [--log kvlog.aof] [--fsync always|everysec|no] [--no-log]
//
// One thread per connection; the store is sharded so connections only contend on the same shard.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "kvlog/commands.hpp"
#include "kvlog/log.hpp"
#include "kvlog/net.hpp"
#include "kvlog/resp.hpp"
#include "kvlog/store.hpp"

using namespace kvlog;

namespace {

struct Options {
    std::string host = "127.0.0.1";
    std::uint16_t port = 6380;
    std::string log = "kvlog.aof";
    SyncPolicy sync = SyncPolicy::EverySecond;
    bool persist = true;
    int max_connections = 1024;
};

Options parse_args(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
            return argv[++i];
        };
        if (a == "--port") o.port = static_cast<std::uint16_t>(std::stoi(value()));
        else if (a == "--host") o.host = value();
        else if (a == "--log") o.log = value();
        else if (a == "--no-log") o.persist = false;
        else if (a == "--max-connections") o.max_connections = std::stoi(value());
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

void serve(socket_t client, Store& store, Stats& stats, std::atomic<int>& live) {
    RespParser parser;
    std::string out;
    char buf[64 * 1024];
    bool open = true;
    while (open) {
        const int n = net::recv_some(client, buf, sizeof buf);
        if (n == 0) break;
        parser.feed(std::string_view(buf, static_cast<std::size_t>(n)));
        out.clear();
        // Answer every complete command in this read with one send: pipelined clients get batched replies.
        while (auto cmd = parser.next()) {
            if (!dispatch(store, *cmd, out, &stats)) { open = false; break; }
        }
        if (!parser.error().empty()) {
            reply_error(out, "ERR Protocol error: " + parser.error());
            open = false;
        }
        if (!out.empty() && !net::send_all(client, out)) break;
    }
    close_socket(client);
    --live;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Options opt = parse_args(argc, argv);
        net::Init sockets;

        std::unique_ptr<LogWriter> log;
        auto store = std::make_unique<Store>();
        if (opt.persist) {
            // Recovery runs before the writer opens the file, so a torn tail is truncated first.
            const auto t0 = std::chrono::steady_clock::now();
            const RecoveryReport r = recover(opt.log, [&](const Record& rec) { store->apply(rec); });
            const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
            std::fprintf(stderr, "recovered %zu records (%zu keys) from %s in %.1f ms; truncated %ju bad tail bytes\n",
                         r.records, store->size(), opt.log.c_str(), ms, r.truncated_bytes);
            log = std::make_unique<LogWriter>(opt.log, opt.sync);
            store->attach_log(log.get());
        }
        Store* store_ptr = store.get();

        Stats stats;
        std::atomic<int> live{0};
        const socket_t listener = net::listen_on(opt.host, opt.port);
        std::fprintf(stderr, "kvlog listening on %s:%u (log: %s, fsync: %s)\n", opt.host.c_str(), opt.port,
                     opt.persist ? opt.log.c_str() : "off",
                     opt.sync == SyncPolicy::Always ? "always" : opt.sync == SyncPolicy::EverySecond ? "everysec" : "no");
        std::fflush(stderr);
        for (;;) {
            socket_t client = ::accept(listener, nullptr, nullptr);
            if (client == kBadSocket) continue;
            if (live.load() >= opt.max_connections) {
                net::send_all(client, "-ERR max number of clients reached\r\n");
                close_socket(client);
                continue;
            }
            net::no_delay(client);
            ++live;
            ++stats.connections;
            std::thread(serve, client, std::ref(*store_ptr), std::ref(stats), std::ref(live)).detach();
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "kvlog-server: %s\n", e.what());
        return 1;
    }
}
