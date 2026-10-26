// kvlog-bench: a RESP load generator, so kvlog and Redis are measured by the same client.
//
//   kvlog-bench [--host 127.0.0.1] [--port 6380] [--clients 16] [--requests 20000] [--pipeline 1]
//               [--value-size 64] [--keyspace 100000] [--set-ratio 0.5] [--seed 7] [--label name]
//
// Each client thread owns one connection and sends `requests` commands in batches of `pipeline`.
// Latency is measured per batch (with --pipeline 1 that is per request). Prints one JSON line.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "kvlog/net.hpp"
#include "kvlog/resp.hpp"

using namespace kvlog;
using Clock = std::chrono::steady_clock;

namespace {

struct Options {
    std::string host = "127.0.0.1", label = "kvlog";
    std::uint16_t port = 6380;
    int clients = 16, pipeline = 1, value_size = 64;
    long requests = 20000, keyspace = 100000;
    double set_ratio = 0.5;
    unsigned seed = 7;
};

// Reads exactly `count` replies from the socket, returning false on disconnect or an error reply.
class ReplyReader {
public:
    explicit ReplyReader(socket_t s) : s_(s) {}
    bool read(int count) {
        while (count > 0) {
            const std::size_t used = one_reply();
            if (used == 0) {
                if (!fill()) return false;
                continue;
            }
            if (buf_[pos_] == '-') return false;
            pos_ += used;
            --count;
        }
        if (pos_ > (1 << 20)) { buf_.erase(0, pos_); pos_ = 0; }
        return true;
    }

private:
    std::size_t one_reply() const {
        if (pos_ >= buf_.size()) return 0;
        const std::size_t eol = buf_.find("\r\n", pos_);
        if (eol == std::string::npos) return 0;
        const char t = buf_[pos_];
        if (t == '+' || t == '-' || t == ':') return eol + 2 - pos_;
        if (t == '$') {
            const long len = std::stol(buf_.substr(pos_ + 1, eol - pos_ - 1));
            if (len < 0) return eol + 2 - pos_;
            const std::size_t need = eol + 2 + static_cast<std::size_t>(len) + 2;
            return buf_.size() >= need ? need - pos_ : 0;
        }
        throw std::runtime_error("unexpected reply type");
    }
    bool fill() {
        char tmp[64 * 1024];
        const int n = net::recv_some(s_, tmp, sizeof tmp);
        if (n == 0) return false;
        buf_.append(tmp, static_cast<std::size_t>(n));
        return true;
    }
    socket_t s_;
    std::string buf_;
    std::size_t pos_ = 0;
};

double pct(std::vector<double>& v, double p) {
    if (v.empty()) return 0;
    const std::size_t i = std::min(v.size() - 1, static_cast<std::size_t>(p * static_cast<double>(v.size())));
    std::nth_element(v.begin(), v.begin() + static_cast<std::ptrdiff_t>(i), v.end());
    return v[i];
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("missing value for " + a);
            return argv[++i];
        };
        if (a == "--host") o.host = next();
        else if (a == "--port") o.port = static_cast<std::uint16_t>(std::stoi(next()));
        else if (a == "--clients") o.clients = std::stoi(next());
        else if (a == "--requests") o.requests = std::stol(next());
        else if (a == "--pipeline") o.pipeline = std::stoi(next());
        else if (a == "--value-size") o.value_size = std::stoi(next());
        else if (a == "--keyspace") o.keyspace = std::stol(next());
        else if (a == "--set-ratio") o.set_ratio = std::stod(next());
        else if (a == "--seed") o.seed = static_cast<unsigned>(std::stoul(next()));
        else if (a == "--label") o.label = next();
        else { std::fprintf(stderr, "unknown option %s\n", a.c_str()); return 2; }
    }
    net::Init sockets;
    std::vector<std::vector<double>> lat(static_cast<std::size_t>(o.clients));
    std::atomic<long> failures{0};
    std::vector<std::thread> threads;
    const auto start = Clock::now();
    for (int c = 0; c < o.clients; ++c) {
        threads.emplace_back([&, c] {
            try {
                const socket_t s = net::connect_to(o.host, o.port);
                ReplyReader reader(s);
                std::mt19937_64 rng(o.seed * 1000003u + static_cast<unsigned>(c));
                std::uniform_int_distribution<long> key(0, o.keyspace - 1);
                std::bernoulli_distribution is_set(o.set_ratio);
                const std::string value(static_cast<std::size_t>(o.value_size), 'v');
                auto& mine = lat[static_cast<std::size_t>(c)];
                mine.reserve(static_cast<std::size_t>(o.requests / o.pipeline + 1));
                std::string batch;
                for (long sent = 0; sent < o.requests;) {
                    batch.clear();
                    const int n = static_cast<int>(std::min<long>(o.pipeline, o.requests - sent));
                    for (int k = 0; k < n; ++k) {
                        const std::string kname = "key:" + std::to_string(key(rng));
                        batch += is_set(rng) ? encode_command({"SET", kname, value}) : encode_command({"GET", kname});
                    }
                    const auto t0 = Clock::now();
                    if (!net::send_all(s, batch) || !reader.read(n)) { ++failures; break; }
                    mine.push_back(std::chrono::duration<double, std::micro>(Clock::now() - t0).count());
                    sent += n;
                }
                close_socket(s);
            } catch (const std::exception& e) {
                std::fprintf(stderr, "client %d: %s\n", c, e.what());
                ++failures;
            }
        });
    }
    for (auto& t : threads) t.join();
    const double secs = std::chrono::duration<double>(Clock::now() - start).count();
    std::vector<double> all;
    for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
    const double ops = static_cast<double>(o.clients) * static_cast<double>(o.requests) / secs;
    std::printf("{\"label\":\"%s\",\"clients\":%d,\"pipeline\":%d,\"requests_per_client\":%ld,\"set_ratio\":%.2f,"
                "\"value_size\":%d,\"seconds\":%.3f,\"ops_per_sec\":%.0f,\"p50_us\":%.1f,\"p99_us\":%.1f,\"p999_us\":%.1f,"
                "\"failures\":%ld}\n",
                o.label.c_str(), o.clients, o.pipeline, o.requests, o.set_ratio, o.value_size, secs, ops,
                pct(all, 0.50), pct(all, 0.99), pct(all, 0.999), failures.load());
    return failures.load() == 0 ? 0 : 1;
}
