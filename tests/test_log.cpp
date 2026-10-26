#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "check.hpp"
#include "kvlog/log.hpp"

using namespace kvlog;
namespace fs = std::filesystem;

static void write_all(const fs::path& p, std::string_view s) {
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f.write(s.data(), static_cast<std::streamsize>(s.size()));
}

static std::map<std::string, std::string> replay(const fs::path& p, RecoveryReport* report = nullptr) {
    std::map<std::string, std::string> m;
    auto r = recover(p, [&](const Record& rec) {
        if (rec.op == Op::Set) m[std::string(rec.key)] = std::string(rec.value);
        else m.erase(std::string(rec.key));
    });
    if (report) *report = r;
    return m;
}

static void records_round_trip() {
    for (auto [op, k, v] : std::vector<std::tuple<Op, std::string, std::string>>{
             {Op::Set, "k", "v"}, {Op::Set, "", ""}, {Op::Del, "gone", ""}, {Op::Set, std::string(300, 'x'), std::string("\0\r\n", 3)}}) {
        const std::string rec = encode_record(op, k, v);
        Record out{};
        CHECK(decode_record(rec, 0, out) == rec.size());
        CHECK(out.op == op && out.key == k && out.value == v);
    }
}

// The core durability property: cutting the file at ANY byte keeps exactly the records that were
// complete before the cut, and truncates the file back to that boundary.
static void a_cut_at_every_offset_recovers_the_complete_prefix() {
    std::vector<std::string> recs = {encode_record(Op::Set, "a", "1"), encode_record(Op::Set, "b", "22"),
                                     encode_record(Op::Del, "a"), encode_record(Op::Set, "c", std::string(40, 'z'))};
    std::string full;
    std::vector<std::size_t> boundaries{0};
    for (auto& r : recs) { full += r; boundaries.push_back(full.size()); }
    const fs::path p = "cut.aof";
    for (std::size_t cut = 0; cut <= full.size(); ++cut) {
        write_all(p, std::string_view(full).substr(0, cut));
        RecoveryReport rep;
        replay(p, &rep);
        std::size_t complete = 0;
        while (complete + 1 < boundaries.size() && boundaries[complete + 1] <= cut) ++complete;
        CHECK(rep.records == complete);
        CHECK(rep.valid_bytes == boundaries[complete]);
        CHECK(fs::file_size(p) == boundaries[complete]);
    }
    fs::remove(p);
}

// Damage in the middle (a valid record follows it) must not be "repaired" by deleting later records.
static void mid_log_corruption_refuses_to_truncate() {
    const std::string a = encode_record(Op::Set, "a", "1"), b = encode_record(Op::Set, "b", "2"),
                      c = encode_record(Op::Set, "c", "3");
    std::string data = a + b + c;
    data[a.size() + 10] ^= 0x01;  // flip a bit inside record b's payload; c is intact after it
    const fs::path p = "corrupt.aof";
    write_all(p, data);
    bool refused = false;
    try {
        replay(p);
    } catch (const std::runtime_error&) {
        refused = true;
    }
    CHECK(refused);
    CHECK(fs::file_size(p) == data.size());  // nothing deleted
    fs::remove(p);
}

// Damage in the last record (nothing valid after it) is a torn tail: truncate it, keep the rest.
static void corrupt_last_record_is_treated_as_a_torn_tail() {
    const std::string a = encode_record(Op::Set, "a", "1"), b = encode_record(Op::Set, "b", "2");
    std::string data = a + b;
    data[a.size() + 10] ^= 0x01;
    const fs::path p = "tail.aof";
    write_all(p, data);
    RecoveryReport rep;
    auto m = replay(p, &rep);
    CHECK(rep.records == 1 && m.size() == 1 && m.count("a"));
    CHECK(fs::file_size(p) == a.size());
    fs::remove(p);
}

#ifdef __linux__
// /dev/full fails every write with ENOSPC: the writer must refuse to acknowledge, then stay failed.
static void write_errors_are_sticky_and_never_acknowledged() {
    LogWriter w("/dev/full", SyncPolicy::Always);
    bool first_failed = false, later_failed = false;
    try {
        w.wait_durable(w.append(encode_record(Op::Set, "k", "v")));
    } catch (const LogFailed&) {
        first_failed = true;
    }
    try {
        w.append(encode_record(Op::Set, "k2", "v"));
    } catch (const LogFailed&) {
        later_failed = true;
    }
    CHECK(first_failed);
    CHECK(later_failed);
    CHECK(w.failed());
}
#endif

static void writer_appends_after_a_truncated_recovery() {
    const fs::path p = "append.aof";
    write_all(p, encode_record(Op::Set, "a", "1") + "\x05\x00\x00");  // torn tail
    replay(p);
    {
        LogWriter w(p, SyncPolicy::Always);
        w.wait_durable(w.append(encode_record(Op::Set, "b", "2")));
    }
    RecoveryReport rep;
    auto m = replay(p, &rep);
    CHECK(rep.records == 2 && rep.truncated_bytes == 0);
    CHECK(m == (std::map<std::string, std::string>{{"a", "1"}, {"b", "2"}}));
    fs::remove(p);
}

static void rewrite_replaces_the_log_atomically() {
    const fs::path p = "rewrite.aof";
    fs::remove(p);
    {
        LogWriter w(p, SyncPolicy::Never);
        for (int i = 0; i < 100; ++i) w.append(encode_record(Op::Set, "k", std::to_string(i)));
        const auto before = w.size();
        w.rewrite([](const std::function<void(std::string_view)>& emit) { emit(encode_record(Op::Set, "k", "99")); });
        w.append(encode_record(Op::Set, "after", "x"));
        CHECK(w.size() < before);
    }
    auto m = replay(p);
    CHECK(m == (std::map<std::string, std::string>{{"k", "99"}, {"after", "x"}}));
    CHECK(!fs::exists(fs::path(p).concat(".rewrite")));
    fs::remove(p);
}

int main() {
    records_round_trip();
    a_cut_at_every_offset_recovers_the_complete_prefix();
    mid_log_corruption_refuses_to_truncate();
    corrupt_last_record_is_treated_as_a_torn_tail();
#ifdef __linux__
    write_errors_are_sticky_and_never_acknowledged();
#endif
    writer_appends_after_a_truncated_recovery();
    rewrite_replaces_the_log_atomically();
    return finish("test_log");
}
