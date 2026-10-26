#include <string>

#include "check.hpp"
#include "kvlog/commands.hpp"
#include "kvlog/resp.hpp"

using namespace kvlog;

static void whole_and_byte_by_byte_parse_the_same() {
    const Command a{"SET", "key", std::string("va\r\nlue\0x", 9)};  // binary-safe: CRLF and NUL inside a value
    const Command b{"GET", "key"};
    const std::string wire = encode_command(a) + encode_command(b);

    RespParser whole;
    whole.feed(wire);
    CHECK(whole.next() == a);
    CHECK(whole.next() == b);
    CHECK(!whole.next());

    RespParser drip;
    std::vector<Command> got;
    for (char c : wire) {
        drip.feed(std::string_view(&c, 1));
        while (auto cmd = drip.next()) got.push_back(*cmd);
    }
    CHECK(got.size() == 2 && got[0] == a && got[1] == b);
    CHECK(drip.error().empty());
}

static void malformed_input_sets_an_error() {
    const char* bad[] = {
        "PING\r\n",                       // inline commands are not supported
        "*0\r\n",                         // empty command
        "*1\r\n$-1\r\n",                  // null bulk as an argument
        "*1\r\n$3\r\nabcXY",              // bulk not followed by CRLF
        "*1\r\n$x\r\n",                   // non-numeric length
        "*99999\r\n",                     // too many arguments
        "*1\r\n$999999999999\r\n",        // absurd bulk length
    };
    for (const char* b : bad) {
        RespParser p;
        p.feed(b);
        CHECK(!p.next());
        CHECK(!p.error().empty());
    }
}

static void incomplete_input_waits_without_error() {
    RespParser p;
    p.feed("*2\r\n$3\r\nGET\r\n$3\r\nke");
    CHECK(!p.next());
    CHECK(p.error().empty());
    p.feed("y\r\n");
    auto cmd = p.next();
    CHECK(cmd && (*cmd)[1] == "key");
}

static void dispatch_replies_like_redis() {
    Store store;
    std::string out;
    dispatch(store, {"set", "a", "1"}, out);
    dispatch(store, {"GET", "a"}, out);
    dispatch(store, {"GET", "missing"}, out);
    dispatch(store, {"EXISTS", "a", "missing", "a"}, out);
    dispatch(store, {"DEL", "a", "missing"}, out);
    dispatch(store, {"DBSIZE"}, out);
    dispatch(store, {"PING"}, out);
    dispatch(store, {"GET"}, out);
    dispatch(store, {"NOPE"}, out);
    CHECK(out == "+OK\r\n$1\r\n1\r\n$-1\r\n:2\r\n:1\r\n:0\r\n+PONG\r\n"
                 "-ERR wrong number of arguments for 'GET' command\r\n-ERR unknown command 'NOPE'\r\n");
    std::string quit;
    CHECK(!dispatch(store, {"QUIT"}, quit));
}

int main() {
    whole_and_byte_by_byte_parse_the_same();
    malformed_input_sets_an_error();
    incomplete_input_waits_without_error();
    dispatch_replies_like_redis();
    return finish("test_resp");
}
