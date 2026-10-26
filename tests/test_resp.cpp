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
        "PING\r\n",                                  // inline commands are not supported
        "*0\r\n",                                    // empty command
        "*1\r\n$-1\r\n",                             // null bulk as an argument
        "*1\r\n$3\r\nabcXY",                         // bulk not followed by CRLF
        "*1\r\n$x\r\n",                              // non-numeric length
        "*99999\r\n",                                // too many arguments
        "*1\r\n$999999999999\r\n",                   // absurd bulk length
        "*1\r\n$18446744073709551621\r\nhello\r\n",  // 20 digits: used to overflow and wrap to 5
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
    dispatch_now(store, {"set", "a", "1"}, out);
    dispatch_now(store, {"GET", "a"}, out);
    dispatch_now(store, {"GET", "missing"}, out);
    dispatch_now(store, {"EXISTS", "a", "missing", "a"}, out);
    dispatch_now(store, {"DEL", "a", "missing"}, out);
    dispatch_now(store, {"DBSIZE"}, out);
    dispatch_now(store, {"PING"}, out);
    dispatch_now(store, {"GET"}, out);
    dispatch_now(store, {"NOPE"}, out);
    dispatch_now(store, {"COMMAND", "DOCS"}, out);
    CHECK(out == "+OK\r\n$1\r\n1\r\n$-1\r\n:2\r\n:1\r\n:0\r\n+PONG\r\n"
                 "-ERR wrong number of arguments for 'GET' command\r\n-ERR unknown command 'NOPE'\r\n*0\r\n");
    std::string quit;
    CHECK(!dispatch_now(store, {"QUIT"}, quit));
}

static void client_text_cannot_forge_replies() {
    Store store;
    std::string out;
    dispatch_now(store, {"X\r\n+OK"}, out);
    CHECK(out == "-ERR unknown command 'X  +OK'\r\n");  // one line: CR and LF replaced by spaces
}

static void query_buffer_is_bounded() {
    RespParser p;
    const std::string chunk(1u << 20, 'x');
    p.feed("*1\r\n$67108864\r\n");
    for (int i = 0; i < 300 && p.error().empty(); ++i) p.feed(chunk);
    CHECK(p.error() == "query buffer limit exceeded");
    CHECK(p.buffered() <= RespParser::kMaxBuffered);
}

int main() {
    whole_and_byte_by_byte_parse_the_same();
    malformed_input_sets_an_error();
    incomplete_input_waits_without_error();
    dispatch_replies_like_redis();
    client_text_cannot_forge_replies();
    query_buffer_is_bounded();
    return finish("test_resp");
}
