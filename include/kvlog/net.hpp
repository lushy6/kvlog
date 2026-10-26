// Minimal portable TCP sockets (Winsock on Windows, BSD sockets elsewhere).
#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
using socket_t = SOCKET;
inline constexpr socket_t kBadSocket = INVALID_SOCKET;
inline void close_socket(socket_t s) { ::closesocket(s); }
#else
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
using socket_t = int;
inline constexpr socket_t kBadSocket = -1;
inline void close_socket(socket_t s) { ::close(s); }
#endif

namespace kvlog::net {

struct Init {
    Init() {
#ifdef _WIN32
        WSADATA d;
        if (WSAStartup(MAKEWORD(2, 2), &d) != 0) throw std::runtime_error("WSAStartup failed");
#endif
    }
    ~Init() {
#ifdef _WIN32
        WSACleanup();
#endif
    }
};

inline void no_delay(socket_t s) {
    int one = 1;
    ::setsockopt(s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one), sizeof one);
}

inline socket_t listen_on(const std::string& host, std::uint16_t port, int backlog = 512) {
    socket_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == kBadSocket) throw std::runtime_error("socket() failed");
    int one = 1;
    ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one), sizeof one);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) throw std::runtime_error("bad host " + host);
    if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0) throw std::runtime_error("bind failed on port " + std::to_string(port));
    if (::listen(s, backlog) != 0) throw std::runtime_error("listen failed");
    return s;
}

inline socket_t connect_to(const std::string& host, std::uint16_t port) {
    socket_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == kBadSocket) throw std::runtime_error("socket() failed");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) throw std::runtime_error("bad host " + host);
    if (::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0) {
        close_socket(s);
        throw std::runtime_error("connect failed to " + host + ":" + std::to_string(port));
    }
    no_delay(s);
    return s;
}

#ifdef MSG_NOSIGNAL
inline constexpr int kSendFlags = MSG_NOSIGNAL;  // a vanished peer must not raise SIGPIPE and kill the server
#else
inline constexpr int kSendFlags = 0;
#endif

inline bool interrupted() {
#ifdef _WIN32
    return false;
#else
    return errno == EINTR;
#endif
}

// Sends all of `data`; false if the peer went away.
inline bool send_all(socket_t s, std::string_view data) {
    while (!data.empty()) {
        const int n = ::send(s, data.data(), static_cast<int>(data.size() > (1 << 30) ? (1 << 30) : data.size()), kSendFlags);
        if (n < 0 && interrupted()) continue;
        if (n <= 0) return false;
        data.remove_prefix(static_cast<std::size_t>(n));
    }
    return true;
}

// Receives up to `cap` bytes; 0 on orderly close or error.
inline int recv_some(socket_t s, char* buf, int cap) {
    for (;;) {
        const int n = ::recv(s, buf, cap, 0);
        if (n < 0 && interrupted()) continue;
        return n < 0 ? 0 : n;
    }
}

}  // namespace kvlog::net
