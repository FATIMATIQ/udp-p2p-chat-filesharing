// net.hpp - Thin UDP socket wrapper. Works with Winsock (Windows) and POSIX (Linux/macOS).
#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#ifndef SIO_UDP_CONNRESET
#define SIO_UDP_CONNRESET _WSAIOW(IOC_VENDOR, 12)
#endif
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace net {

#ifdef _WIN32
    using sock_t = SOCKET;
    using socklen = int;
    inline const sock_t BAD_SOCK = INVALID_SOCKET;
    inline void close_sock(sock_t s) { ::closesocket(s); }
    struct WsaInit {
        WsaInit() { WSADATA d; if (WSAStartup(MAKEWORD(2, 2), &d) != 0) throw std::runtime_error("WSAStartup failed"); }
        ~WsaInit() { WSACleanup(); }
    };
    inline void net_init() { static WsaInit w; }
#else
    using sock_t = int;
    using socklen = socklen_t;
    inline const sock_t BAD_SOCK = -1;
    inline void close_sock(sock_t s) { ::close(s); }
    inline void net_init() {}
#endif

    // IPv4 address + port. ip is stored in HOST byte order.
    struct Addr {
        uint32_t ip = 0;
        uint16_t port = 0;

        bool operator==(const Addr& o) const { return ip == o.ip && port == o.port; }
        bool operator!=(const Addr& o) const { return !(*this == o); }
        bool operator<(const Addr& o) const { return ip != o.ip ? ip < o.ip : port < o.port; }

        std::string str() const {
            char b[32];
            std::snprintf(b, sizeof b, "%u.%u.%u.%u:%u", (ip >> 24) & 255, (ip >> 16) & 255,
                (ip >> 8) & 255, ip & 255, port);
            return b;
        }
        // Parses "a.b.c.d:port". Returns false on failure.
        static bool parse(const std::string& s, Addr& out) {
            size_t c = s.rfind(':');
            if (c == std::string::npos) return false;
            in_addr a;
            if (inet_pton(AF_INET, s.substr(0, c).c_str(), &a) != 1) return false;
            int p = std::atoi(s.substr(c + 1).c_str());
            if (p <= 0 || p > 65535) return false;
            out.ip = ntohl(a.s_addr);
            out.port = static_cast<uint16_t>(p);
            return true;
        }
        static Addr make(uint8_t a, uint8_t b, uint8_t c, uint8_t d, uint16_t port) {
            Addr r;
            r.ip = (uint32_t(a) << 24) | (uint32_t(b) << 16) | (uint32_t(c) << 8) | d;
            r.port = port;
            return r;
        }
        static Addr loopback(uint16_t port) { return make(127, 0, 0, 1, port); }
        static Addr broadcast(uint16_t port) { return make(255, 255, 255, 255, port); }
    };

    class UdpSocket {
    public:
        UdpSocket() {
            net_init();
            s_ = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
            if (s_ == BAD_SOCK) throw std::runtime_error("socket() failed");
#ifdef _WIN32
            // Without this, Windows reports an error on recvfrom() after sending to a dead peer.
            DWORD bytes = 0;
            BOOL off = FALSE;
            WSAIoctl(s_, SIO_UDP_CONNRESET, &off, sizeof off, nullptr, 0, &bytes, nullptr, nullptr);
#endif
        }
        virtual ~UdpSocket() { if (s_ != BAD_SOCK) close_sock(s_); }
        UdpSocket(const UdpSocket&) = delete;
        UdpSocket& operator=(const UdpSocket&) = delete;

        // port 0 = let the OS pick. reuse = allow several sockets on the same port.
        void bind(uint16_t port = 0, bool reuse = false) {
            if (reuse) {
                int one = 1;
                ::setsockopt(s_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one), sizeof one);
            }
            sockaddr_in sa{};
            sa.sin_family = AF_INET;
            sa.sin_addr.s_addr = htonl(INADDR_ANY);
            sa.sin_port = htons(port);
            if (::bind(s_, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0)
                throw std::runtime_error("bind() failed on port " + std::to_string(port));
        }

        uint16_t local_port() const {
            sockaddr_in sa{};
            socklen len = sizeof sa;
            ::getsockname(s_, reinterpret_cast<sockaddr*>(&sa), &len);
            return ntohs(sa.sin_port);
        }

        void enable_broadcast() {
            int one = 1;
            ::setsockopt(s_, SOL_SOCKET, SO_BROADCAST, reinterpret_cast<const char*>(&one), sizeof one);
        }

        void set_recv_buffer(int bytes) {
            ::setsockopt(s_, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&bytes), sizeof bytes);
        }

        // Sends one datagram. Returns true if the OS accepted it.
        virtual bool send_to(const Addr& to, const uint8_t* data, size_t len) {
            sockaddr_in sa{};
            sa.sin_family = AF_INET;
            sa.sin_addr.s_addr = htonl(to.ip);
            sa.sin_port = htons(to.port);
            int n = static_cast<int>(::sendto(s_, reinterpret_cast<const char*>(data),
                static_cast<int>(len), 0,
                reinterpret_cast<sockaddr*>(&sa), sizeof sa));
            return n == static_cast<int>(len);
        }
        bool send_to(const Addr& to, const std::vector<uint8_t>& v) { return send_to(to, v.data(), v.size()); }

        // Waits up to timeout_ms (-1 = forever) for a datagram.
        // Returns: >0 bytes received, 0 = timeout, -1 = error (caller may just continue).
        int recv_from(uint8_t* buf, size_t cap, Addr& from, int timeout_ms) {
            fd_set fds;
            FD_ZERO(&fds);
            FD_SET(s_, &fds);
            timeval tv;
            tv.tv_sec = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;
            int r = ::select(static_cast<int>(s_) + 1, &fds, nullptr, nullptr, timeout_ms < 0 ? nullptr : &tv);
            if (r == 0) return 0;
            if (r < 0) return -1;
            sockaddr_in sa{};
            socklen len = sizeof sa;
            int n = static_cast<int>(::recvfrom(s_, reinterpret_cast<char*>(buf), static_cast<int>(cap), 0,
                reinterpret_cast<sockaddr*>(&sa), &len));
            if (n < 0) return -1;
            from.ip = ntohl(sa.sin_addr.s_addr);
            from.port = ntohs(sa.sin_port);
            return n;
        }

    private:
        sock_t s_ = BAD_SOCK;
    };

}  // namespace net