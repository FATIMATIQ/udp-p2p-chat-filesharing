#include <chrono>
#include <cstdio>

#include "lossy_socket.hpp"
#include "net.hpp"

static int failures = 0;
#define CHECK(cond)                                                    \
    do {                                                               \
        if (!(cond)) {                                                 \
            std::printf("FAIL line %d: %s\n", __LINE__, #cond);        \
            ++failures;                                                \
        }                                                              \
    } while (0)

// Sends `total` packets in batches and counts how many arrive at `rx`.
static int send_and_count(net::UdpSocket& tx, net::UdpSocket& rx, int total) {
    net::Addr dest = net::Addr::loopback(rx.local_port());
    uint8_t msg[4] = { 1, 2, 3, 4 };
    uint8_t buf[2048];
    net::Addr from;
    int received = 0;
    for (int sent = 0; sent < total; sent += 100) {
        for (int i = 0; i < 100; ++i) tx.send_to(dest, msg, sizeof msg);
        while (rx.recv_from(buf, sizeof buf, from, 50) > 0) ++received;
    }
    return received;
}

int main() {
    // --- address parsing ---
    {
        net::Addr a;
        CHECK(net::Addr::parse("192.168.1.5:5001", a));
        CHECK(a.ip == 0xC0A80105u && a.port == 5001);
        CHECK(a.str() == "192.168.1.5:5001");
        CHECK(!net::Addr::parse("garbage", a));
        CHECK(!net::Addr::parse("1.2.3.4:99999", a));
    }

    // --- plain send / receive on loopback ---
    {
        net::UdpSocket a, b;
        a.bind(0);
        b.bind(0);
        uint8_t msg[] = { 'h', 'i' };
        CHECK(a.send_to(net::Addr::loopback(b.local_port()), msg, 2));
        uint8_t buf[64];
        net::Addr from;
        int n = b.recv_from(buf, sizeof buf, from, 1000);
        CHECK(n == 2 && buf[0] == 'h' && buf[1] == 'i');
        CHECK(from.port == a.local_port());
    }

    // --- timeout returns 0 ---
    {
        net::UdpSocket a;
        a.bind(0);
        uint8_t buf[64];
        net::Addr from;
        CHECK(a.recv_from(buf, sizeof buf, from, 50) == 0);
    }

    // --- no impairment: everything arrives ---
    {
        LossySocket tx;
        net::UdpSocket rx;
        rx.bind(0);
        CHECK(send_and_count(tx, rx, 1000) == 1000);
    }

    // --- 100% loss ---
    {
        LossySocket tx(Impairment{ 1.0 });
        net::UdpSocket rx;
        rx.bind(0);
        CHECK(send_and_count(tx, rx, 200) == 0);
        CHECK(tx.dropped == 200);
    }

    // --- ~50% loss ---
    {
        LossySocket tx(Impairment{ 0.5 });
        net::UdpSocket rx;
        rx.bind(0);
        int got = send_and_count(tx, rx, 1000);
        std::printf("50%% loss: received %d / 1000\n", got);
        CHECK(got > 400 && got < 600);
    }

    // --- 100% duplication ---
    {
        Impairment cfg;
        cfg.dup = 1.0;
        LossySocket tx(cfg);
        net::UdpSocket rx;
        rx.bind(0);
        CHECK(send_and_count(tx, rx, 100) == 200);
    }

    // --- delay ---
    {
        Impairment cfg;
        cfg.delay_min_ms = 100;
        cfg.delay_max_ms = 100;
        LossySocket tx(cfg);
        net::UdpSocket rx;
        rx.bind(0);
        uint8_t msg[1] = { 7 };
        auto t0 = std::chrono::steady_clock::now();
        tx.send_to(net::Addr::loopback(rx.local_port()), msg, 1);
        uint8_t buf[16];
        net::Addr from;
        CHECK(rx.recv_from(buf, sizeof buf, from, 20) == 0);  // not there yet
        CHECK(rx.recv_from(buf, sizeof buf, from, 1000) == 1);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        std::printf("delayed packet arrived after %lld ms\n", static_cast<long long>(ms));
        CHECK(ms >= 90);
    }

    if (failures == 0) std::printf("ALL TESTS PASSED\n");
    return failures == 0 ? 0 : 1;
}