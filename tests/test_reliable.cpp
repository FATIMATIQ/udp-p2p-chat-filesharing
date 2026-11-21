#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "lossy_socket.hpp"
#include "net.hpp"
#include "protocol.hpp"
#include "reliable.hpp"

static int failures = 0;
#define CHECK(cond)                                                    \
    do {                                                               \
        if (!(cond)) {                                                 \
            std::printf("FAIL line %d: %s\n", __LINE__, #cond);        \
            ++failures;                                                \
        }                                                              \
    } while (0)

static bool wait_for(const std::function<bool()>& pred, int timeout_ms) {
    auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < end) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

// Message payload: 4-byte big-endian counter, then `extra` filler bytes equal to (counter & 0xFF).
static std::vector<uint8_t> make_payload(uint32_t v, size_t extra = 0) {
    std::vector<uint8_t> p(4 + extra, static_cast<uint8_t>(v & 0xFF));
    proto::put32(p.data(), v);
    return p;
}

struct Node {
    LossySocket sock;
    uint32_t id;
    std::mutex m;
    std::map<uint32_t, std::vector<uint32_t>> got;  // sender id -> values in delivery order
    std::atomic<int> bad_payload{ 0 };
    std::atomic<int> heartbeats{ 0 };
    std::unique_ptr<ReliableEndpoint> ep;  // keep LAST so it is destroyed first

    Node(uint32_t id_, Impairment imp, ReliableConfig cfg, uint16_t port = 0) : sock(imp), id(id_) {
        sock.bind(port);
        ep.reset(new ReliableEndpoint(sock, id, cfg));
        ep->set_deliver_handler([this](const net::Addr&, const proto::Packet& p) {
            if (p.type == proto::HEARTBEAT) { ++heartbeats; return; }
            if (p.type != proto::CHAT || p.payload.size() < 4) return;
            uint32_t v = proto::get32(p.payload.data());
            for (size_t i = 4; i < p.payload.size(); ++i)
                if (p.payload[i] != static_cast<uint8_t>(v & 0xFF)) { ++bad_payload; break; }
            std::lock_guard<std::mutex> lk(m);
            got[p.sender_id].push_back(v);
            });
        ep->start();
    }
    net::Addr addr() const { return net::Addr::loopback(sock.local_port()); }
    size_t count_from(uint32_t sender) {
        std::lock_guard<std::mutex> lk(m);
        return got[sender].size();
    }
    // true if values from `sender` are exactly first, first+1, ..., first+n-1 (in order, no dups)
    bool exact_sequence(uint32_t sender, uint32_t first, size_t n) {
        std::lock_guard<std::mutex> lk(m);
        auto& v = got[sender];
        if (v.size() != n) return false;
        for (size_t i = 0; i < n; ++i)
            if (v[i] != first + i) return false;
        return true;
    }
};

static ReliableConfig fast_cfg() {
    ReliableConfig c;
    c.window = 32;
    c.initial_rto_ms = 50;
    c.min_rto_ms = 30;
    c.max_rto_ms = 400;
    c.max_retries = 40;
    return c;
}

int main() {
    // ---------- 1. two peers, 1000 messages each way, 20% loss + 10% duplicates + jitter ----------
    {
        std::printf("[1] 2 peers, 1000 msgs each way, 20%% loss, 10%% dup, 0-5ms jitter\n");
        Impairment ia; ia.loss = 0.2; ia.dup = 0.1; ia.delay_max_ms = 5; ia.seed = 1;
        Impairment ib = ia; ib.seed = 2;
        Node A(1, ia, fast_cfg()), B(2, ib, fast_cfg());
        auto t0 = std::chrono::steady_clock::now();
        for (uint32_t i = 0; i < 1000; ++i) {
            A.ep->send_reliable(B.addr(), proto::CHAT, make_payload(i));
            B.ep->send_reliable(A.addr(), proto::CHAT, make_payload(i));
        }
        bool ok = wait_for([&] { return B.count_from(1) >= 1000 && A.count_from(2) >= 1000; }, 60000);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));  // let any stray dup show up
        CHECK(ok);
        CHECK(B.exact_sequence(1, 0, 1000));
        CHECK(A.exact_sequence(2, 0, 1000));
        auto& s = A.ep->stats();
        std::printf("    done in %lld ms | A: sent=%llu timeout-retx=%llu fast-retx=%llu dups-dropped=%llu\n",
            static_cast<long long>(ms), (unsigned long long)s.data_sent.load(),
            (unsigned long long)s.retransmits.load(), (unsigned long long)s.fast_retransmits.load(),
            (unsigned long long)s.duplicates_dropped.load());
    }

    // ---------- 2. four peers, everyone sends 250 msgs to everyone, 15% loss ----------
    {
        std::printf("[2] 4 peers full mesh, 250 msgs per pair, 15%% loss\n");
        std::vector<std::unique_ptr<Node>> n;
        for (uint32_t i = 0; i < 4; ++i) {
            Impairment imp; imp.loss = 0.15; imp.seed = 100 + i;
            n.emplace_back(new Node(10 + i, imp, fast_cfg()));
        }
        for (uint32_t m = 0; m < 250; ++m)
            for (size_t i = 0; i < 4; ++i)
                for (size_t j = 0; j < 4; ++j)
                    if (i != j) n[i]->ep->send_reliable(n[j]->addr(), proto::CHAT, make_payload(m));
        bool ok = wait_for([&] {
            for (size_t j = 0; j < 4; ++j)
                for (size_t i = 0; i < 4; ++i)
                    if (i != j && n[j]->count_from(10 + i) < 250) return false;
            return true;
            }, 60000);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        CHECK(ok);
        for (size_t j = 0; j < 4; ++j)
            for (size_t i = 0; i < 4; ++i)
                if (i != j) CHECK(n[j]->exact_sequence(10 + i, 0, 250));
    }

    // ---------- 3. bulk: 1500 full-size packets (1180 B), 10% loss, integrity check ----------
    {
        std::printf("[3] bulk transfer: 1500 x 1180-byte packets, 10%% loss\n");
        Impairment imp; imp.loss = 0.1; imp.seed = 7;
        Node A(1, imp, fast_cfg()), B(2, Impairment{}, fast_cfg());
        auto t0 = std::chrono::steady_clock::now();
        for (uint32_t i = 0; i < 1500; ++i)
            A.ep->send_reliable(B.addr(), proto::CHAT, make_payload(i, proto::MAX_PAYLOAD - 4));
        bool ok = wait_for([&] { return B.count_from(1) >= 1500; }, 60000);
        double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
        CHECK(ok);
        CHECK(B.exact_sequence(1, 0, 1500));
        CHECK(B.bad_payload == 0);
        std::printf("    %.1f s, goodput %.2f MB/s\n", secs, 1500.0 * proto::MAX_PAYLOAD / 1e6 / secs);
    }

    // ---------- 4. unreachable peer is reported ----------
    {
        std::printf("[4] unreachable peer detection\n");
        ReliableConfig cfg;
        cfg.initial_rto_ms = 20; cfg.min_rto_ms = 20; cfg.max_rto_ms = 50; cfg.max_retries = 3;
        LossySocket sock; sock.bind(0);
        ReliableEndpoint ep(sock, 1, cfg);
        std::atomic<bool> fired{ false };
        ep.set_unreachable_handler([&](const net::Addr&) { fired = true; });
        ep.start();
        net::UdpSocket sink;  // bound but nobody reads or answers
        sink.bind(0);
        ep.send_reliable(net::Addr::loopback(sink.local_port()), proto::CHAT, make_payload(1));
        CHECK(wait_for([&] { return fired.load(); }, 5000));
        CHECK(ep.stats().retransmits >= 3);
    }

    // ---------- 5. a peer restarts on the same port with a new id ----------
    {
        std::printf("[5] peer restart (new session) is handled\n");
        Node A(1, Impairment{}, fast_cfg());
        std::unique_ptr<Node> B(new Node(2, Impairment{}, fast_cfg()));
        uint16_t portB = B->sock.local_port();
        for (uint32_t i = 0; i < 5; ++i) A.ep->send_reliable(B->addr(), proto::CHAT, make_payload(i));
        CHECK(wait_for([&] { return B->count_from(1) >= 5; }, 5000));
        B.reset();  // "crash"
        B.reset(new Node(3, Impairment{}, fast_cfg(), portB));  // restart, new id
        B->ep->send_unreliable(A.addr(), proto::HEARTBEAT, {});  // announces itself
        CHECK(wait_for([&] { return A.heartbeats.load() >= 1; }, 5000));
        for (uint32_t i = 100; i < 105; ++i) A.ep->send_reliable(B->addr(), proto::CHAT, make_payload(i));
        CHECK(wait_for([&] { return B->count_from(1) >= 5; }, 5000));
        CHECK(B->exact_sequence(1, 100, 5));
    }

    // ---------- 6. corrupted garbage does not crash anything ----------
    {
        std::printf("[6] garbage datagrams are ignored\n");
        Node A(1, Impairment{}, fast_cfg());
        net::UdpSocket raw;
        raw.bind(0);
        uint8_t junk[50];
        for (int i = 0; i < 50; ++i) junk[i] = static_cast<uint8_t>(i * 7);
        for (int i = 0; i < 20; ++i) raw.send_to(A.addr(), junk, sizeof junk);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        CHECK(A.ep->stats().bad_packets >= 1);
    }

    if (failures == 0) std::printf("ALL TESTS PASSED\n");
    return failures == 0 ? 0 : 1;
}