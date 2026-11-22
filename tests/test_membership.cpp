#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "lossy_socket.hpp"
#include "membership.hpp"
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

static ReliableConfig rcfg() {
    ReliableConfig c;
    c.initial_rto_ms = 50; c.min_rto_ms = 30; c.max_rto_ms = 400; c.max_retries = 40;
    return c;
}
static MembershipConfig mcfg() {
    MembershipConfig c;
    c.heartbeat_ms = 200;
    c.timeout_ms = 1500;
    c.join_retry_ms = 200;
    c.join_attempts = 2;
    c.use_broadcast = false;  // tests use bootstrap addresses only
    return c;
}

struct MNode {
    LossySocket sock;
    uint32_t id;
    std::string name;
    std::atomic<int> joined{ 0 }, left{ 0 }, crashed{ 0 };
    std::unique_ptr<ReliableEndpoint> ep;  // must be declared before ms
    std::unique_ptr<Membership> ms;        // destroyed first

    MNode(uint32_t id_, const std::string& nm, double loss, std::optional<net::Addr> bootstrap,
        uint16_t port = 0)
        : sock(Impairment{ loss, 0, 0, 0, id_ }), id(id_), name(nm) {
        sock.bind(port);
        ep.reset(new ReliableEndpoint(sock, id, rcfg()));
        ms.reset(new Membership(*ep, id, name, mcfg()));
        ms->set_event_handler([this](PeerEvent e, const PeerInfo&) {
            if (e == PeerEvent::Joined) ++joined;
            else if (e == PeerEvent::Left) ++left;
            else ++crashed;
            });
        ep->set_deliver_handler([this](const net::Addr& from, const proto::Packet& p) { ms->handle(from, p); });
        ep->set_unreachable_handler([this](const net::Addr& a) { ms->on_unreachable(a); });
        ep->start();
        ms->start(bootstrap);
    }
    // Shutdown order matters: stop the network thread FIRST so no handler can run
    // while the objects it points to are being destroyed.
    ~MNode() {
        ep->stop();
        ms->stop();
    }
    net::Addr addr() const { return net::Addr::loopback(sock.local_port()); }
    std::set<uint32_t> peer_ids() const {
        std::set<uint32_t> s;
        for (auto& p : ms->peers()) s.insert(p.id);
        return s;
    }
};

int main() {
    const double LOSS = 0.10;

    std::printf("[1] four peers join via one bootstrap peer (10%% packet loss)\n");
    std::vector<std::unique_ptr<MNode>> n(5);
    n[1].reset(new MNode(1, "Ali", LOSS, std::nullopt));
    n[2].reset(new MNode(2, "Sara", LOSS, n[1]->addr()));
    n[3].reset(new MNode(3, "Omar", LOSS, n[1]->addr()));
    n[4].reset(new MNode(4, "Hina", LOSS, n[1]->addr()));
    bool ok = wait_for([&] {
        for (int i = 1; i <= 4; ++i) if (n[i]->peer_ids().size() != 3) return false;
        return true;
        }, 15000);
    CHECK(ok);
    for (int i = 1; i <= 4; ++i) {
        auto ids = n[i]->peer_ids();
        for (int j = 1; j <= 4; ++j) CHECK((j == i) != (ids.count(j) == 1));
    }
    CHECK(n[4]->ms->find_by_name("Sara").has_value());
    CHECK(n[1]->ms->member_ids().size() == 4);
    std::printf("    everybody knows everybody (not only the bootstrap peer)\n");

    std::printf("[2] a peer crashes (killed without goodbye)\n");
    {
        auto t0 = std::chrono::steady_clock::now();
        n[3].reset();
        bool det = wait_for([&] {
            return n[1]->crashed >= 1 && n[2]->crashed >= 1 && n[4]->crashed >= 1;
            }, 8000);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        CHECK(det);
        std::printf("    all survivors detected the crash after %lld ms (timeout = 1500 ms)\n",
            static_cast<long long>(ms));
        CHECK(ms < 4000);
        for (int i : {1, 2, 4}) {
            CHECK(n[i]->peer_ids().size() == 2);
            CHECK(n[i]->peer_ids().count(3) == 0);
            CHECK(n[i]->left == 0);
        }
    }

    std::printf("[3] a peer leaves gracefully\n");
    {
        auto t0 = std::chrono::steady_clock::now();
        n[4]->ms->leave();
        bool det = wait_for([&] { return n[1]->left >= 1 && n[2]->left >= 1; }, 3000);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        CHECK(det);
        std::printf("    others noticed the LEAVE after %lld ms\n", static_cast<long long>(ms));
        n[4].reset();
        for (int i : {1, 2}) CHECK(n[i]->peer_ids().size() == 1);
    }

    std::printf("[4] a new peer joins later\n");
    n[0].reset(new MNode(5, "Zara", LOSS, n[2]->addr()));  // bootstrap through Sara this time
    ok = wait_for([&] {
        return n[0]->peer_ids().size() == 2 && n[1]->peer_ids().size() == 2 && n[2]->peer_ids().size() == 2;
        }, 10000);
    CHECK(ok);
    CHECK(n[1]->peer_ids().count(5) == 1);

    std::printf("[5] a peer restarts on the same port with a new id\n");
    {
        uint16_t port = n[2]->sock.local_port();
        n[2].reset();  // crash
        n[2].reset(new MNode(6, "Sara2", LOSS, n[1]->addr(), port));
        ok = wait_for([&] {
            auto a = n[1]->peer_ids(), b = n[0]->peer_ids(), c = n[2]->peer_ids();
            return a == std::set<uint32_t>{5, 6}&& b == std::set<uint32_t>{1, 6}&&
                c == std::set<uint32_t>{1, 5};
            }, 10000);
        CHECK(ok);
    }

    std::printf("[6] malformed membership packets are ignored\n");
    {
        net::UdpSocket raw;
        raw.bind(0);
        auto t = n[1]->addr();
        raw.send_to(t, proto::pack(proto::JOIN, 999, 0, 0, { 200, 'a' }));               // name longer than payload
        raw.send_to(t, proto::pack(proto::JOIN_REPLY, 998, 0, 0, { 0, 0, 0xFF, 0xFF }));  // count 65535, no entries
        raw.send_to(t, proto::pack(proto::HEARTBEAT, 997, 0, 0, {}));                   // empty payload
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        auto ids = n[1]->peer_ids();
        CHECK(ids.count(999) == 0 && ids.count(998) == 0 && ids.count(997) == 0);
        CHECK(ids == std::set<uint32_t>({ 5, 6 }));
    }

    if (failures == 0) std::printf("ALL TESTS PASSED\n");
    return failures == 0 ? 0 : 1;
}