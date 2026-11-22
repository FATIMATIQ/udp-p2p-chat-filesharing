#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "chat.hpp"
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
    c.heartbeat_ms = 200; c.timeout_ms = 3000; c.join_retry_ms = 200; c.join_attempts = 2;
    c.use_broadcast = false;
    return c;
}

struct Rec {
    uint32_t sender;
    bool priv;
    std::string text;
};

struct CNode {
    LossySocket sock;
    uint32_t id;
    std::string name;
    std::mutex m;
    std::vector<Rec> got;
    std::unique_ptr<ReliableEndpoint> ep;
    std::unique_ptr<Membership> ms;
    std::unique_ptr<Chat> chat;

    CNode(uint32_t id_, const std::string& nm, double loss, std::optional<net::Addr> bootstrap)
        : sock(Impairment{ loss, 0, 0, 0, id_ }), id(id_), name(nm) {
        sock.bind(0);
        ep.reset(new ReliableEndpoint(sock, id, rcfg()));
        ms.reset(new Membership(*ep, id, name, mcfg()));
        chat.reset(new Chat(*ep, *ms));
        chat->set_message_handler([this](const ChatMsg& c) {
            std::lock_guard<std::mutex> lk(m);
            got.push_back({ c.sender_id, c.is_private, c.text });
            });
        ep->set_deliver_handler([this](const net::Addr& from, const proto::Packet& p) {
            if (ms->handle(from, p)) return;
            chat->handle(from, p);
            });
        ep->set_unreachable_handler([this](const net::Addr& a) { ms->on_unreachable(a); });
        ep->start();
        ms->start(bootstrap);
    }
    ~CNode() {  // stop the network thread first
        ep->stop();
        ms->stop();
    }
    net::Addr addr() const { return net::Addr::loopback(sock.local_port()); }
    size_t count() {
        std::lock_guard<std::mutex> lk(m);
        return got.size();
    }
};

int main() {
    const double LOSS = 0.15;
    const int N = 4, GROUP_MSGS = 100, PRIV_MSGS = 20;

    std::printf("[1] 4 peers join (15%% loss)\n");
    std::vector<std::unique_ptr<CNode>> n;
    n.emplace_back(new CNode(1, "Ali", LOSS, std::nullopt));
    for (uint32_t i = 2; i <= 4; ++i)
        n.emplace_back(new CNode(i, i == 2 ? "Sara" : i == 3 ? "Omar" : "Hina", LOSS, n[0]->addr()));
    CHECK(wait_for([&] {
        for (auto& x : n) if (x->ms->peers().size() != 3) return false;
        return true;
        }, 15000));

    std::printf("[2] everybody sends %d group messages and %d private messages (15%% loss)\n",
        GROUP_MSGS, PRIV_MSGS);
    // interleave senders so streams from different senders mix
    for (int k = 0; k < GROUP_MSGS; ++k)
        for (int s = 0; s < N; ++s) {
            n[s]->chat->send_group("G" + std::to_string(s) + "#" + std::to_string(k));
            if (k < PRIV_MSGS) {
                int t = (s + 1) % N;
                n[s]->chat->send_private(n[t]->name, "P" + std::to_string(s) + ">" +
                    std::to_string(t) + "#" + std::to_string(k));
            }
        }
    const size_t expect = (N - 1) * GROUP_MSGS + PRIV_MSGS;  // 3 senders' groups + 1 sender's privates
    bool ok = wait_for([&] {
        for (auto& x : n) if (x->count() < expect) return false;
        return true;
        }, 60000);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));  // let stray duplicates show up
    CHECK(ok);

    for (int r = 0; r < N; ++r) {
        std::lock_guard<std::mutex> lk(n[r]->m);
        CHECK(n[r]->got.size() == expect);
        // per-sender FIFO and no duplicates
        std::map<uint32_t, int> next_g, next_p;
        for (auto& rec : n[r]->got) {
            int s = static_cast<int>(rec.sender) - 1;
            if (!rec.priv) {
                CHECK(rec.text == "G" + std::to_string(s) + "#" + std::to_string(next_g[rec.sender]));
                ++next_g[rec.sender];
            }
            else {
                CHECK(s == (r + N - 1) % N);  // private messages only from the previous peer
                CHECK(rec.text == "P" + std::to_string(s) + ">" + std::to_string(r) + "#" +
                    std::to_string(next_p[rec.sender]));
                ++next_p[rec.sender];
            }
        }
        for (int s = 0; s < N; ++s)
            if (s != r) CHECK(next_g[static_cast<uint32_t>(s + 1)] == GROUP_MSGS);
        CHECK(next_p[static_cast<uint32_t>((r + N - 1) % N + 1)] == PRIV_MSGS);
    }
    std::printf("    every peer received all messages, in order per sender, exactly once\n");

    std::printf("[3] private message to unknown name fails cleanly\n");
    CHECK(!n[0]->chat->send_private("Nobody", "hi"));

    std::printf("[4] the same message replayed is shown only once\n");
    {
        size_t before = n[0]->count();
        proto::Packet pkt;
        pkt.type = proto::CHAT;
        pkt.sender_id = 4242;
        proto::Writer w;
        w.u32(7); w.u8(0); w.u16(5); w.bytes(reinterpret_cast<const uint8_t*>("hello"), 5);
        pkt.payload = w.data();
        n[0]->chat->handle(net::Addr::loopback(1), pkt);
        n[0]->chat->handle(net::Addr::loopback(1), pkt);  // duplicate
        CHECK(n[0]->count() == before + 1);
        CHECK(n[0]->chat->duplicates_ >= 1);
    }

    std::printf("[5] malformed chat payloads are ignored\n");
    {
        size_t before = n[0]->count();
        proto::Packet pkt;
        pkt.type = proto::CHAT;
        pkt.sender_id = 5555;
        pkt.payload = { 0, 0, 0, 1, 0, 0xFF, 0xFF, 'x' };  // claims 65535 text bytes
        n[0]->chat->handle(net::Addr::loopback(1), pkt);
        pkt.payload = { 0, 0, 0, 2, 9, 0, 1, 'x' };  // bad kind
        n[0]->chat->handle(net::Addr::loopback(1), pkt);
        pkt.payload = {};  // empty
        n[0]->chat->handle(net::Addr::loopback(1), pkt);
        CHECK(n[0]->count() == before);
    }

    std::printf("[6] over-long text is truncated, not rejected\n");
    {
        size_t before = n[1]->count();
        n[0]->chat->send_group(std::string(5000, 'z'));
        CHECK(wait_for([&] { return n[1]->count() > before; }, 5000));
        std::lock_guard<std::mutex> lk(n[1]->m);
        CHECK(n[1]->got.back().text.size() == Chat::MAX_TEXT);
    }

    if (failures == 0) std::printf("ALL TESTS PASSED\n");
    return failures == 0 ? 0 : 1;
}