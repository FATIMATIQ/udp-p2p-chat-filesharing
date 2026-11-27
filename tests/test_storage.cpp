#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "lossy_socket.hpp"
#include "membership.hpp"
#include "net.hpp"
#include "protocol.hpp"
#include "reliable.hpp"
#include "sha256.hpp"
#include "storage.hpp"

using namespace storage;

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
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
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
    c.heartbeat_ms = 200; c.timeout_ms = 2000; c.join_retry_ms = 200; c.join_attempts = 2;
    c.use_broadcast = false;
    return c;
}
static StorageConfig scfg() {
    StorageConfig c;
    c.reconcile_ms = 200; c.replica_update_ms = 500; c.fetch_delay_ms = 1500; c.fetch_timeout_ms = 1500;
    return c;
}

struct SNode {
    LossySocket sock;
    uint32_t id;
    std::string name;
    std::string dir;
    std::unique_ptr<ReliableEndpoint> ep;
    std::unique_ptr<Membership> ms;
    std::unique_ptr<Storage> st;

    SNode(uint32_t id_, const std::string& nm, double loss, const std::string& base,
        std::optional<net::Addr> bootstrap)
        : sock(Impairment{ loss, 0, 0, 0, id_ }), id(id_), name(nm), dir(base + "/" + nm) {
        sock.bind(0);
        ep.reset(new ReliableEndpoint(sock, id, rcfg()));
        ms.reset(new Membership(*ep, id, name, mcfg()));
        st.reset(new Storage(*ep, *ms, dir, scfg()));
        ms->set_event_handler([this](PeerEvent e, const PeerInfo& p) { st->on_peer_event(e, p); });
        ep->set_deliver_handler([this](const net::Addr& from, const proto::Packet& p) {
            if (ms->handle(from, p)) return;
            st->handle(from, p);
            });
        ep->set_unreachable_handler([this](const net::Addr& a) { ms->on_unreachable(a); });
        ep->start();
        st->start();
        ms->start(bootstrap);
    }
    ~SNode() {
        ep->stop();
        ms->stop();
        st->stop();
    }
    net::Addr addr() const { return net::Addr::loopback(sock.local_port()); }
};

static void make_file(const std::string& path, size_t size, uint64_t seed) {
    std::mt19937_64 g(seed);
    std::vector<uint8_t> d(size);
    for (auto& b : d) b = static_cast<uint8_t>(g());
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(d.data()), static_cast<std::streamsize>(d.size()));
}

static bool chunk_on_disk(const std::string& node_dir, uint64_t file, uint32_t idx) {
    return fs::exists(fs::path(node_dir) / hex64(file) / (std::to_string(idx) + ".chunk"));
}

int main() {
    // ============ 1. SHA-256 known answers ============
    std::printf("[1] SHA-256 test vectors\n");
    {
        auto h = [](const std::string& s) {
            return crypto::Sha256::hex(crypto::Sha256::hash(reinterpret_cast<const uint8_t*>(s.data()), s.size()));
            };
        CHECK(h("") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        CHECK(h("abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        CHECK(h("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq") ==
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
        crypto::Sha256 s;  // one million 'a', fed in odd-sized pieces
        std::string piece(997, 'a');
        size_t left = 1000000;
        while (left) {
            size_t k = std::min(left, piece.size());
            s.update(reinterpret_cast<const uint8_t*>(piece.data()), k);
            left -= k;
        }
        CHECK(crypto::Sha256::hex(s.finish()) == "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
    }

    // ============ 2. placement properties ============
    std::printf("[2] rendezvous-hashing placement\n");
    {
        std::vector<uint32_t> m4{ 11, 22, 33, 44 }, m5{ 11, 22, 33, 44, 55 }, m3_no33{ 11, 22, 44 };
        const uint64_t F = 0xABCDEF0123456789ULL;
        const uint32_t N = 2000;
        auto p4 = place_file(F, N, m4, 2), p4b = place_file(F, N, m4, 2);
        CHECK(p4 == p4b);  // deterministic
        std::map<uint32_t, int> load;
        bool distinct = true;
        for (auto& o : p4) {
            if (o.size() != 2 || o[0] == o[1]) distinct = false;
            for (uint32_t id : o) ++load[id];
        }
        CHECK(distinct);
        for (uint32_t id : m4) CHECK(load[id] > 850 && load[id] < 1150);  // ~ N*R/M = 1000 each
        // adding a peer only moves chunks TO the new peer
        auto p5 = place_file(F, N, m5, 2);
        int moved = 0;
        bool only_to_new = true;
        for (uint32_t c = 0; c < N; ++c)
            if (p5[c] != p4[c]) {
                ++moved;
                if (!contains(p5[c], 55)) only_to_new = false;
            }
        CHECK(only_to_new);
        CHECK(moved > 600 && moved < 1000);  // ~ 2/5 of the chunks
        // removing a peer only changes chunks that peer held
        auto pr = place_file(F, N, m3_no33, 2);
        bool only_its = true;
        for (uint32_t c = 0; c < N; ++c)
            if (pr[c] != p4[c] && !contains(p4[c], 33)) only_its = false;
        CHECK(only_its);
        // nobody holds a complete file (3 peers, 8 chunks, many different files)
        bool nobody_full = true;
        for (uint64_t f = 1; f <= 300; ++f) {
            auto pl = place_file(f * 7919, 8, { 1, 2, 3 }, 2);
            std::map<uint32_t, int> cnt;
            for (auto& o : pl)
                for (uint32_t id : o) ++cnt[id];
            for (auto& kv : cnt)
                if (kv.second == 8) nobody_full = false;
            for (auto& o : pl)
                if (o.size() != 2 || o[0] == o[1]) nobody_full = false;
        }
        CHECK(nobody_full);
    }

    // ============ 3. four peers share 2 files of 2 MB each, 10% packet loss ============
    const std::string base = (fs::temp_directory_path() /
        ("p2p_storage_test_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()))).string();
    fs::create_directories(base + "/shared");
    const double LOSS = 0.10;
    const size_t MB2 = 2 * 1024 * 1024;
    std::printf("[3] 4 peers each share 2 files of 2 MB (10%% loss), temp dir %s\n", base.c_str());
    std::vector<std::unique_ptr<SNode>> n;
    n.reserve(8);  // we keep pointers to the elements, so the vector must never reallocate
    n.emplace_back(new SNode(101, "Ali", LOSS, base, std::nullopt));
    const char* names[] = { "Ali", "Sara", "Omar", "Hina" };
    for (uint32_t i = 1; i < 4; ++i) n.emplace_back(new SNode(101 + i * 17, names[i], LOSS, base, n[0]->addr()));
    CHECK(wait_for([&] {
        for (auto& x : n) if (x->ms->peers().size() != 3) return false;
        return true;
        }, 15000));

    std::vector<std::vector<std::string>> paths(4);
    std::vector<std::string> shared_err(8);
    for (int i = 0; i < 4; ++i)
        for (int k = 0; k < 2; ++k) {
            std::string p = base + "/shared/" + names[i] + std::to_string(k) + ".bin";
            make_file(p, MB2 + (k ? 12345 : 0), 1000 + i * 10 + k);  // second file is not a multiple of the chunk size
            paths[i].push_back(p);
        }
    auto t0 = std::chrono::steady_clock::now();
    {
        std::vector<std::thread> th;
        for (int i = 0; i < 4; ++i)
            th.emplace_back([&, i] {
            for (int k = 0; k < 2; ++k) shared_err[i * 2 + k] = n[i]->st->share_file(paths[i][k]);
                });
        for (auto& t : th) t.join();
    }
    for (auto& e : shared_err) CHECK(e.empty());
    CHECK(!n[0]->st->share_file(paths[0][0]).empty());  // sharing the same file twice is refused

    // every node must learn all 8 files and see >= 2 holders for every chunk
    auto all_replicated = [&](std::vector<std::unique_ptr<SNode>*>& nodes, size_t want_files, size_t min_copies) {
        for (auto* np : nodes) {
            auto infos = (*np)->st->file_infos();
            if (infos.size() != want_files) return false;
            for (auto& fi : infos)
                for (auto& h : fi.holders)
                    if (h.size() < min_copies) return false;
        }
        return true;
        };
    std::vector<std::unique_ptr<SNode>*> alive;
    for (auto& x : n) alive.push_back(&x);
    bool ok = wait_for([&] { return all_replicated(alive, 8, 2); }, 90000);
    double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    CHECK(ok);
    std::printf("    8 files (16 MB) distributed and replicated x2 in %.1f s\n", secs);

    // let the placement settle (surplus copies dropped), then inspect the DISKS
    std::this_thread::sleep_for(std::chrono::seconds(4));
    auto infos0 = n[0]->st->file_infos();
    auto audit = [&](const std::vector<SNode*>& nodes, const char* label, bool exact2) {
        int lost = 0, under = 0, over = 0, complete_holders = 0, corrupt = 0;
        for (auto& fi : infos0) {
            std::map<SNode*, uint32_t> held;
            for (uint32_t c = 0; c < fi.meta.nchunks; ++c) {
                int copies = 0;
                for (auto* nd : nodes) {
                    if (!chunk_on_disk(nd->dir, fi.meta.id, c)) continue;
                    ++copies;
                    ++held[nd];
                    // check content against manifest
                    std::ifstream f(fs::path(nd->dir) / hex64(fi.meta.id) / (std::to_string(c) + ".chunk"), std::ios::binary);
                    std::vector<uint8_t> d((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
                    if (crypto::Sha256::hash(d) != fi.meta.chunk_sha[c]) ++corrupt;
                }
                if (copies == 0) ++lost;
                if (copies < 2) ++under;
                if (copies > 2) ++over;
            }
            for (auto& kv : held)
                if (kv.second == fi.meta.nchunks) ++complete_holders;
        }
        std::printf("    [%s] chunks lost=%d under-replicated=%d over-replicated=%d corrupt=%d peers-with-full-file=%d\n",
            label, lost, under, over, corrupt, complete_holders);
        CHECK(lost == 0);
        CHECK(under == 0);
        CHECK(corrupt == 0);
        CHECK(complete_holders == 0);
        if (exact2) CHECK(over == 0);
        };
    {
        std::vector<SNode*> nodes;
        for (auto& x : n) nodes.push_back(x.get());
        audit(nodes, "4 peers", true);
        for (auto& x : n) {
            size_t mine = 0, total = 0;
            for (auto& fi : x->st->file_infos()) { mine += fi.mine; total += fi.meta.nchunks; }
            std::printf("    %s holds %zu of %zu chunks (%.0f%%)\n", x->name.c_str(), mine, total, 100.0 * mine / total);
        }
    }

    // ============ 4. a new peer joins and takes over its share ============
    std::printf("[4] a 5th peer joins: it receives the file list and takes over its share\n");
    n.emplace_back(new SNode(555, "Zara", LOSS, base, n[2]->addr()));
    alive.push_back(&n.back());
    ok = wait_for([&] {
        auto infos = n[4]->st->file_infos();
        if (infos.size() != 8) return false;
        size_t mine = 0;
        for (auto& fi : infos) mine += fi.mine;
        return mine >= 40;  // roughly 40% of 8*~33 chunks
        }, 60000);
    CHECK(ok);
    {
        // exact expectation: the chunks Zara must hold are those where she is in the top-2 of 5 peers
        std::vector<uint32_t> members{ 101, 118, 135, 152, 555 };
        std::sort(members.begin(), members.end());
        size_t expected = 0;
        for (auto& fi : infos0) {
            auto pl = place_file(fi.meta.id, fi.meta.nchunks, members, 2);
            for (auto& o : pl) if (contains(o, 555)) ++expected;
        }
        ok = wait_for([&] {
            size_t mine = 0;
            for (auto& fi : n[4]->st->file_infos()) mine += fi.mine;
            return mine == expected;
            }, 30000);
        CHECK(ok);
        std::printf("    Zara now holds exactly the %zu chunks the placement assigns to her\n", expected);
    }
    ok = wait_for([&] { return all_replicated(alive, 8, 2); }, 30000);
    CHECK(ok);
    std::this_thread::sleep_for(std::chrono::seconds(4));  // old owners drop what they no longer own
    {
        std::vector<SNode*> nodes;
        for (auto& x : n) nodes.push_back(x.get());
        audit(nodes, "5 peers", true);
    }

    // ============ 5. a peer crashes: chunks are re-replicated ============
    std::printf("[5] peer 'Omar' crashes: surviving peers restore 2 copies of every chunk\n");
    {
        auto tc = std::chrono::steady_clock::now();
        n[2].reset();
        std::vector<std::unique_ptr<SNode>*> surv{ &n[0], &n[1], &n[3], &n[4] };
        // first wait until every survivor has noticed the crash (otherwise the dead peer still counts as a holder)
        ok = wait_for([&] {
            for (auto* x : surv) if ((*x)->ms->peers().size() != 3) return false;
            return true;
            }, 15000);
        CHECK(ok);
        double s_detect = std::chrono::duration<double>(std::chrono::steady_clock::now() - tc).count();
        uint64_t fetched_before = 0;
        for (auto* x : surv) fetched_before += (*x)->st->stats().chunks_stored;
        ok = wait_for([&] { return all_replicated(surv, 8, 2); }, 60000);
        double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - tc).count();
        CHECK(ok);
        uint64_t fetched_after = 0;
        for (auto* x : surv) fetched_after += (*x)->st->stats().chunks_stored;
        std::printf("    crash detected after %.1f s, full 2x replication restored after %.1f s (%llu chunks re-replicated)\n",
            s_detect, s, (unsigned long long)(fetched_after - fetched_before));
        CHECK(fetched_after > fetched_before);  // chunks really were copied again
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::vector<SNode*> nodes{ n[0].get(), n[1].get(), n[3].get(), n[4].get() };
        audit(nodes, "4 survivors", false);
    }

    // ============ 6. request one chunk from a specific peer ============
    std::printf("[6] request_chunk(): fetch a chunk from a chosen holder\n");
    {
        SNode* asker = n[0].get();
        FileInfo target;
        uint32_t cidx = 0;
        bool found = false;
        for (auto& fi : asker->st->file_infos()) {
            for (uint32_t c = 0; c < fi.meta.nchunks && !found; ++c)
                if (!contains(fi.holders[c], asker->id) && !fi.holders[c].empty()) {
                    target = fi; cidx = c; found = true;
                }
            if (found) break;
        }
        CHECK(found);
        if (found) {
            std::mutex m;
            std::vector<uint8_t> got;
            std::atomic<bool> arrived{ false };
            asker->st->set_chunk_listener([&](uint64_t f, uint32_t i, const std::vector<uint8_t>& d, uint32_t) {
                if (f == target.meta.id && i == cidx) { std::lock_guard<std::mutex> lk(m); got = d; arrived = true; }
                });
            CHECK(asker->st->request_chunk(target.holders[cidx][0], target.meta.id, cidx));
            CHECK(wait_for([&] { return arrived.load(); }, 10000));
            std::lock_guard<std::mutex> lk(m);
            CHECK(crypto::Sha256::hash(got) == target.meta.chunk_sha[cidx]);
        }
    }

    // ============ 7. malformed storage packets are ignored ============
    std::printf("[7] malformed storage packets are ignored\n");
    {
        size_t files_before = n[0]->st->file_infos().size();
        net::UdpSocket raw;
        raw.bind(0);
        auto t = n[0]->addr();
        const uint8_t t_types[] = { proto::FILE_ANNOUNCE, proto::CHUNK_STORE, proto::CHUNK_DATA,
                                   proto::CHUNK_REQUEST, proto::CHUNK_NACK, proto::REPLICA_UPDATE };
        for (uint8_t ty : t_types) {
            raw.send_to(t, proto::pack(ty, 777, 0, 0, {}));
            raw.send_to(t, proto::pack(ty, 777, 0, 0, { 1, 2, 3, 4, 5 }));
            std::vector<uint8_t> junk(200);
            for (size_t i = 0; i < junk.size(); ++i) junk[i] = static_cast<uint8_t>(i * 31 + ty);
            raw.send_to(t, proto::pack(ty, 777, 0, 0, junk));
        }
        // a FILE_ANNOUNCE whose file_id does not match its hash
        proto::Writer w;
        w.u64(12345); w.u64(100000); w.u32(65536); w.u32(2); w.u32(1); w.str8("evil.bin");
        std::vector<uint8_t> sha(32, 0xAB);
        w.bytes(sha); w.u32(0); w.u16(0);
        raw.send_to(t, proto::pack(proto::FILE_ANNOUNCE, 777, 0, 0, w.data()));
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        CHECK(n[0]->st->file_infos().size() == files_before);
    }

    n.clear();
    std::error_code ec;
    fs::remove_all(base, ec);
    if (failures == 0) std::printf("ALL TESTS PASSED\n");
    return failures == 0 ? 0 : 1;
}