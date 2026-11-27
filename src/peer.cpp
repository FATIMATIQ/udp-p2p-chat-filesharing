// peer.cpp - The peer program. Run one copy per participant.
//
//   peer --name Ali --port 5001 --first
//   peer --name Sara --port 5002 --bootstrap 127.0.0.1:5001
//
// Options:
//   --name <n>          display name (default peer<port>)
//   --port <p>          UDP port to listen on (default 5000)
//   --bootstrap <ip:p>  address of any peer already in the group
//   --first             I am the first peer: do not wait for broadcast discovery
//   --discovery-port <p> port that broadcast JOINs are sent to (default 5000)
//   --loss <0..1>       simulate packet loss on everything this peer sends (testing/demo)
//   --delay <ms>        simulate extra send delay (testing/demo)
//   --window <n>        reliable-layer window size, 1..64 (default 32)
//   --heartbeat <ms>    heartbeat interval (default 1000)
//   --timeout <ms>      declare a peer dead after this much silence (default 4000)
//   --store <dir>       where this peer keeps its chunks (default store/<name>_<port>)
//   --share <file>      share a file at startup (can be repeated)
#include <atomic>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <map>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <vector>
#include <string>
#include <thread>

#include "chat.hpp"
#include "lossy_socket.hpp"
#include "membership.hpp"
#include "net.hpp"
#include "protocol.hpp"
#include "reliable.hpp"
#include "storage.hpp"

static std::atomic<bool> g_quit{ false };
static void on_sigint(int) { g_quit = true; }

static std::mutex g_out;
static void say(const std::string& s) {
    std::lock_guard<std::mutex> lk(g_out);
    std::cout << s << std::endl;
}

struct Args {
    std::string name;
    uint16_t port = 5000;
    std::optional<net::Addr> bootstrap;
    bool first = false;
    uint16_t discovery_port = 5000;
    double loss = 0;
    int delay_ms = 0;
    int window = 32;
    int heartbeat_ms = 1000;
    int timeout_ms = 4000;
    std::string store_dir;
    std::vector<std::string> share;
};

static void usage() {
    std::cout << "usage: peer [--name N] [--port P] [--bootstrap ip:port | --first]\n"
        "            [--discovery-port P] [--loss 0..1] [--delay ms] [--window n]\n"
        "            [--heartbeat ms] [--timeout ms] [--store dir] [--share file]...\n";
}

static bool parse_args(int argc, char** argv, Args& a) {
    for (int i = 1; i < argc; ++i) {
        std::string k = argv[i];
        auto val = [&]() -> const char* { return i + 1 < argc ? argv[++i] : nullptr; };
        if (k == "--first") { a.first = true; continue; }
        const char* v = val();
        if (!v) { std::cout << "missing value for " << k << "\n"; return false; }
        if (k == "--name") a.name = v;
        else if (k == "--port") a.port = static_cast<uint16_t>(std::atoi(v));
        else if (k == "--bootstrap") {
            net::Addr b;
            if (!net::Addr::parse(v, b)) { std::cout << "bad --bootstrap (use ip:port)\n"; return false; }
            a.bootstrap = b;
        }
        else if (k == "--discovery-port") a.discovery_port = static_cast<uint16_t>(std::atoi(v));
        else if (k == "--loss") a.loss = std::atof(v);
        else if (k == "--delay") a.delay_ms = std::atoi(v);
        else if (k == "--window") a.window = std::atoi(v);
        else if (k == "--heartbeat") a.heartbeat_ms = std::atoi(v);
        else if (k == "--timeout") a.timeout_ms = std::atoi(v);
        else if (k == "--store") a.store_dir = v;
        else if (k == "--share") a.share.push_back(v);
        else { std::cout << "unknown option " << k << "\n"; return false; }
    }
    if (a.port == 0) { std::cout << "bad --port\n"; return false; }
    if (a.name.empty()) a.name = "peer" + std::to_string(a.port);
    if (a.store_dir.empty()) a.store_dir = "store/" + a.name + "_" + std::to_string(a.port);
    return true;
}

static uint32_t random_id(uint16_t port) {
    std::random_device rd;
    uint32_t id = rd();
    id ^= static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    id ^= static_cast<uint32_t>(port) << 16;
    return id == 0 ? 1 : id;
}

static std::string hex(uint32_t v) {
    char b[16];
    std::snprintf(b, sizeof b, "%08x", v);
    return b;
}

static std::string human_size(uint64_t b) {
    char buf[32];
    if (b >= 1024 * 1024) std::snprintf(buf, sizeof buf, "%.2f MB", b / 1048576.0);
    else std::snprintf(buf, sizeof buf, "%.1f KB", b / 1024.0);
    return buf;
}

// creates a file of `mb` megabytes with pseudo-random content (for demos and tests)
static bool generate_file(const std::string& path, size_t bytes, uint64_t seed) {
    std::mt19937_64 g(seed);
    std::ofstream f(path, std::ios::binary);
    if (!f) return false;
    std::vector<uint8_t> blk(65536);
    for (size_t done = 0; done < bytes;) {
        size_t n = std::min(blk.size(), bytes - done);
        for (size_t i = 0; i < n; ++i) blk[i] = static_cast<uint8_t>(g());
        f.write(reinterpret_cast<const char*>(blk.data()), static_cast<std::streamsize>(n));
        done += n;
    }
    return static_cast<bool>(f);
}

int main(int argc, char** argv) {
    Args args;
    if (!parse_args(argc, argv, args)) { usage(); return 1; }
    std::signal(SIGINT, on_sigint);
#ifdef SIGTERM
    std::signal(SIGTERM, on_sigint);
#endif

    uint32_t my_id = random_id(args.port);  // NEW random id on every start (required by membership)

    try {
        std::unique_ptr<net::UdpSocket> sock;
        if (args.loss > 0 || args.delay_ms > 0) {
            Impairment imp;
            imp.loss = args.loss;
            imp.delay_min_ms = args.delay_ms;
            imp.delay_max_ms = args.delay_ms;
            imp.seed = my_id;
            sock.reset(new LossySocket(imp));
        }
        else {
            sock.reset(new net::UdpSocket());
        }
        sock->bind(args.port, true);
        sock->enable_broadcast();
        sock->set_recv_buffer(4 * 1024 * 1024);

        ReliableConfig rc;
        rc.window = static_cast<size_t>(args.window);
        ReliableEndpoint ep(*sock, my_id, rc);

        MembershipConfig mc;
        mc.heartbeat_ms = args.heartbeat_ms;
        mc.timeout_ms = args.timeout_ms;
        mc.discovery_port = args.discovery_port;
        mc.use_broadcast = !args.first;
        Membership ms(ep, my_id, args.name, mc);
        Chat chat(ep, ms);
        storage::Storage st(ep, ms, args.store_dir);

        ms.set_event_handler([&](PeerEvent e, const PeerInfo& p) {
            const char* what = e == PeerEvent::Joined ? "joined" : e == PeerEvent::Left ? "left" : "CRASHED";
            say("*** " + p.name + " " + what + " (" + p.addr.str() + ")");
            st.on_peer_event(e, p);
            });
        chat.set_message_handler([](const ChatMsg& m) {
            say(m.is_private ? "[" + m.sender_name + " -> you] " + m.text : "[" + m.sender_name + "] " + m.text);
            });
        ep.set_deliver_handler([&](const net::Addr& from, const proto::Packet& p) {
            if (ms.handle(from, p)) return;
            if (chat.handle(from, p)) return;
            st.handle(from, p);
            });
        ep.set_unreachable_handler([&](const net::Addr& a) { ms.on_unreachable(a); });

        ep.start();
        st.start();
        if (args.bootstrap)
            ms.start(args.bootstrap);
        else
            ms.start(std::nullopt);

        say("Peer '" + args.name + "' (id " + hex(my_id) + ") listening on UDP port " +
            std::to_string(sock->local_port()));
        say(args.bootstrap ? "Joining via bootstrap " + args.bootstrap->str() + " ..."
            : (args.first ? "Starting as the first peer." : "Looking for peers by broadcast ..."));
        say("Type /help for commands. Anything else is sent to the whole group.");

        for (auto& path : args.share) {
            std::string err = st.share_file(path);
            say(err.empty() ? "Shared " + path : "Cannot share " + path + ": " + err);
        }

        // ---- command line runs in its own thread so Ctrl+C can always stop us ----
        std::thread([&] {
            std::string line;
            while (std::getline(std::cin, line)) {
                if (line.empty()) continue;
                if (line[0] != '/') {
                    size_t n = chat.send_group(line);
                    if (n == 0) say("(nobody else is here yet)");
                    continue;
                }
                std::istringstream is(line);
                std::string cmd;
                is >> cmd;
                if (cmd == "/quit" || cmd == "/exit") {
                    g_quit = true;
                    break;
                }
                else if (cmd == "/help") {
                    say("/peers                list known peers\n"
                        "/msg <name> <text>    private message\n"
                        "/gen <name> <MB>      create a random test file shared/<name>\n"
                        "/share <path>         share a file with the group\n"
                        "/files                list shared files and how they are stored\n"
                        "/chunks <n>           which peers hold each chunk of file number n\n"
                        "/stats                reliability and storage statistics\n"
                        "/quit                 leave the group and exit");
                }
                else if (cmd == "/peers") {
                    auto ps = ms.peers();
                    std::string out = "Peers (" + std::to_string(ps.size() + 1) + " incl. me):\n  " + args.name +
                        "  " + hex(my_id) + "  (me)";
                    for (auto& p : ps) out += "\n  " + p.name + "  " + hex(p.id) + "  " + p.addr.str();
                    say(out);
                }
                else if (cmd == "/msg") {
                    std::string to;
                    is >> to;
                    std::string text;
                    std::getline(is, text);
                    if (!text.empty() && text[0] == ' ') text.erase(0, 1);
                    if (to.empty() || text.empty()) say("usage: /msg <name> <text>");
                    else if (!chat.send_private(to, text)) say("no peer named '" + to + "'");
                }
                else if (cmd == "/gen") {
                    std::string nm;
                    double mb = 0;
                    is >> nm >> mb;
                    if (nm.empty() || mb <= 0) { say("usage: /gen <name> <MB>"); continue; }
                    std::error_code ec;
                    std::filesystem::create_directories("shared", ec);
                    std::string path = "shared/" + nm;
                    if (generate_file(path, static_cast<size_t>(mb * 1048576), my_id ^ std::hash<std::string>{}(nm)))
                        say("Created " + path + " (" + human_size(static_cast<uint64_t>(mb * 1048576)) + "). Share it with: /share " + path);
                    else say("could not create " + path);
                }
                else if (cmd == "/share") {
                    std::string path;
                    std::getline(is, path);
                    if (!path.empty() && path[0] == ' ') path.erase(0, 1);
                    if (path.empty()) { say("usage: /share <path>"); continue; }
                    say("Sharing " + path + " ... (splitting into chunks and sending replicas)");
                    uint64_t fid = 0;
                    std::string err = st.share_file(path, &fid);
                    say(err.empty() ? "Shared " + path + " (file id " + storage::hex64(fid) + ")" : "Cannot share: " + err);
                }
                else if (cmd == "/files" || cmd == "/chunks") {
                    auto infos = st.file_infos();
                    std::sort(infos.begin(), infos.end(), [](const storage::FileInfo& a, const storage::FileInfo& b) {
                        return a.meta.name != b.meta.name ? a.meta.name < b.meta.name : a.meta.id < b.meta.id;
                        });
                    std::map<uint32_t, std::string> nm;
                    nm[my_id] = args.name;
                    for (auto& p : ms.peers()) nm[p.id] = p.name;
                    if (cmd == "/files") {
                        if (infos.empty()) { say("(no shared files known yet)"); continue; }
                        std::string out;
                        int i = 0;
                        for (auto& fi : infos) {
                            size_t min_copies = ~size_t(0);
                            std::map<uint32_t, size_t> per_peer;
                            for (auto& h : fi.holders) {
                                min_copies = std::min(min_copies, h.size());
                                for (uint32_t id : h) ++per_peer[id];
                            }
                            out += "#" + std::to_string(++i) + "  " + fi.meta.name + "  " + human_size(fi.meta.size) +
                                "  id=" + storage::hex64(fi.meta.id) + "  " + std::to_string(fi.meta.nchunks) +
                                " chunks  min copies=" + std::to_string(min_copies) + "  mine=" + std::to_string(fi.mine) + "\n      held by:";
                            for (auto& kv : per_peer) out += " " + (nm.count(kv.first) ? nm[kv.first] : "?") + "(" + std::to_string(kv.second) + ")";
                            out += "\n";
                        }
                        say(out.substr(0, out.size() - 1));
                    }
                    else {
                        size_t n = 0;
                        is >> n;
                        if (n < 1 || n > infos.size()) { say("usage: /chunks <file number from /files>"); continue; }
                        auto& fi = infos[n - 1];
                        std::string out = fi.meta.name + ": chunk -> holders\n";
                        for (uint32_t c = 0; c < fi.meta.nchunks; ++c) {
                            out += "  " + std::to_string(c) + ": ";
                            for (size_t k = 0; k < fi.holders[c].size(); ++k)
                                out += (k ? "," : "") + (nm.count(fi.holders[c][k]) ? nm[fi.holders[c][k]] : "?");
                            if (fi.holders[c].empty()) out += "(none)";
                            out += (c % 4 == 3) ? "\n" : "   ";
                        }
                        say(out);
                    }
                }
                else if (cmd == "/stats") {
                    auto& s = ep.stats();
                    std::ostringstream o;
                    o << "reliable: sent=" << s.data_sent << " retransmits=" << s.retransmits
                        << " fast_retransmits=" << s.fast_retransmits << " delivered=" << s.delivered
                        << " duplicates_dropped=" << s.duplicates_dropped << " bad_packets=" << s.bad_packets
                        << "\nchat: sent=" << chat.sent_ << " received=" << chat.received_
                        << "\nstorage: stored=" << st.stats().chunks_stored << " dropped=" << st.stats().chunks_dropped
                        << " served=" << st.stats().chunks_served << " fetches=" << st.stats().fetches_started
                        << " bad=" << st.stats().bad_chunks;
                    say(o.str());
                }
                else {
                    say("unknown command (try /help)");
                }
            }
            g_quit = true;  // stdin closed
            }).detach();

        while (!g_quit) std::this_thread::sleep_for(std::chrono::milliseconds(100));

        say("Leaving the group ...");
        ms.leave();   // tell everyone, stop heartbeat thread
        ep.stop();    // stop the network thread before anything it points to is destroyed
        st.stop();
        std::cout.flush();
        std::_Exit(0);  // the input thread may still be blocked in getline(); skip normal teardown
    }
    catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << std::endl;
        return 2;
    }
    return 0;
}