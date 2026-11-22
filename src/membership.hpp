// membership.hpp - Peer discovery and group membership without any central server.
//
// Messages (payload formats, all integers big-endian):
//   JOIN       (unreliable) : [u8 nlen][name]                      "I am new, here is my name"
//   JOIN_REPLY (reliable)   : [u8 nlen][name]                      replier's name, then
//                             [u16 count] count x ( [u32 id][u32 ip][u16 port][u8 nlen][name] )
//                             = every other peer the replier knows (never the requester)
//   HEARTBEAT  (unreliable) : [u8 nlen][name]                      sent to every known peer each second
//   LEAVE      (unreliable) : (empty)                              graceful goodbye, sent several times
//
// Join procedure:
//   1. New peer sends JOIN to the bootstrap peer (or broadcasts it), retrying until answered.
//   2. Each receiver adds the newcomer and answers with JOIN_REPLY (its peer list).
//   3. The newcomer adds everybody listed and sends them a JOIN, so they learn about it too.
//   After that the bootstrap peer is not needed any more.
//
// Failure detection: any packet from a peer refreshes its "last seen" time.
// No packet for timeout_ms => the peer is declared crashed and removed.
// A peer id that was declared dead (crashed or left) is never accepted again;
// a restarted peer must use a NEW random id.
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "codec.hpp"
#include "net.hpp"
#include "protocol.hpp"
#include "reliable.hpp"

struct MembershipConfig {
    int heartbeat_ms = 1000;
    int timeout_ms = 4000;       // silence longer than this => peer is dead
    int join_retry_ms = 500;
    int join_attempts = 6;       // broadcast mode only: after this many tries assume we are first
    uint16_t discovery_port = 5000;  // port that broadcast JOINs are sent to
    bool use_broadcast = true;   // used only when no bootstrap address is given
};

struct PeerInfo {
    uint32_t id = 0;
    net::Addr addr;
    std::string name;
};

enum class PeerEvent { Joined, Left, Crashed };

class Membership {
public:
    // Called from internal threads. Must be thread-safe and must not block.
    using EventFn = std::function<void(PeerEvent, const PeerInfo&)>;

    Membership(ReliableEndpoint& ep, uint32_t my_id, std::string name, MembershipConfig cfg = {})
        : ep_(ep), my_id_(my_id), name_(name.substr(0, 32)), cfg_(cfg) {
    }
    ~Membership() { stop(); }
    Membership(const Membership&) = delete;
    Membership& operator=(const Membership&) = delete;

    void set_event_handler(EventFn f) { event_fn_ = std::move(f); }

    // bootstrap: address of any peer already in the group. Empty => broadcast (or first peer).
    void start(std::optional<net::Addr> bootstrap = std::nullopt) {
        if (running_.exchange(true)) return;
        bootstrap_ = bootstrap;
        auto now = Clock::now();
        next_join_ = now;
        next_hb_ = now;
        thread_ = std::thread([this] { run(); });
    }
    void stop() {
        if (!running_.exchange(false)) return;
        cv_.notify_all();
        if (thread_.joinable()) thread_.join();
    }
    // Graceful exit: tell everybody, then stop.
    void leave() {
        if (!running_) return;
        std::vector<net::Addr> addrs;
        {
            std::lock_guard<std::mutex> lk(m_);
            for (auto& kv : peers_) addrs.push_back(kv.second.info.addr);
        }
        for (int i = 0; i < 5; ++i) {
            for (auto& a : addrs) ep_.send_unreliable(a, proto::LEAVE, {});
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        stop();
    }

    // Feed every packet received from the network here.
    // Returns true if the packet was a membership message (consumed).
    bool handle(const net::Addr& from, const proto::Packet& p) {
        bool membership_type = p.type == proto::JOIN || p.type == proto::JOIN_REPLY ||
            p.type == proto::HEARTBEAT || p.type == proto::LEAVE;
        std::vector<Ev> evs;
        std::vector<net::Addr> to_join;
        {
            std::lock_guard<std::mutex> lk(m_);
            auto now = Clock::now();
            auto it = peers_.find(p.sender_id);
            if (it != peers_.end() && it->second.info.addr == from) it->second.last_seen = now;
            if (!membership_type) return false;
            if (dead_.count(p.sender_id)) return true;  // ignore: this id was declared dead

            proto::Reader r(p.payload);
            switch (p.type) {
            case proto::JOIN: {
                std::string nm = r.str8();
                if (!r.good() || nm.empty()) break;  // malformed: drop whole packet
                add_peer(p.sender_id, from, nm, evs);
                send_join_reply(from, p.sender_id);
                break;
            }
            case proto::JOIN_REPLY: {
                std::string nm = r.str8();
                uint16_t count = r.u16();
                if (!r.good() || nm.empty()) break;
                // parse EVERYTHING first; if the packet is truncated or malformed, drop it all
                std::vector<PeerInfo> listed;
                for (uint16_t i = 0; i < count; ++i) {
                    PeerInfo pi;
                    pi.id = r.u32();
                    pi.addr.ip = r.u32();
                    pi.addr.port = r.u16();
                    pi.name = r.str8();
                    if (!r.good()) break;
                    listed.push_back(pi);
                }
                if (!r.good()) break;
                add_peer(p.sender_id, from, nm, evs);
                joined_ = true;
                for (auto& pi : listed) {
                    if (pi.id == 0 || pi.addr.port == 0 || pi.name.empty() || pi.id == my_id_) continue;
                    if (dead_.count(pi.id) || peers_.count(pi.id)) continue;
                    if (add_peer(pi.id, pi.addr, pi.name, evs)) to_join.push_back(pi.addr);
                }
                break;
            }
            case proto::HEARTBEAT: {
                std::string nm = r.str8();
                if (!r.good() || nm.empty()) break;
                if (add_peer(p.sender_id, from, nm, evs)) to_join.push_back(from);
                break;
            }
            case proto::LEAVE:
                if (it != peers_.end() && it->second.info.addr == from)
                    remove_peer(p.sender_id, PeerEvent::Left, evs);
                break;
            }
            for (auto& a : to_join) ep_.send_unreliable(a, proto::JOIN, join_payload());
        }
        fire(evs);
        return true;
    }

    // Wire this to ReliableEndpoint::set_unreachable_handler.
    void on_unreachable(const net::Addr& addr) {
        std::vector<Ev> evs;
        {
            std::lock_guard<std::mutex> lk(m_);
            for (auto& kv : peers_)
                if (kv.second.info.addr == addr) {
                    remove_peer(kv.first, PeerEvent::Crashed, evs);
                    break;
                }
        }
        fire(evs);
    }

    // ---- queries ----
    std::vector<PeerInfo> peers() const {
        std::lock_guard<std::mutex> lk(m_);
        std::vector<PeerInfo> v;
        for (auto& kv : peers_) v.push_back(kv.second.info);
        return v;
    }
    // Ids of all alive members INCLUDING this peer, sorted. Used for chunk placement.
    std::vector<uint32_t> member_ids() const {
        std::lock_guard<std::mutex> lk(m_);
        std::vector<uint32_t> v{ my_id_ };
        for (auto& kv : peers_) v.push_back(kv.first);
        std::sort(v.begin(), v.end());
        return v;
    }
    std::optional<PeerInfo> find(uint32_t id) const {
        std::lock_guard<std::mutex> lk(m_);
        auto it = peers_.find(id);
        if (it == peers_.end()) return std::nullopt;
        return it->second.info;
    }
    std::optional<PeerInfo> find_by_name(const std::string& n) const {
        std::lock_guard<std::mutex> lk(m_);
        for (auto& kv : peers_)
            if (kv.second.info.name == n) return kv.second.info;
        return std::nullopt;
    }
    bool joined() const { return joined_; }
    uint32_t my_id() const { return my_id_; }
    const std::string& name() const { return name_; }

private:
    using Clock = std::chrono::steady_clock;
    struct Entry {
        PeerInfo info;
        Clock::time_point last_seen;
    };
    struct Ev {
        PeerEvent type;
        PeerInfo info;
    };

    void fire(const std::vector<Ev>& evs) {
        if (!event_fn_) return;
        for (auto& e : evs) event_fn_(e.type, e.info);
    }

    std::vector<uint8_t> join_payload() const {
        proto::Writer w;
        w.str8(name_);
        return w.data();
    }

    // call with m_ held. Returns true if the peer is new.
    bool add_peer(uint32_t id, const net::Addr& addr, const std::string& name, std::vector<Ev>& evs) {
        if (id == 0 || id == my_id_ || dead_.count(id)) return false;
        auto it = peers_.find(id);
        if (it != peers_.end()) {
            it->second.last_seen = Clock::now();
            return false;
        }
        // another (older) peer id on the same address means that peer restarted: replace it
        for (auto jt = peers_.begin(); jt != peers_.end();) {
            if (jt->second.info.addr == addr) {
                evs.push_back({ PeerEvent::Crashed, jt->second.info });
                dead_.insert(jt->first);
                jt = peers_.erase(jt);
            }
            else {
                ++jt;
            }
        }
        Entry e;
        e.info.id = id;
        e.info.addr = addr;
        e.info.name = name.substr(0, 32);
        e.last_seen = Clock::now();
        peers_.emplace(id, e);
        evs.push_back({ PeerEvent::Joined, e.info });
        return true;
    }

    // call with m_ held
    void remove_peer(uint32_t id, PeerEvent why, std::vector<Ev>& evs) {
        auto it = peers_.find(id);
        if (it == peers_.end()) return;
        evs.push_back({ why, it->second.info });
        ep_.forget_peer(it->second.info.addr);
        dead_.insert(id);
        peers_.erase(it);
    }

    // call with m_ held: send our peer list (minus the requester), several packets if needed
    void send_join_reply(const net::Addr& to, uint32_t requester) {
        std::vector<const PeerInfo*> list;
        for (auto& kv : peers_)
            if (kv.first != requester) list.push_back(&kv.second.info);
        const size_t per_packet = 15;
        size_t i = 0;
        do {
            size_t n = std::min(per_packet, list.size() - i);
            proto::Writer w;
            w.str8(name_);
            w.u16(static_cast<uint16_t>(n));
            for (size_t k = 0; k < n; ++k) {
                const PeerInfo& pi = *list[i + k];
                w.u32(pi.id);
                w.u32(pi.addr.ip);
                w.u16(pi.addr.port);
                w.str8(pi.name);
            }
            ep_.send_reliable(to, proto::JOIN_REPLY, w.data());
            i += n;
        } while (i < list.size());
    }

    void run() {
        while (running_) {
            tick();
            std::unique_lock<std::mutex> lk(cv_m_);
            cv_.wait_for(lk, std::chrono::milliseconds(50), [this] { return !running_.load(); });
        }
    }

    void tick() {
        std::vector<Ev> evs;
        {
            std::lock_guard<std::mutex> lk(m_);
            auto now = Clock::now();

            // 1. joining
            if (!joined_ && now >= next_join_) {
                if (bootstrap_) {
                    ep_.send_unreliable(*bootstrap_, proto::JOIN, join_payload());  // keep trying
                    next_join_ = now + std::chrono::milliseconds(cfg_.join_retry_ms);
                }
                else if (cfg_.use_broadcast && attempts_ < cfg_.join_attempts) {
                    ep_.send_unreliable(net::Addr::broadcast(cfg_.discovery_port), proto::JOIN,
                        join_payload());
                    ++attempts_;
                    next_join_ = now + std::chrono::milliseconds(cfg_.join_retry_ms);
                }
                else {
                    joined_ = true;  // nobody answered: we are the first peer
                }
            }

            // 2. heartbeats
            if (now >= next_hb_) {
                std::vector<uint8_t> pl = join_payload();
                for (auto& kv : peers_) ep_.send_unreliable(kv.second.info.addr, proto::HEARTBEAT, pl);
                next_hb_ = now + std::chrono::milliseconds(cfg_.heartbeat_ms);
            }

            // 3. failure detector
            std::vector<uint32_t> dead_now;
            for (auto& kv : peers_)
                if (now - kv.second.last_seen > std::chrono::milliseconds(cfg_.timeout_ms))
                    dead_now.push_back(kv.first);
            for (uint32_t id : dead_now) remove_peer(id, PeerEvent::Crashed, evs);
        }
        fire(evs);
    }

    ReliableEndpoint& ep_;
    uint32_t my_id_;
    std::string name_;
    MembershipConfig cfg_;
    EventFn event_fn_;
    std::optional<net::Addr> bootstrap_;

    mutable std::mutex m_;
    std::map<uint32_t, Entry> peers_;
    std::set<uint32_t> dead_;
    std::atomic<bool> joined_{ false };
    int attempts_ = 0;
    Clock::time_point next_join_, next_hb_;

    std::atomic<bool> running_{ false };
    std::thread thread_;
    std::mutex cv_m_;
    std::condition_variable cv_;
};