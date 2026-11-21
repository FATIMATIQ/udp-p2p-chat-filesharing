// reliable.hpp - TCP-like reliability on top of UDP (Selective Repeat ARQ).
//
// What it gives you, per remote peer:
//   * every reliable message is delivered exactly once, in the order it was sent
//   * lost packets are retransmitted (timeout RTO + fast retransmit from SACK info)
//   * duplicates and corrupted packets are dropped
//   * a sliding window limits how many packets are in flight (flow control)
//   * if a peer never answers, the unreachable-handler is called
//
// Wire details (see protocol.hpp for the 20-byte header):
//   Reliable packet : flags has FLAG_RELIABLE, header.seq = per-peer sequence number (starts at 0)
//   ACK packet      : type ACK (not reliable), header.ack = cumulative ACK = next expected seq
//                     (everything < ack has been received), payload = 8-byte big-endian bitmap:
//                     bit i set  =>  seq (ack + 1 + i) was also received (selective ACK)
//   Unreliable packet: flags without FLAG_RELIABLE (e.g. HEARTBEAT) is handed to the app as is.
//
// Threading: one internal I/O thread receives packets and runs timers.
// The deliver handler is called from that thread, one packet at a time, in order.
// Do NOT block inside the handler.
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "net.hpp"
#include "protocol.hpp"

struct ReliableConfig {
    size_t window = 32;        // max unacknowledged packets per peer (1..64)
    int initial_rto_ms = 300;  // timeout before the first RTT measurement
    int min_rto_ms = 200;
    int max_rto_ms = 2000;
    int max_retries = 12;      // retransmissions of one packet before giving up on the peer
    int dup_ack_threshold = 3; // fast retransmit after this many ACKs that skip a packet
};

struct ReliableStats {
    std::atomic<uint64_t> data_sent{ 0 };         // first transmissions of reliable packets
    std::atomic<uint64_t> retransmits{ 0 };       // timeout-based retransmissions
    std::atomic<uint64_t> fast_retransmits{ 0 };  // SACK-triggered retransmissions
    std::atomic<uint64_t> acks_sent{ 0 };
    std::atomic<uint64_t> acks_received{ 0 };
    std::atomic<uint64_t> delivered{ 0 };         // reliable packets handed to the app
    std::atomic<uint64_t> duplicates_dropped{ 0 };
    std::atomic<uint64_t> out_of_window{ 0 };
    std::atomic<uint64_t> bad_packets{ 0 };       // failed checksum / malformed
};

class ReliableEndpoint {
public:
    using DeliverFn = std::function<void(const net::Addr& from, const proto::Packet& pkt)>;
    using UnreachableFn = std::function<void(const net::Addr& peer)>;

    // my_id must be non-zero and should be random per run (it identifies this "session").
    ReliableEndpoint(net::UdpSocket& sock, uint32_t my_id, ReliableConfig cfg = {})
        : sock_(sock), my_id_(my_id), cfg_(cfg) {
        if (my_id_ == 0) throw std::invalid_argument("peer id must be non-zero");
        cfg_.window = std::max<size_t>(1, std::min<size_t>(cfg_.window, 64));
    }
    ~ReliableEndpoint() { stop(); }
    ReliableEndpoint(const ReliableEndpoint&) = delete;
    ReliableEndpoint& operator=(const ReliableEndpoint&) = delete;

    // Set handlers BEFORE start().
    void set_deliver_handler(DeliverFn f) { deliver_fn_ = std::move(f); }
    void set_unreachable_handler(UnreachableFn f) { unreachable_fn_ = std::move(f); }

    void start() {
        if (running_.exchange(true)) return;
        io_thread_ = std::thread([this] { io_loop(); });
    }
    void stop() {
        if (!running_.exchange(false)) return;
        if (io_thread_.joinable()) io_thread_.join();
    }

    // Queue a message for reliable, in-order delivery. Never blocks.
    void send_reliable(const net::Addr& to, uint8_t type, const std::vector<uint8_t>& payload,
        uint8_t flags = 0) {
        if (payload.size() > proto::MAX_PAYLOAD) throw std::invalid_argument("payload too large");
        if (!proto::valid_type(type) || type == proto::ACK) throw std::invalid_argument("bad type");
        std::lock_guard<std::mutex> lk(m_);
        Channel& ch = channel(to);
        ch.pending.push_back(Pending{ type, flags, payload });
        pump(ch);
    }

    // Fire-and-forget (HEARTBEAT, JOIN broadcast, ...). May be lost.
    void send_unreliable(const net::Addr& to, uint8_t type, const std::vector<uint8_t>& payload) {
        sock_.send_to(to, proto::pack(type, my_id_, 0, 0, payload, 0));
    }

    // Drop all state for a peer (call when membership says it is gone).
    void forget_peer(const net::Addr& peer) {
        std::lock_guard<std::mutex> lk(m_);
        channels_.erase(peer);
    }

    // Packets queued or in flight to this peer (use for back-pressure).
    size_t queued(const net::Addr& peer) {
        std::lock_guard<std::mutex> lk(m_);
        auto it = channels_.find(peer);
        return it == channels_.end() ? 0 : it->second.pending.size() + it->second.inflight.size();
    }

    // Wait until everything sent to peer was acknowledged. Returns false on timeout.
    bool wait_idle(const net::Addr& peer, int timeout_ms) {
        auto end = Clock::now() + std::chrono::milliseconds(timeout_ms);
        while (Clock::now() < end) {
            if (queued(peer) == 0) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return queued(peer) == 0;
    }

    uint32_t my_id() const { return my_id_; }
    const ReliableStats& stats() const { return stats_; }

private:
    using Clock = std::chrono::steady_clock;

    struct Pending {
        uint8_t type;
        uint8_t flags;
        std::vector<uint8_t> payload;
    };
    struct InFlight {
        uint32_t seq = 0;
        std::vector<uint8_t> datagram;
        Clock::time_point sent_at, deadline;
        int retries = 0;
        bool sacked = false;        // receiver has it (selective ACK) but cumulative ACK not there yet
        bool retransmitted = false; // Karn's rule: no RTT sample from retransmitted packets
        int dup_hits = 0;
        bool fast_done = false;     // at most one fast retransmit per transmission; RTO handles the rest
    };
    struct Channel {
        net::Addr addr;
        uint32_t remote_id = 0;  // identifies the remote session; a change means it restarted
        // ---- sender side ----
        uint32_t next_seq = 0;
        std::deque<Pending> pending;
        std::map<uint32_t, InFlight> inflight;
        bool have_rtt = false;
        double srtt = 0, rttvar = 0, rto_ms = 300;
        // ---- receiver side ----
        uint32_t expected = 0;
        std::map<uint32_t, proto::Packet> rbuf;
    };

    // ---------------- helpers (call with m_ held) ----------------
    Channel& channel(const net::Addr& a) {
        auto it = channels_.find(a);
        if (it == channels_.end()) {
            Channel c;
            c.addr = a;
            c.rto_ms = cfg_.initial_rto_ms;
            it = channels_.emplace(a, std::move(c)).first;
        }
        return it->second;
    }

    void reset_channel(Channel& ch) {
        ch.next_seq = 0;
        ch.pending.clear();
        ch.inflight.clear();
        ch.have_rtt = false;
        ch.srtt = ch.rttvar = 0;
        ch.rto_ms = cfg_.initial_rto_ms;
        ch.expected = 0;
        ch.rbuf.clear();
    }

    void check_epoch(Channel& ch, uint32_t remote_id) {
        if (ch.remote_id == 0) {
            ch.remote_id = remote_id;
        }
        else if (ch.remote_id != remote_id) {  // peer restarted with a new id: fresh start
            reset_channel(ch);
            ch.remote_id = remote_id;
        }
    }

    Clock::duration ms(double v) const {
        return std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double, std::milli>(v));
    }

    double backoff_rto(const Channel& ch, int retries) const {
        double r = ch.rto_ms * static_cast<double>(1u << std::min(retries, 10));
        return std::min<double>(r, cfg_.max_rto_ms);
    }

    void update_rtt(Channel& ch, double r) {  // RFC 6298
        if (!ch.have_rtt) {
            ch.srtt = r;
            ch.rttvar = r / 2;
            ch.have_rtt = true;
        }
        else {
            ch.rttvar = 0.75 * ch.rttvar + 0.25 * std::abs(ch.srtt - r);
            ch.srtt = 0.875 * ch.srtt + 0.125 * r;
        }
        double rto = ch.srtt + std::max(1.0, 4 * ch.rttvar);
        ch.rto_ms = std::max<double>(cfg_.min_rto_ms, std::min<double>(rto, cfg_.max_rto_ms));
    }

    // Move queued messages into the window and transmit them.
    void pump(Channel& ch) {
        while (!ch.pending.empty()) {
            uint32_t base = ch.inflight.empty() ? ch.next_seq : ch.inflight.begin()->first;
            if (ch.next_seq - base >= cfg_.window) break;
            Pending p = std::move(ch.pending.front());
            ch.pending.pop_front();
            InFlight e;
            e.seq = ch.next_seq++;
            e.datagram = proto::pack(p.type, my_id_, e.seq, 0, p.payload,
                static_cast<uint8_t>(p.flags | proto::FLAG_RELIABLE));
            auto now = Clock::now();
            e.sent_at = now;
            e.deadline = now + ms(ch.rto_ms);
            sock_.send_to(ch.addr, e.datagram);
            ++stats_.data_sent;
            uint32_t key = e.seq;
            ch.inflight.emplace(key, std::move(e));
        }
    }

    void send_ack(Channel& ch) {
        uint64_t bitmap = 0;
        for (int i = 0; i < 64; ++i)
            if (ch.rbuf.count(ch.expected + 1 + static_cast<uint32_t>(i))) bitmap |= (1ull << i);
        std::vector<uint8_t> pl(8);
        proto::put32(&pl[0], static_cast<uint32_t>(bitmap >> 32));
        proto::put32(&pl[4], static_cast<uint32_t>(bitmap & 0xFFFFFFFFu));
        sock_.send_to(ch.addr, proto::pack(proto::ACK, my_id_, 0, ch.expected, pl, 0));
        ++stats_.acks_sent;
    }

    void on_ack(Channel& ch, const proto::Packet& pkt) {
        ++stats_.acks_received;
        uint32_t cum = pkt.ack;
        if (cum > ch.next_seq) return;  // acknowledges data we never sent: stale or foreign
        uint64_t bitmap = 0;
        if (pkt.payload.size() >= 8)
            bitmap = (static_cast<uint64_t>(proto::get32(&pkt.payload[0])) << 32) |
            proto::get32(&pkt.payload[4]);
        auto now = Clock::now();
        bool have_sample = false;
        Clock::time_point latest_sent;
        auto consider = [&](const InFlight& e) {
            if (e.retransmitted) return;
            if (!have_sample || e.sent_at > latest_sent) latest_sent = e.sent_at;
            have_sample = true;
            };

        // cumulative part
        while (!ch.inflight.empty() && ch.inflight.begin()->first < cum) {
            if (!ch.inflight.begin()->second.sacked) consider(ch.inflight.begin()->second);
            ch.inflight.erase(ch.inflight.begin());
        }
        // selective part
        uint32_t max_sacked = 0;
        bool any_sacked = false;
        for (int i = 0; i < 64; ++i) {
            if (!(bitmap & (1ull << i))) continue;
            auto it = ch.inflight.find(cum + 1 + static_cast<uint32_t>(i));
            if (it == ch.inflight.end()) continue;
            if (!it->second.sacked) {
                it->second.sacked = true;
                consider(it->second);
            }
            any_sacked = true;
            max_sacked = std::max(max_sacked, it->first);
        }
        if (have_sample)
            update_rtt(ch, std::chrono::duration<double, std::milli>(now - latest_sent).count());

        // fast retransmit: packets below a SACKed one that keep being skipped
        if (any_sacked) {
            for (auto& kv : ch.inflight) {
                InFlight& e = kv.second;
                if (e.sacked || e.fast_done || e.seq >= max_sacked) continue;
                if (++e.dup_hits >= cfg_.dup_ack_threshold) {
                    e.dup_hits = 0;
                    e.fast_done = true;
                    e.retransmitted = true;
                    e.sent_at = now;
                    e.deadline = now + ms(backoff_rto(ch, e.retries));
                    sock_.send_to(ch.addr, e.datagram);
                    ++stats_.fast_retransmits;
                }
            }
        }
        pump(ch);
    }

    void on_reliable(Channel& ch, const proto::Packet& pkt, std::vector<proto::Packet>& out) {
        uint32_t seq = pkt.seq;
        if (seq < ch.expected) {  // already delivered: duplicate, but re-ACK so the sender stops
            ++stats_.duplicates_dropped;
            send_ack(ch);
            return;
        }
        if (seq >= ch.expected + 64) {  // beyond our receive window
            ++stats_.out_of_window;
            return;
        }
        if (ch.rbuf.count(seq)) {
            ++stats_.duplicates_dropped;
        }
        else {
            ch.rbuf.emplace(seq, pkt);
        }
        for (auto it = ch.rbuf.find(ch.expected); it != ch.rbuf.end(); it = ch.rbuf.find(ch.expected)) {
            out.push_back(std::move(it->second));
            ch.rbuf.erase(it);
            ++ch.expected;
            ++stats_.delivered;
        }
        send_ack(ch);
    }

    // ---------------- I/O thread ----------------
    void handle_datagram(const net::Addr& from, const uint8_t* data, size_t n) {
        auto pkt = proto::unpack(data, n);
        if (!pkt) {
            ++stats_.bad_packets;
            return;
        }
        if (pkt->sender_id == my_id_) return;  // our own broadcast echoed back

        std::vector<proto::Packet> to_deliver;
        {
            std::lock_guard<std::mutex> lk(m_);
            Channel& ch = channel(from);
            check_epoch(ch, pkt->sender_id);
            if (pkt->type == proto::ACK)
                on_ack(ch, *pkt);
            else if (pkt->flags & proto::FLAG_RELIABLE)
                on_reliable(ch, *pkt, to_deliver);
            else
                to_deliver.push_back(*pkt);
        }
        if (deliver_fn_)
            for (auto& p : to_deliver) deliver_fn_(from, p);
    }

    void check_timers() {
        auto now = Clock::now();
        std::vector<net::Addr> dead;
        {
            std::lock_guard<std::mutex> lk(m_);
            for (auto& cv : channels_) {
                Channel& ch = cv.second;
                bool give_up = false;
                for (auto& kv : ch.inflight) {
                    InFlight& e = kv.second;
                    if (e.sacked || now < e.deadline) continue;
                    if (e.retries >= cfg_.max_retries) {
                        give_up = true;
                        break;
                    }
                    ++e.retries;
                    e.retransmitted = true;
                    e.sent_at = now;
                    e.deadline = now + ms(backoff_rto(ch, e.retries));
                    sock_.send_to(ch.addr, e.datagram);
                    ++stats_.retransmits;
                }
                if (give_up) dead.push_back(cv.first);
            }
            for (auto& a : dead) channels_.erase(a);
        }
        if (unreachable_fn_)
            for (auto& a : dead) unreachable_fn_(a);
    }

    void io_loop() {
        std::vector<uint8_t> buf(proto::MAX_DATAGRAM + 64);
        auto last_timer = Clock::now();
        while (running_) {
            net::Addr from;
            int n = sock_.recv_from(buf.data(), buf.size(), from, 5);
            if (n > 0) handle_datagram(from, buf.data(), static_cast<size_t>(n));
            auto now = Clock::now();
            if (now - last_timer >= std::chrono::milliseconds(5)) {
                check_timers();
                last_timer = now;
            }
        }
    }

    net::UdpSocket& sock_;
    uint32_t my_id_;
    ReliableConfig cfg_;
    ReliableStats stats_;
    DeliverFn deliver_fn_;
    UnreachableFn unreachable_fn_;
    std::mutex m_;
    std::map<net::Addr, Channel> channels_;
    std::atomic<bool> running_{ false };
    std::thread io_thread_;
};