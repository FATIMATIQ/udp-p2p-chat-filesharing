// chat.hpp - Group chat and private messages.
//
// CHAT payload (big-endian):  [u32 msg_id][u8 kind][u16 text_len][text bytes]
//   msg_id : per-sender counter, starts at 1 and grows by 1 for every message the sender
//            creates (group or private). Receivers drop anything with msg_id <= last seen
//            from that sender, so a duplicate is never shown twice.
//   kind   : 0 = group message, 1 = private message
//
// Ordering: a message to the group is sent as one reliable unicast to every alive peer.
// The reliable layer delivers each peer's stream in order, so every receiver sees the
// messages of one sender in the order they were sent (FIFO per sender), exactly once,
// even when packets are lost. (There is no global order between different senders.)
#pragma once

#include <atomic>
#include <cstdio>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "codec.hpp"
#include "membership.hpp"
#include "net.hpp"
#include "protocol.hpp"
#include "reliable.hpp"

struct ChatMsg {
    uint32_t sender_id = 0;
    std::string sender_name;
    uint32_t msg_id = 0;
    bool is_private = false;
    std::string text;
};

class Chat {
public:
    static constexpr size_t MAX_TEXT = 1000;  // bytes; longer text is truncated
    // Called from the network thread. Must not block.
    using MessageFn = std::function<void(const ChatMsg&)>;

    Chat(ReliableEndpoint& ep, Membership& ms) : ep_(ep), ms_(ms) {}

    void set_message_handler(MessageFn f) { on_message_ = std::move(f); }

    // Sends to every alive peer. Returns how many peers it was queued for.
    size_t send_group(const std::string& text) {
        auto payload = encode(next_id_++, 0, text);
        auto peers = ms_.peers();
        for (auto& p : peers) ep_.send_reliable(p.addr, proto::CHAT, payload);
        sent_ += peers.size();
        return peers.size();
    }

    // Returns false if no alive peer has that name.
    bool send_private(const std::string& name, const std::string& text) {
        auto p = ms_.find_by_name(name);
        if (!p) return false;
        ep_.send_reliable(p->addr, proto::CHAT, encode(next_id_++, 1, text));
        ++sent_;
        return true;
    }

    // Feed every delivered packet here. Returns true if it was a CHAT packet.
    bool handle(const net::Addr&, const proto::Packet& pkt) {
        if (pkt.type != proto::CHAT) return false;
        proto::Reader r(pkt.payload);
        uint32_t id = r.u32();
        uint8_t kind = r.u8();
        uint16_t len = r.u16();
        const uint8_t* body = r.bytes(len);
        if (!r.good() || r.remaining() != 0 || kind > 1 || len == 0 || !body) return true;  // malformed

        ChatMsg m;
        m.sender_id = pkt.sender_id;
        m.msg_id = id;
        m.is_private = (kind == 1);
        m.text.assign(reinterpret_cast<const char*>(body), len);
        auto who = ms_.find(pkt.sender_id);
        if (who) {
            m.sender_name = who->name;
        }
        else {
            char b[24];
            std::snprintf(b, sizeof b, "peer-%08x", pkt.sender_id);
            m.sender_name = b;
        }
        {
            std::lock_guard<std::mutex> lk(m_);
            uint32_t& last = last_id_[pkt.sender_id];
            if (id <= last) {  // already seen
                ++duplicates_;
                return true;
            }
            last = id;
            history_.push_back(m);
            if (history_.size() > 200) history_.pop_front();
        }
        ++received_;
        if (on_message_) on_message_(m);
        return true;
    }

    std::vector<ChatMsg> history() const {
        std::lock_guard<std::mutex> lk(m_);
        return std::vector<ChatMsg>(history_.begin(), history_.end());
    }

    std::atomic<uint64_t> sent_{ 0 }, received_{ 0 }, duplicates_{ 0 };

private:
    static std::vector<uint8_t> encode(uint32_t id, uint8_t kind, std::string text) {
        if (text.size() > MAX_TEXT) text.resize(MAX_TEXT);
        proto::Writer w;
        w.u32(id);
        w.u8(kind);
        w.u16(static_cast<uint16_t>(text.size()));
        w.bytes(reinterpret_cast<const uint8_t*>(text.data()), text.size());
        return w.data();
    }

    ReliableEndpoint& ep_;
    Membership& ms_;
    MessageFn on_message_;
    std::atomic<uint32_t> next_id_{ 1 };
    mutable std::mutex m_;
    std::map<uint32_t, uint32_t> last_id_;  // sender -> highest msg_id delivered
    std::deque<ChatMsg> history_;
};