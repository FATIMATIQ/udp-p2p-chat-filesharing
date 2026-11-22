// protocol.hpp - Wire format for the UDP P2P chat + file sharing system.
//
// Header (20 bytes, network byte order / big-endian):
//   offset size field
//   0      1    version      (currently 1)
//   1      1    type         (see MsgType)
//   2      1    flags        (bit0 = needs ACK, bit1 = last fragment)
//   3      1    reserved     (must be 0)
//   4      4    sender_id
//   8      4    seq
//   12     4    ack
//   16     2    payload_len
//   18     2    checksum     (low 16 bits of CRC32, checksum field = 0 while computing)
#pragma once
#include <array>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <vector>

namespace proto {

    constexpr uint8_t VERSION = 1;
    constexpr size_t HEADER_SIZE = 20;
    constexpr size_t MAX_DATAGRAM = 1200;
    constexpr size_t MAX_PAYLOAD = MAX_DATAGRAM - HEADER_SIZE;  // 1180

    constexpr uint8_t FLAG_RELIABLE = 0x01;
    constexpr uint8_t FLAG_LAST_FRAG = 0x02;

    enum MsgType : uint8_t {
        JOIN = 1,
        JOIN_REPLY = 2,
        LEAVE = 3,
        HEARTBEAT = 4,
        CHAT = 5,
        ACK = 6,
        FILE_ANNOUNCE = 7,
        CHUNK_STORE = 8,
        CHUNK_REQUEST = 9,
        CHUNK_DATA = 10,
        CHUNK_NACK = 11,
        REPLICA_UPDATE = 12
    };

    inline bool valid_type(uint8_t t) { return t >= JOIN && t <= REPLICA_UPDATE; }

    struct Packet {
        uint8_t version = VERSION;
        uint8_t type = 0;
        uint8_t flags = 0;
        uint32_t sender_id = 0;
        uint32_t seq = 0;
        uint32_t ack = 0;
        std::vector<uint8_t> payload;
    };

    inline void put16(uint8_t* p, uint16_t v) {
        p[0] = static_cast<uint8_t>(v >> 8);
        p[1] = static_cast<uint8_t>(v);
    }
    inline void put32(uint8_t* p, uint32_t v) {
        p[0] = static_cast<uint8_t>(v >> 24);
        p[1] = static_cast<uint8_t>(v >> 16);
        p[2] = static_cast<uint8_t>(v >> 8);
        p[3] = static_cast<uint8_t>(v);
    }
    inline uint16_t get16(const uint8_t* p) {
        return static_cast<uint16_t>((p[0] << 8) | p[1]);
    }
    inline uint32_t get32(const uint8_t* p) {
        return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
            (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
    }

    // Standard CRC-32 (IEEE 802.3, same as zlib).
    // The lookup table is built once, in a thread-safe way (C++11 function-local static).
    inline const uint32_t* crc_table() {
        static const std::array<uint32_t, 256> table = [] {
            std::array<uint32_t, 256> t{};
            for (uint32_t i = 0; i < 256; ++i) {
                uint32_t c = i;
                for (int k = 0; k < 8; ++k) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
                t[i] = c;
            }
            return t;
            }();
        return table.data();
    }

    inline uint32_t crc32(const uint8_t* data, size_t len) {
        const uint32_t* table = crc_table();
        uint32_t crc = 0xFFFFFFFFu;
        for (size_t i = 0; i < len; ++i) crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFFu;
    }

    // Builds a datagram. Throws std::invalid_argument on bad type / oversized payload.
    inline std::vector<uint8_t> pack(uint8_t type, uint32_t sender_id, uint32_t seq = 0,
        uint32_t ack = 0, const std::vector<uint8_t>& payload = {},
        uint8_t flags = 0) {
        if (!valid_type(type)) throw std::invalid_argument("unknown message type");
        if (payload.size() > MAX_PAYLOAD) throw std::invalid_argument("payload too large");

        std::vector<uint8_t> buf(HEADER_SIZE + payload.size(), 0);
        buf[0] = VERSION;
        buf[1] = type;
        buf[2] = flags;
        buf[3] = 0;
        put32(&buf[4], sender_id);
        put32(&buf[8], seq);
        put32(&buf[12], ack);
        put16(&buf[16], static_cast<uint16_t>(payload.size()));
        put16(&buf[18], 0);
        for (size_t i = 0; i < payload.size(); ++i) buf[HEADER_SIZE + i] = payload[i];

        put16(&buf[18], static_cast<uint16_t>(crc32(buf.data(), buf.size()) & 0xFFFF));
        return buf;
    }

    // Returns std::nullopt if the datagram is invalid. Caller should silently drop it.
    inline std::optional<Packet> unpack(const uint8_t* data, size_t len) {
        if (len < HEADER_SIZE) return std::nullopt;
        if (data[0] != VERSION) return std::nullopt;
        if (!valid_type(data[1])) return std::nullopt;
        if (data[3] != 0) return std::nullopt;

        uint16_t plen = get16(data + 16);
        if (plen != len - HEADER_SIZE || plen > MAX_PAYLOAD) return std::nullopt;

        uint16_t received_sum = get16(data + 18);
        std::vector<uint8_t> tmp(data, data + len);
        put16(&tmp[18], 0);
        if (static_cast<uint16_t>(crc32(tmp.data(), tmp.size()) & 0xFFFF) != received_sum)
            return std::nullopt;

        Packet p;
        p.version = data[0];
        p.type = data[1];
        p.flags = data[2];
        p.sender_id = get32(data + 4);
        p.seq = get32(data + 8);
        p.ack = get32(data + 12);
        p.payload.assign(data + HEADER_SIZE, data + len);
        return p;
    }

    inline std::optional<Packet> unpack(const std::vector<uint8_t>& v) {
        return unpack(v.data(), v.size());
    }

}  // namespace proto