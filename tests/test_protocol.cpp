#include <cstdio>
#include <cstdlib>
#include <string>
#include "../src/protocol.hpp"

static int failures = 0;
#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::printf("FAIL line %d: %s\n", __LINE__, #cond);            \
            ++failures;                                                    \
        }                                                                  \
    } while (0)

static std::vector<uint8_t> bytes(const std::string& s) { return { s.begin(), s.end() }; }

int main() {
    using namespace proto;

    CHECK(HEADER_SIZE == 20);

    // known CRC32 value: crc32("123456789") = 0xCBF43926
    std::string chk = "123456789";
    CHECK(crc32(reinterpret_cast<const uint8_t*>(chk.data()), chk.size()) == 0xCBF43926u);

    // round trip
    {
        auto d = pack(CHAT, 0xDEADBEEF, 7, 3, bytes("hello"), FLAG_RELIABLE);
        auto p = unpack(d);
        CHECK(p.has_value());
        CHECK(p->type == CHAT);
        CHECK(p->sender_id == 0xDEADBEEFu);
        CHECK(p->seq == 7);
        CHECK(p->ack == 3);
        CHECK(p->flags == FLAG_RELIABLE);
        CHECK(p->payload == bytes("hello"));
    }

    // big-endian check
    {
        auto d = pack(HEARTBEAT, 0x01020304);
        CHECK(d[4] == 0x01 && d[5] == 0x02 && d[6] == 0x03 && d[7] == 0x04);
    }

    // empty payload
    {
        auto p = unpack(pack(HEARTBEAT, 1));
        CHECK(p.has_value() && p->payload.empty());
    }

    // max payload
    {
        auto d = pack(CHUNK_DATA, 1, 0, 0, std::vector<uint8_t>(MAX_PAYLOAD, 'x'));
        CHECK(d.size() == MAX_DATAGRAM);
        CHECK(unpack(d).has_value());
    }

    // payload too large
    {
        bool threw = false;
        try { pack(CHUNK_DATA, 1, 0, 0, std::vector<uint8_t>(MAX_PAYLOAD + 1, 'x')); }
        catch (const std::invalid_argument&) { threw = true; }
        CHECK(threw);
    }

    // unknown type
    {
        bool threw = false;
        try { pack(99, 1); }
        catch (const std::invalid_argument&) { threw = true; }
        CHECK(threw);
    }

    // payload corruption
    {
        auto d = pack(CHAT, 1, 0, 0, bytes("hello world"));
        d.back() ^= 0xFF;
        CHECK(!unpack(d).has_value());
    }

    // header corruption (flip a bit in seq)
    {
        auto d = pack(CHAT, 1, 0, 0, bytes("hi"));
        d[9] ^= 0x01;
        CHECK(!unpack(d).has_value());
    }

    // truncated
    {
        auto d = pack(CHAT, 1, 0, 0, bytes("hello"));
        CHECK(!unpack(d.data(), d.size() - 1).has_value());
        CHECK(!unpack(d.data(), 5).has_value());
        CHECK(!unpack(d.data(), 0).has_value());
    }

    // wrong version
    {
        auto d = pack(CHAT, 1);
        d[0] = 2;
        CHECK(!unpack(d).has_value());
    }

    if (failures == 0) std::printf("ALL TESTS PASSED\n");
    return failures == 0 ? 0 : 1;
}