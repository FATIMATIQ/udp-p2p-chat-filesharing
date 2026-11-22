// codec.hpp - Big-endian Writer / Reader for message payloads.
// Reader never reads out of bounds: on overrun it sets good()==false and returns zeros,
// so malformed packets from the network can never crash a peer.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace proto {

    class Writer {
    public:
        void u8(uint8_t v) { b_.push_back(v); }
        void u16(uint16_t v) { b_.push_back(uint8_t(v >> 8)); b_.push_back(uint8_t(v)); }
        void u32(uint32_t v) {
            for (int s = 24; s >= 0; s -= 8) b_.push_back(uint8_t(v >> s));
        }
        void u64(uint64_t v) {
            for (int s = 56; s >= 0; s -= 8) b_.push_back(uint8_t(v >> s));
        }
        // length-prefixed (1 byte) string, truncated to 255 bytes
        void str8(const std::string& s) {
            size_t n = s.size() > 255 ? 255 : s.size();
            u8(static_cast<uint8_t>(n));
            b_.insert(b_.end(), s.begin(), s.begin() + n);
        }
        void bytes(const uint8_t* p, size_t n) { b_.insert(b_.end(), p, p + n); }
        void bytes(const std::vector<uint8_t>& v) { bytes(v.data(), v.size()); }

        size_t size() const { return b_.size(); }
        const std::vector<uint8_t>& data() const { return b_; }

    private:
        std::vector<uint8_t> b_;
    };

    class Reader {
    public:
        Reader(const uint8_t* p, size_t n) : p_(p), n_(n) {}
        explicit Reader(const std::vector<uint8_t>& v) : p_(v.data()), n_(v.size()) {}

        uint8_t u8() { return need(1) ? p_[pos_++] : 0; }
        uint16_t u16() {
            if (!need(2)) return 0;
            uint16_t v = uint16_t((p_[pos_] << 8) | p_[pos_ + 1]);
            pos_ += 2;
            return v;
        }
        uint32_t u32() {
            if (!need(4)) return 0;
            uint32_t v = 0;
            for (int i = 0; i < 4; ++i) v = (v << 8) | p_[pos_ + i];
            pos_ += 4;
            return v;
        }
        uint64_t u64() {
            if (!need(8)) return 0;
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i) v = (v << 8) | p_[pos_ + i];
            pos_ += 8;
            return v;
        }
        std::string str8() {
            size_t n = u8();
            if (!need(n)) return {};
            std::string s(reinterpret_cast<const char*>(p_ + pos_), n);
            pos_ += n;
            return s;
        }
        // returns a pointer to the next n bytes (or nullptr on overrun) and skips them
        const uint8_t* bytes(size_t n) {
            if (!need(n)) return nullptr;
            const uint8_t* r = p_ + pos_;
            pos_ += n;
            return r;
        }

        bool good() const { return ok_; }
        size_t remaining() const { return n_ - pos_; }

    private:
        bool need(size_t k) {
            if (!ok_ || n_ - pos_ < k) {
                ok_ = false;
                return false;
            }
            return true;
        }
        const uint8_t* p_;
        size_t n_;
        size_t pos_ = 0;
        bool ok_ = true;
    };

}  // namespace proto