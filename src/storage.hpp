// storage.hpp - Distributed file storage across the peers (no central server).
//
// FILES & CHUNKS
//   A shared file is cut into fixed-size chunks (default 64 KB). Every chunk has a SHA-256,
//   the whole file has a SHA-256, and file_id = first 8 bytes of the whole-file SHA-256.
//
// PLACEMENT  (rendezvous / highest-random-weight hashing)
//   For each chunk every peer computes score = hash(peer_id, file_id, chunk_index); the R peers
//   with the highest scores (R = 2) must store the chunk. All peers compute the same answer from
//   the same member list, with no coordination. When a peer joins or leaves, only the chunks
//   whose top-R changes move. A fix-up step guarantees that, with >= 3 peers, no single peer
//   ends up holding every chunk of a file.
//
// MESSAGES (all integers big-endian)
//   FILE_ANNOUNCE (reliable): [u64 file_id][u64 size][u32 chunk_size][u32 nchunks][u32 uploader_id]
//        [u8 nlen][name][32B file_sha256][u32 part_start][u16 part_count][part_count x 32B chunk_sha256]
//        The header part is repeated in every packet; hashes are split over several packets.
//   CHUNK_STORE / CHUNK_DATA (reliable): [u64 file_id][u32 chunk_idx][u16 frag_idx][u16 frag_count][data]
//        One chunk is sent as frag_count packets (1164 data bytes each). CHUNK_STORE = push a replica
//        to its owner, CHUNK_DATA = answer to a CHUNK_REQUEST. FLAG_LAST_FRAG marks the last fragment.
//   CHUNK_REQUEST / CHUNK_NACK (reliable): [u64 file_id][u32 chunk_idx]
//   REPLICA_UPDATE (unreliable, repeated every second): [u64 file_id][u32 version][u32 nchunks][bitmap]
//        Bit i (LSB first inside each byte) set = "I hold chunk i". This is the chunk map: it tells
//        everybody where every chunk really is. Higher version wins.
//
// A background worker thread (a) serves chunk requests, (b) verifies and stores incoming chunks,
// (c) every few hundred ms reconciles reality with the placement: it fetches chunks it should hold
// but does not (join takeover, re-replication after a crash) and drops chunks it should no longer
// hold - but only after every new owner has confirmed it holds the chunk (never below R copies).
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "codec.hpp"
#include "membership.hpp"
#include "net.hpp"
#include "protocol.hpp"
#include "reliable.hpp"
#include "sha256.hpp"

namespace storage {

    namespace fs = std::filesystem;

    constexpr size_t FRAG_HDR = 16;
    constexpr size_t FRAG_DATA = proto::MAX_PAYLOAD - FRAG_HDR;  // 1164 bytes of chunk data per packet
    constexpr uint32_t HASHES_PER_PART = 30;

    struct StorageConfig {
        uint32_t chunk_size = 64 * 1024;
        size_t replication = 2;         // R: copies of every chunk
        int reconcile_ms = 500;
        int replica_update_ms = 1000;
        int fetch_delay_ms = 2000;      // after learning a file, wait before fetching (the uploader's push usually arrives first)
        int fetch_timeout_ms = 2000;
        size_t max_parallel_fetch = 8;
    };

    // ------------------------------------------------------------------ placement
    inline uint64_t mix64(uint64_t x) {
        x += 0x9E3779B97F4A7C15ULL;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
        return x ^ (x >> 31);
    }
    inline uint64_t hrw_score(uint32_t peer, uint64_t file, uint32_t chunk) {
        return mix64(mix64(file ^ (uint64_t(chunk) * 0xD6E8FEB86659FD93ULL)) ^ mix64(peer));
    }
    // all members ordered from highest to lowest score for this chunk
    inline std::vector<uint32_t> rank_peers(uint64_t file, uint32_t chunk, const std::vector<uint32_t>& members) {
        std::vector<std::pair<uint64_t, uint32_t>> v;
        for (uint32_t m : members) v.push_back({ hrw_score(m, file, chunk), m });
        std::sort(v.begin(), v.end(), [](const auto& a, const auto& b) {
            return a.first != b.first ? a.first > b.first : a.second > b.second;
            });
        std::vector<uint32_t> r;
        for (auto& p : v) r.push_back(p.second);
        return r;
    }
    inline bool contains(const std::vector<uint32_t>& v, uint32_t x) {
        return std::find(v.begin(), v.end(), x) != v.end();
    }
    // owners[c] = the peers that must store chunk c. `members` = ids of all alive peers (incl. me).
    inline std::vector<std::vector<uint32_t>> place_file(uint64_t file, uint32_t nchunks,
        const std::vector<uint32_t>& members, size_t R) {
        std::vector<std::vector<uint32_t>> owners(nchunks), rank(nchunks);
        size_t M = members.size();
        if (M == 0) return owners;
        size_t Re = std::min(R, M);
        for (uint32_t c = 0; c < nchunks; ++c) {
            rank[c] = rank_peers(file, c, members);
            owners[c].assign(rank[c].begin(), rank[c].begin() + Re);
        }
        // Fix-up: with enough peers, nobody may hold ALL chunks of a file.
        if (M > Re && nchunks >= 2) {
            for (int iter = 0; iter < 32; ++iter) {
                std::map<uint32_t, uint32_t> cnt;
                for (auto& o : owners)
                    for (uint32_t id : o) ++cnt[id];
                bool found = false;
                uint32_t full = 0;
                for (auto& kv : cnt)
                    if (kv.second == nchunks) { full = kv.first; found = true; break; }
                if (!found) break;
                int best = -1;
                uint32_t best_cand = 0, best_cnt = ~0u;
                uint64_t best_score = ~0ULL;
                for (uint32_t c = 0; c < nchunks; ++c) {
                    if (!contains(owners[c], full)) continue;
                    uint32_t cand = rank[c][Re];  // best peer that is not an owner yet
                    uint32_t cc = cnt.count(cand) ? cnt[cand] : 0;
                    uint64_t sc = hrw_score(full, file, c);
                    if (cc < best_cnt || (cc == best_cnt && sc < best_score)) {
                        best = static_cast<int>(c); best_cand = cand; best_cnt = cc; best_score = sc;
                    }
                }
                if (best < 0) break;
                for (auto& id : owners[best])
                    if (id == full) id = best_cand;
            }
        }
        return owners;
    }

    // ------------------------------------------------------------------ data types
    struct FileMeta {
        uint64_t id = 0;
        std::string name;
        uint64_t size = 0;
        uint32_t chunk_size = 0;
        uint32_t nchunks = 0;
        uint32_t uploader = 0;
        crypto::Digest file_sha{};
        std::vector<crypto::Digest> chunk_sha;
        std::vector<bool> have_hash;
        uint32_t hashes_known = 0;
        std::string source_path;  // only set on the uploader
        std::chrono::steady_clock::time_point learned_at;

        bool complete() const { return nchunks > 0 && hashes_known == nchunks; }
        uint32_t chunk_len(uint32_t idx) const {
            return idx + 1 < nchunks ? chunk_size : static_cast<uint32_t>(size - uint64_t(nchunks - 1) * chunk_size);
        }
    };

    struct FileInfo {
        FileMeta meta;
        std::vector<std::vector<uint32_t>> holders;  // per chunk: ids of alive peers that hold it
        size_t mine = 0;                              // how many chunks I hold
    };

    struct StorageStats {
        std::atomic<uint64_t> chunks_stored{ 0 }, chunks_dropped{ 0 }, chunks_served{ 0 };
        std::atomic<uint64_t> fetches_started{ 0 }, bad_chunks{ 0 }, nacks_sent{ 0 }, repushes{ 0 };
    };

    inline std::string hex64(uint64_t v) {
        char b[24];
        std::snprintf(b, sizeof b, "%016llx", static_cast<unsigned long long>(v));
        return b;
    }

    // ------------------------------------------------------------------ Storage
    class Storage {
    public:
        // Called (from the worker thread) for every verified CHUNK_DATA; used by the downloader.
        using ChunkFn = std::function<void(uint64_t file, uint32_t idx, const std::vector<uint8_t>& data, uint32_t from_id)>;

        Storage(ReliableEndpoint& ep, Membership& ms, std::string dir, StorageConfig cfg = {})
            : ep_(ep), ms_(ms), my_id_(ms.my_id()), dir_(std::move(dir)), cfg_(cfg) {
            std::error_code ec;
            fs::remove_all(dir_, ec);  // start with an empty store; chunks are re-fetched as needed
            fs::create_directories(dir_, ec);
        }
        ~Storage() { stop(); }
        Storage(const Storage&) = delete;
        Storage& operator=(const Storage&) = delete;

        void set_chunk_listener(ChunkFn f) { listener_ = std::move(f); }
        void start() {
            if (running_.exchange(true)) return;
            worker_ = std::thread([this] { run(); });
        }
        void stop() {
            if (!running_.exchange(false)) return;
            cv_.notify_all();
            if (worker_.joinable()) worker_.join();
        }

        // Feed every delivered packet here. Returns true if it was a storage message.
        bool handle(const net::Addr& from, const proto::Packet& p) {
            switch (p.type) {
            case proto::FILE_ANNOUNCE: on_announce(p); return true;
            case proto::CHUNK_STORE: on_fragment(p, true); return true;
            case proto::CHUNK_DATA: on_fragment(p, false); return true;
            case proto::CHUNK_REQUEST: on_request(from, p); return true;
            case proto::CHUNK_NACK: on_nack(p); return true;
            case proto::REPLICA_UPDATE: on_replica_update(p); return true;
            default: return false;
            }
        }

        // Wire this to the membership event handler.
        void on_peer_event(PeerEvent e, const PeerInfo& peer) {
            if (e == PeerEvent::Joined) {  // give the newcomer the list of shared files
                std::vector<FileMeta> list;
                {
                    std::lock_guard<std::mutex> lk(m_);
                    for (auto& kv : files_)
                        if (kv.second.complete()) list.push_back(kv.second);
                }
                for (auto& m : list) send_manifest(peer.addr, m);
            }
            else {  // left or crashed: forget what it held
                std::lock_guard<std::mutex> lk(m_);
                for (auto& kv : holder_bits_) kv.second.erase(peer.id);
                for (auto& kv : pending_)
                    if (kv.second.active && kv.second.peer == peer.id) {
                        kv.second.tried.insert(peer.id);
                        kv.second.active = false;
                    }
                for (auto it = asm_.begin(); it != asm_.end();)
                    it = (std::get<0>(it->first) == peer.id) ? asm_.erase(it) : std::next(it);
            }
        }

        // Share a local file with the group. Returns "" on success, otherwise an error message.
        // Blocks while the replicas are pushed to their owners (paced by the reliable layer's queue).
        std::string share_file(const std::string& path, uint64_t* out_id = nullptr) {
            std::error_code ec;
            uint64_t size = fs::file_size(path, ec);
            if (ec) return "cannot read file: " + path;
            if (size == 0) return "file is empty";
            if (size > (uint64_t(1) << 31)) return "file too large (max 2 GB)";
            const uint32_t cs = cfg_.chunk_size;
            const uint32_t n = static_cast<uint32_t>((size + cs - 1) / cs);

            FileMeta m;
            m.size = size;
            m.chunk_size = cs;
            m.nchunks = n;
            m.uploader = my_id_;
            m.chunk_sha.resize(n);
            m.have_hash.assign(n, true);
            m.hashes_known = n;
            std::ifstream in(path, std::ios::binary);
            if (!in) return "cannot open file: " + path;
            std::vector<uint8_t> buf(cs);
            crypto::Sha256 whole;
            for (uint32_t c = 0; c < n; ++c) {
                uint32_t len = m.chunk_len(c);
                in.read(reinterpret_cast<char*>(buf.data()), len);
                if (static_cast<uint32_t>(in.gcount()) != len) return "read error";
                whole.update(buf.data(), len);
                m.chunk_sha[c] = crypto::Sha256::hash(buf.data(), len);
            }
            m.file_sha = whole.finish();
            for (int i = 0; i < 8; ++i) m.id = (m.id << 8) | m.file_sha[i];
            m.name = fs::path(path).filename().string().substr(0, 100);
            m.source_path = fs::absolute(path, ec).string();
            m.learned_at = Clock::now();
            if (out_id) *out_id = m.id;

            {
                std::lock_guard<std::mutex> lk(m_);
                if (files_.count(m.id)) return "this file is already shared";
                files_[m.id] = m;
                mine_[m.id].assign(n, false);
            }
            auto peers = ms_.peers();
            for (auto& p : peers) send_manifest(p.addr, m);

            auto members = ms_.member_ids();
            auto place = place_file(m.id, n, members, cfg_.replication);
            in.clear();
            in.seekg(0);
            for (uint32_t c = 0; c < n; ++c) {
                uint32_t len = m.chunk_len(c);
                in.read(reinterpret_cast<char*>(buf.data()), len);
                std::vector<uint8_t> data(buf.begin(), buf.begin() + len);
                for (uint32_t owner : place[c]) {
                    if (owner == my_id_) {
                        if (write_chunk(m.id, c, data)) {
                            std::lock_guard<std::mutex> lk(m_);
                            mine_[m.id][c] = true;
                        }
                    }
                    else if (auto pi = ms_.find(owner)) {
                        pace(pi->addr);
                        send_fragments(pi->addr, proto::CHUNK_STORE, m.id, c, data);
                    }
                }
            }
            dirty_ = true;
            return "";
        }

        // ---- queries ----
        std::vector<FileInfo> file_infos() const {
            auto members = ms_.member_ids();
            std::set<uint32_t> alive(members.begin(), members.end());
            std::lock_guard<std::mutex> lk(m_);
            std::vector<FileInfo> out;
            for (auto& kv : files_) {
                if (!kv.second.complete()) continue;
                FileInfo fi;
                fi.meta = kv.second;
                fi.holders.resize(kv.second.nchunks);
                auto mit = mine_.find(kv.first);
                for (uint32_t c = 0; c < kv.second.nchunks; ++c) {
                    fi.holders[c] = holders_locked(kv.first, c, alive);
                    if (mit != mine_.end() && mit->second[c]) ++fi.mine;
                }
                out.push_back(std::move(fi));
            }
            return out;
        }
        std::optional<FileMeta> meta(uint64_t id) const {
            std::lock_guard<std::mutex> lk(m_);
            auto it = files_.find(id);
            if (it == files_.end() || !it->second.complete()) return std::nullopt;
            return it->second;
        }
        std::vector<uint32_t> holders(uint64_t file, uint32_t idx) const {
            auto members = ms_.member_ids();
            std::set<uint32_t> alive(members.begin(), members.end());
            std::lock_guard<std::mutex> lk(m_);
            return holders_locked(file, idx, alive);
        }
        bool have_chunk(uint64_t file, uint32_t idx) const {
            std::lock_guard<std::mutex> lk(m_);
            auto it = mine_.find(file);
            return it != mine_.end() && idx < it->second.size() && it->second[idx];
        }
        // Reads a chunk from my own store and verifies it. nullopt if I do not hold it (or it is corrupt).
        std::optional<std::vector<uint8_t>> read_chunk(uint64_t file, uint32_t idx) const {
            crypto::Digest want;
            uint32_t len;
            {
                std::lock_guard<std::mutex> lk(m_);
                auto it = files_.find(file);
                auto mit = mine_.find(file);
                if (it == files_.end() || !it->second.complete() || idx >= it->second.nchunks) return std::nullopt;
                if (mit == mine_.end() || !mit->second[idx]) return std::nullopt;
                want = it->second.chunk_sha[idx];
                len = it->second.chunk_len(idx);
            }
            std::ifstream f(chunk_path(file, idx), std::ios::binary);
            if (!f) return std::nullopt;
            std::vector<uint8_t> d(len);
            f.read(reinterpret_cast<char*>(d.data()), len);
            if (static_cast<uint32_t>(f.gcount()) != len) return std::nullopt;
            if (crypto::Sha256::hash(d) != want) return std::nullopt;
            return d;
        }
        // Ask a specific peer for a chunk; the answer arrives via the chunk listener.
        bool request_chunk(uint32_t peer_id, uint64_t file, uint32_t idx) {
            auto pi = ms_.find(peer_id);
            if (!pi) return false;
            proto::Writer w;
            w.u64(file);
            w.u32(idx);
            ep_.send_reliable(pi->addr, proto::CHUNK_REQUEST, w.data());
            return true;
        }
        const StorageStats& stats() const { return stats_; }
        const std::string& dir() const { return dir_; }
        uint32_t my_id() const { return my_id_; }

    private:
        using Clock = std::chrono::steady_clock;
        static Clock::duration ms(int v) { return std::chrono::milliseconds(v); }

        struct Task {
            enum Type { Serve, Verify } type = Serve;
            net::Addr from;
            uint32_t from_id = 0;
            uint64_t file = 0;
            uint32_t idx = 0;
            bool is_push = false;
            std::vector<uint8_t> data;
        };
        struct Assembly {
            uint16_t count = 0, next = 0;
            std::vector<uint8_t> data;
        };
        struct Pending {
            bool active = false;
            uint32_t peer = 0;
            Clock::time_point deadline;
            std::set<uint32_t> tried;
        };
        struct HolderBits {
            uint32_t version = 0;
            std::vector<bool> bits;
        };
        using ChunkKey = std::pair<uint64_t, uint32_t>;

        // ---------------- disk ----------------
        std::string chunk_path(uint64_t file, uint32_t idx) const {
            return (fs::path(dir_) / hex64(file) / (std::to_string(idx) + ".chunk")).string();
        }
        bool write_chunk(uint64_t file, uint32_t idx, const std::vector<uint8_t>& d) {
            std::error_code ec;
            fs::create_directories(fs::path(dir_) / hex64(file), ec);
            std::ofstream f(chunk_path(file, idx), std::ios::binary | std::ios::trunc);
            if (!f) return false;
            f.write(reinterpret_cast<const char*>(d.data()), static_cast<std::streamsize>(d.size()));
            return static_cast<bool>(f);
        }

        // ---------------- sending ----------------
        void pace(const net::Addr& a) {  // do not queue unlimited data to one peer
            while (running_ && ep_.queued(a) > 300) std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        void send_fragments(const net::Addr& to, uint8_t type, uint64_t file, uint32_t idx,
            const std::vector<uint8_t>& data) {
            size_t frags = std::max<size_t>(1, (data.size() + FRAG_DATA - 1) / FRAG_DATA);
            for (size_t i = 0; i < frags; ++i) {
                size_t off = i * FRAG_DATA;
                size_t len = std::min(FRAG_DATA, data.size() - off);
                proto::Writer w;
                w.u64(file);
                w.u32(idx);
                w.u16(static_cast<uint16_t>(i));
                w.u16(static_cast<uint16_t>(frags));
                w.bytes(data.data() + off, len);
                ep_.send_reliable(to, type, w.data(), i + 1 == frags ? proto::FLAG_LAST_FRAG : 0);
            }
        }
        void send_manifest(const net::Addr& to, const FileMeta& m) {
            uint32_t parts = std::max<uint32_t>(1, (m.nchunks + HASHES_PER_PART - 1) / HASHES_PER_PART);
            for (uint32_t part = 0; part < parts; ++part) {
                uint32_t start = part * HASHES_PER_PART;
                uint32_t cnt = std::min(HASHES_PER_PART, m.nchunks - start);
                proto::Writer w;
                w.u64(m.id);
                w.u64(m.size);
                w.u32(m.chunk_size);
                w.u32(m.nchunks);
                w.u32(m.uploader);
                w.str8(m.name);
                w.bytes(m.file_sha.data(), 32);
                w.u32(start);
                w.u16(static_cast<uint16_t>(cnt));
                for (uint32_t i = 0; i < cnt; ++i) w.bytes(m.chunk_sha[start + i].data(), 32);
                ep_.send_reliable(to, proto::FILE_ANNOUNCE, w.data());
            }
        }

        // ---------------- receiving (network thread) ----------------
        void on_announce(const proto::Packet& p) {
            proto::Reader r(p.payload);
            FileMeta m;
            m.id = r.u64();
            m.size = r.u64();
            m.chunk_size = r.u32();
            m.nchunks = r.u32();
            m.uploader = r.u32();
            m.name = r.str8();
            const uint8_t* sha = r.bytes(32);
            uint32_t start = r.u32();
            uint16_t cnt = r.u16();
            const uint8_t* hs = r.bytes(size_t(cnt) * 32);
            if (!r.good() || r.remaining() != 0 || !sha || !hs) return;
            if (m.name.empty() || m.size == 0 || m.chunk_size < 1024 || m.chunk_size >(1u << 22)) return;
            uint64_t expect = (m.size + m.chunk_size - 1) / m.chunk_size;
            if (m.nchunks != expect || m.nchunks > 500000) return;
            if (uint64_t(start) + cnt > m.nchunks) return;
            std::memcpy(m.file_sha.data(), sha, 32);
            uint64_t idv = 0;
            for (int i = 0; i < 8; ++i) idv = (idv << 8) | sha[i];
            if (idv != m.id) return;  // file_id must match the whole-file hash

            std::lock_guard<std::mutex> lk(m_);
            auto it = files_.find(m.id);
            if (it == files_.end()) {
                m.chunk_sha.resize(m.nchunks);
                m.have_hash.assign(m.nchunks, false);
                m.learned_at = Clock::now();
                uint32_t n = m.nchunks;
                uint64_t id = m.id;
                it = files_.emplace(id, std::move(m)).first;
                mine_[id].assign(n, false);
            }
            FileMeta& f = it->second;
            if (f.size != m.size || f.chunk_size != m.chunk_size || f.nchunks != m.nchunks || f.file_sha != m.file_sha)
                return;  // conflicts with what we know
            for (uint32_t i = 0; i < cnt; ++i) {
                uint32_t idx = start + i;
                if (f.have_hash[idx]) continue;
                std::memcpy(f.chunk_sha[idx].data(), hs + size_t(i) * 32, 32);
                f.have_hash[idx] = true;
                ++f.hashes_known;
            }
        }

        void on_fragment(const proto::Packet& p, bool is_push) {
            proto::Reader r(p.payload);
            uint64_t file = r.u64();
            uint32_t idx = r.u32();
            uint16_t fi = r.u16(), fc = r.u16();
            size_t dlen = r.remaining();
            const uint8_t* d = r.bytes(dlen);
            if (!r.good() || dlen == 0 || !d) return;

            Task task;
            bool done = false;
            {
                std::lock_guard<std::mutex> lk(m_);
                auto it = files_.find(file);
                if (it == files_.end() || !it->second.complete() || idx >= it->second.nchunks) return;
                uint32_t elen = it->second.chunk_len(idx);
                size_t efrags = (elen + FRAG_DATA - 1) / FRAG_DATA;
                if (fc != efrags || fi >= fc) return;
                auto key = std::make_tuple(p.sender_id, file, idx);
                if (fi == 0) {
                    if (asm_.size() >= 512 && !asm_.count(key)) return;
                    Assembly a;
                    a.count = fc;
                    a.data.reserve(elen);
                    asm_[key] = std::move(a);
                }
                auto ait = asm_.find(key);
                if (ait == asm_.end() || ait->second.next != fi) {  // out of sequence: abandon
                    asm_.erase(key);
                    return;
                }
                ait->second.data.insert(ait->second.data.end(), d, d + dlen);
                ++ait->second.next;
                if (ait->second.data.size() > elen) {
                    asm_.erase(ait);
                    return;
                }
                if (fi + 1 == fc) {
                    task.type = Task::Verify;
                    task.from_id = p.sender_id;
                    task.file = file;
                    task.idx = idx;
                    task.is_push = is_push;
                    task.data = std::move(ait->second.data);
                    asm_.erase(ait);
                    done = true;
                }
            }
            if (done) push_task(std::move(task));
        }

        void on_request(const net::Addr& from, const proto::Packet& p) {
            proto::Reader r(p.payload);
            Task t;
            t.type = Task::Serve;
            t.file = r.u64();
            t.idx = r.u32();
            if (!r.good() || r.remaining() != 0) return;
            t.from = from;
            t.from_id = p.sender_id;
            push_task(std::move(t));
        }

        void on_nack(const proto::Packet& p) {
            proto::Reader r(p.payload);
            uint64_t file = r.u64();
            uint32_t idx = r.u32();
            if (!r.good()) return;
            std::lock_guard<std::mutex> lk(m_);
            auto it = pending_.find({ file, idx });
            if (it != pending_.end() && it->second.active && it->second.peer == p.sender_id) {
                it->second.tried.insert(p.sender_id);
                it->second.active = false;
            }
        }

        void on_replica_update(const proto::Packet& p) {
            if (!ms_.find(p.sender_id)) return;  // only alive members
            proto::Reader r(p.payload);
            uint64_t file = r.u64();
            uint32_t version = r.u32();
            uint32_t n = r.u32();
            size_t nbytes = (size_t(n) + 7) / 8;
            const uint8_t* bm = r.bytes(nbytes);
            if (!r.good() || r.remaining() != 0 || !bm) return;
            std::lock_guard<std::mutex> lk(m_);
            auto it = files_.find(file);
            if (it == files_.end() || it->second.nchunks != n) return;
            HolderBits& hb = holder_bits_[file][p.sender_id];
            if (version < hb.version) return;  // older than what we already have
            hb.version = version;
            hb.bits.assign(n, false);
            for (uint32_t i = 0; i < n; ++i) hb.bits[i] = (bm[i / 8] >> (i % 8)) & 1;
        }

        void push_task(Task&& t) {
            {
                std::lock_guard<std::mutex> lk(tm_);
                tasks_.push_back(std::move(t));
            }
            cv_.notify_one();
        }

        // ---------------- helpers (m_ held) ----------------
        std::vector<uint32_t> holders_locked(uint64_t file, uint32_t idx, const std::set<uint32_t>& alive) const {
            std::vector<uint32_t> v;
            auto mit = mine_.find(file);
            if (mit != mine_.end() && idx < mit->second.size() && mit->second[idx]) v.push_back(my_id_);
            auto hit = holder_bits_.find(file);
            if (hit != holder_bits_.end())
                for (auto& kv : hit->second)
                    if (kv.first != my_id_ && alive.count(kv.first) && idx < kv.second.bits.size() && kv.second.bits[idx])
                        v.push_back(kv.first);
            return v;
        }

        // ---------------- worker thread ----------------
        void run() {
            auto next_rec = Clock::now() + ms(cfg_.reconcile_ms);
            auto next_upd = Clock::now();
            while (running_) {
                std::deque<Task> batch;
                {
                    std::unique_lock<std::mutex> lk(tm_);
                    cv_.wait_for(lk, std::chrono::milliseconds(50), [&] { return !tasks_.empty() || !running_; });
                    batch.swap(tasks_);
                }
                for (auto& t : batch) {
                    if (!running_) break;
                    run_task(t);
                }
                auto now = Clock::now();
                if (now >= next_rec) {
                    reconcile();
                    next_rec = now + ms(cfg_.reconcile_ms);
                }
                bool dirty = dirty_.exchange(false);
                if (dirty || now >= next_upd) {
                    broadcast_updates(dirty);
                    next_upd = now + ms(cfg_.replica_update_ms);
                }
            }
        }

        void run_task(Task& t) {
            if (t.type == Task::Serve) {
                auto data = read_chunk(t.file, t.idx);
                if (data) {
                    send_fragments(t.from, proto::CHUNK_DATA, t.file, t.idx, *data);
                    ++stats_.chunks_served;
                }
                else {
                    proto::Writer w;
                    w.u64(t.file);
                    w.u32(t.idx);
                    ep_.send_reliable(t.from, proto::CHUNK_NACK, w.data());
                    ++stats_.nacks_sent;
                }
                return;
            }
            // Verify
            crypto::Digest want;
            uint32_t elen;
            {
                std::lock_guard<std::mutex> lk(m_);
                auto it = files_.find(t.file);
                if (it == files_.end() || !it->second.complete()) return;
                want = it->second.chunk_sha[t.idx];
                elen = it->second.chunk_len(t.idx);
            }
            if (t.data.size() != elen || crypto::Sha256::hash(t.data) != want) {
                ++stats_.bad_chunks;
                return;
            }
            bool store = false;
            {
                std::lock_guard<std::mutex> lk(m_);
                bool have = mine_[t.file][t.idx];
                store = !have && (t.is_push || pending_.count({ t.file, t.idx }));
            }
            if (store && write_chunk(t.file, t.idx, t.data)) {
                std::lock_guard<std::mutex> lk(m_);
                mine_[t.file][t.idx] = true;
                pending_.erase({ t.file, t.idx });
                ++stats_.chunks_stored;
                dirty_ = true;
            }
            if (!t.is_push && listener_) listener_(t.file, t.idx, t.data, t.from_id);
        }

        void broadcast_updates(bool bump) {
            std::vector<net::Addr> addrs;
            for (auto& p : ms_.peers()) addrs.push_back(p.addr);
            std::vector<std::vector<uint8_t>> payloads;
            {
                std::lock_guard<std::mutex> lk(m_);
                for (auto& kv : files_) {
                    if (!kv.second.complete()) continue;
                    uint32_t& ver = my_version_[kv.first];
                    if (bump) ++ver;
                    const auto& bits = mine_[kv.first];
                    uint32_t n = kv.second.nchunks;
                    std::vector<uint8_t> bm((n + 7) / 8, 0);
                    for (uint32_t i = 0; i < n; ++i)
                        if (bits[i]) bm[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
                    proto::Writer w;
                    w.u64(kv.first);
                    w.u32(ver);
                    w.u32(n);
                    w.bytes(bm);
                    payloads.push_back(w.data());
                }
            }
            for (auto& a : addrs)
                for (auto& pl : payloads) ep_.send_unreliable(a, proto::REPLICA_UPDATE, pl);
        }

        void reconcile() {
            auto members = ms_.member_ids();
            std::set<uint32_t> alive(members.begin(), members.end());
            std::map<uint32_t, net::Addr> addr_of;
            for (auto& p : ms_.peers()) addr_of[p.id] = p.addr;

            struct Req { net::Addr to; uint64_t file; uint32_t idx; };
            struct Repush { uint64_t file; uint32_t idx; std::vector<uint32_t> owners; };
            std::vector<Req> reqs;
            std::vector<ChunkKey> drops;
            std::vector<Repush> repush;
            {
                std::lock_guard<std::mutex> lk(m_);
                auto now = Clock::now();
                size_t active = 0;
                for (auto& kv : pending_) {
                    if (kv.second.active && now >= kv.second.deadline) {
                        kv.second.tried.insert(kv.second.peer);
                        kv.second.active = false;
                    }
                    if (kv.second.active) ++active;
                }
                for (auto& fk : files_) {
                    FileMeta& f = fk.second;
                    if (!f.complete()) continue;
                    auto place = place_file(f.id, f.nchunks, members, cfg_.replication);
                    bool ready = now >= f.learned_at + ms(cfg_.fetch_delay_ms);
                    bool can_repush = f.uploader == my_id_ && !f.source_path.empty() &&
                        now >= f.learned_at + ms(cfg_.fetch_delay_ms + 3000);
                    auto& mine = mine_[f.id];
                    for (uint32_t c = 0; c < f.nchunks; ++c) {
                        const auto& desired = place[c];
                        bool i_desired = contains(desired, my_id_);
                        bool have = mine[c];
                        ChunkKey key{ f.id, c };
                        if (have || !i_desired) pending_.erase(key);
                        auto holders = holders_locked(f.id, c, alive);

                        if (holders.empty()) {  // nobody has it: the uploader can restore it from the source file
                            if (can_repush && repush.size() < 4) {
                                auto rit = last_repush_.find(key);
                                if (rit == last_repush_.end() || now >= rit->second + ms(5000)) {
                                    last_repush_[key] = now;
                                    repush.push_back({ f.id, c, desired });
                                }
                            }
                            continue;
                        }
                        if (i_desired && !have) {  // I should hold it: fetch from a current holder
                            if (!ready) continue;
                            Pending& pe = pending_[key];
                            if (pe.active || active >= cfg_.max_parallel_fetch) continue;
                            std::vector<uint32_t> cands;
                            for (uint32_t h : holders)
                                if (h != my_id_ && !pe.tried.count(h) && addr_of.count(h)) cands.push_back(h);
                            if (cands.empty()) {
                                pe.tried.clear();
                                continue;
                            }
                            pe.peer = cands[(my_id_ + c) % cands.size()];
                            pe.active = true;
                            pe.deadline = now + ms(cfg_.fetch_timeout_ms);
                            ++active;
                            reqs.push_back({ addr_of[pe.peer], f.id, c });
                        }
                        else if (have && !i_desired) {  // I should not hold it: drop once every owner has it
                            bool all = true;
                            for (uint32_t d : desired)
                                if (!contains(holders, d)) { all = false; break; }
                            if (all) drops.push_back(key);
                        }
                    }
                }
                for (auto& d : drops) {
                    mine_[d.first][d.second] = false;
                    ++stats_.chunks_dropped;
                    dirty_ = true;
                }
            }
            for (auto& d : drops) {
                std::error_code ec;
                fs::remove(chunk_path(d.first, d.second), ec);
            }
            for (auto& r : reqs) {
                proto::Writer w;
                w.u64(r.file);
                w.u32(r.idx);
                ep_.send_reliable(r.to, proto::CHUNK_REQUEST, w.data());
                ++stats_.fetches_started;
            }
            for (auto& rp : repush) do_repush(rp.file, rp.idx, rp.owners, addr_of);
        }

        // The uploader re-creates a chunk that no alive peer holds, from the original file.
        void do_repush(uint64_t file, uint32_t idx, const std::vector<uint32_t>& owners,
            const std::map<uint32_t, net::Addr>& addr_of) {
            FileMeta f;
            {
                std::lock_guard<std::mutex> lk(m_);
                f = files_[file];
            }
            std::ifstream in(f.source_path, std::ios::binary);
            if (!in) return;
            in.seekg(static_cast<std::streamoff>(uint64_t(idx) * f.chunk_size));
            std::vector<uint8_t> d(f.chunk_len(idx));
            in.read(reinterpret_cast<char*>(d.data()), static_cast<std::streamsize>(d.size()));
            if (static_cast<size_t>(in.gcount()) != d.size() || crypto::Sha256::hash(d) != f.chunk_sha[idx]) return;
            ++stats_.repushes;
            for (uint32_t o : owners) {
                if (o == my_id_) {
                    if (write_chunk(file, idx, d)) {
                        std::lock_guard<std::mutex> lk(m_);
                        mine_[file][idx] = true;
                        dirty_ = true;
                    }
                }
                else {
                    auto it = addr_of.find(o);
                    if (it != addr_of.end()) send_fragments(it->second, proto::CHUNK_STORE, file, idx, d);
                }
            }
        }

        ReliableEndpoint& ep_;
        Membership& ms_;
        uint32_t my_id_;
        std::string dir_;
        StorageConfig cfg_;
        ChunkFn listener_;
        StorageStats stats_;

        mutable std::mutex m_;  // guards everything below
        std::map<uint64_t, FileMeta> files_;
        std::map<uint64_t, std::vector<bool>> mine_;                       // chunks I hold
        std::map<uint64_t, std::map<uint32_t, HolderBits>> holder_bits_;   // chunk map of the others
        std::map<uint64_t, uint32_t> my_version_;
        std::map<std::tuple<uint32_t, uint64_t, uint32_t>, Assembly> asm_;
        std::map<ChunkKey, Pending> pending_;
        std::map<ChunkKey, Clock::time_point> last_repush_;

        std::mutex tm_;  // guards tasks_
        std::deque<Task> tasks_;
        std::condition_variable cv_;
        std::atomic<bool> dirty_{ false };
        std::atomic<bool> running_{ false };
        std::thread worker_;
    };

}  // namespace storage