// lossy_socket.hpp - A UdpSocket that randomly drops, duplicates and delays outgoing packets.
// Used to prove the reliable layer works and to generate benchmark graphs.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <random>
#include <thread>

#include "net.hpp"

struct Impairment {
    double loss = 0.0;        // probability a packet is dropped (0..1)
    double dup = 0.0;         // probability a packet is sent twice (0..1)
    int delay_min_ms = 0;     // each sent copy is delayed by a random time in
    int delay_max_ms = 0;     // [delay_min_ms, delay_max_ms]
    uint32_t seed = 12345;
};

class LossySocket : public net::UdpSocket {
public:
    explicit LossySocket(Impairment cfg = {}) : cfg_(cfg), rng_(cfg.seed) {
        worker_ = std::thread([this] { run(); });
    }
    ~LossySocket() override {
        {
            std::lock_guard<std::mutex> lk(m_);
            stop_ = true;
        }
        cv_.notify_all();
        worker_.join();
    }

    using net::UdpSocket::send_to;  // keep the vector overload visible

    bool send_to(const net::Addr& to, const uint8_t* data, size_t len) override {
        std::unique_lock<std::mutex> lk(m_);
        ++sent;
        if (uni_(rng_) < cfg_.loss) {
            ++dropped;
            return true;  // pretend it was sent
        }
        int copies = 1;
        if (uni_(rng_) < cfg_.dup) {
            copies = 2;
            ++duplicated;
        }
        for (int i = 0; i < copies; ++i) {
            int d = cfg_.delay_min_ms;
            if (cfg_.delay_max_ms > cfg_.delay_min_ms)
                d += static_cast<int>(uni_(rng_) * (cfg_.delay_max_ms - cfg_.delay_min_ms + 1));
            if (d <= 0) {
                lk.unlock();
                net::UdpSocket::send_to(to, data, len);
                lk.lock();
            }
            else {
                auto due = Clock::now() + std::chrono::milliseconds(d);
                queue_.emplace(due, Item{ to, std::vector<uint8_t>(data, data + len) });
                cv_.notify_all();
            }
        }
        return true;
    }

    void set_loss(double p) {
        std::lock_guard<std::mutex> lk(m_);
        cfg_.loss = p;
    }

    std::atomic<uint64_t> sent{ 0 }, dropped{ 0 }, duplicated{ 0 };

private:
    using Clock = std::chrono::steady_clock;
    struct Item {
        net::Addr to;
        std::vector<uint8_t> data;
    };

    void run() {
        std::unique_lock<std::mutex> lk(m_);
        while (!stop_) {
            if (queue_.empty()) {
                cv_.wait(lk);
                continue;
            }
            auto due = queue_.begin()->first;
            if (Clock::now() >= due) {
                Item it = std::move(queue_.begin()->second);
                queue_.erase(queue_.begin());
                lk.unlock();
                net::UdpSocket::send_to(it.to, it.data.data(), it.data.size());
                lk.lock();
            }
            else {
                cv_.wait_until(lk, due);
            }
        }
    }

    Impairment cfg_;
    std::mt19937 rng_;
    std::uniform_real_distribution<double> uni_{ 0.0, 1.0 };
    std::mutex m_;
    std::condition_variable cv_;
    std::multimap<Clock::time_point, Item> queue_;
    bool stop_ = false;
    std::thread worker_;
};