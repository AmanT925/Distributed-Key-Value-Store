#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

#include "kvstore/net.h"
#include "kvstore/protocol.h"
#include "kvstore/rpc_messages.h"

using namespace kvstore;
using namespace std::chrono;

namespace {

struct Options {
    std::string address;
    int threads = 8;
    int duration_seconds = 5;
    int keyspace = 1000;
};

bool get_once(int fd, const std::string& key) {
    if (!send_frame(fd, MsgType::kGet, GetRequest{key}.serialize())) return false;
    MsgType type;
    std::string payload;
    if (!recv_frame(fd, type, payload)) return false;
    return true;
}

void worker(const Options& opts, int thread_id, std::atomic<bool>& stop,
            std::vector<double>& latencies_us, std::mutex& latencies_mutex,
            std::atomic<uint64_t>& total_ops) {
    int fd = dial(opts.address);
    if (fd < 0) {
        std::cerr << "worker " << thread_id << ": failed to connect\n";
        return;
    }
    std::mt19937 rng(thread_id * 7919u + 17);
    std::uniform_int_distribution<int> key_dist(0, opts.keyspace - 1);

    std::vector<double> local_latencies;
    local_latencies.reserve(200000);

    while (!stop.load(std::memory_order_relaxed)) {
        std::string key = "bench-key-" + std::to_string(key_dist(rng));
        auto start = steady_clock::now();
        if (!get_once(fd, key)) break;
        auto end = steady_clock::now();
        local_latencies.push_back(duration_cast<duration<double, std::micro>>(end - start).count());
    }

    total_ops.fetch_add(local_latencies.size(), std::memory_order_relaxed);
    {
        std::lock_guard lock(latencies_mutex);
        latencies_us.insert(latencies_us.end(), local_latencies.begin(), local_latencies.end());
    }
    ::close(fd);
}

double percentile(std::vector<double>& sorted, double p) {
    if (sorted.empty()) return 0.0;
    size_t idx = static_cast<size_t>(p * (sorted.size() - 1));
    return sorted[idx];
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: bench_client <host:port> [threads] [duration_seconds] [keyspace]\n";
        return 1;
    }
    Options opts;
    opts.address = argv[1];
    if (argc > 2) opts.threads = std::atoi(argv[2]);
    if (argc > 3) opts.duration_seconds = std::atoi(argv[3]);
    if (argc > 4) opts.keyspace = std::atoi(argv[4]);

    std::cout << "warming up: writing " << opts.keyspace << " keys to " << opts.address << "...\n";
    {
        int fd = dial(opts.address);
        if (fd < 0) {
            std::cerr << "failed to connect for warmup\n";
            return 1;
        }
        for (int i = 0; i < opts.keyspace; ++i) {
            std::string key = "bench-key-" + std::to_string(i);
            std::string value = "value-" + std::to_string(i);
            send_frame(fd, MsgType::kPut, PutRequest{key, value}.serialize());
            MsgType type;
            std::string payload;
            recv_frame(fd, type, payload);
        }
        ::close(fd);
    }

    std::cout << "running " << opts.threads << " threads for " << opts.duration_seconds
              << "s against " << opts.address << " (keyspace=" << opts.keyspace << ")...\n";

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> total_ops{0};
    std::vector<double> latencies_us;
    std::mutex latencies_mutex;

    std::vector<std::thread> workers;
    for (int i = 0; i < opts.threads; ++i) {
        workers.emplace_back(worker, std::cref(opts), i, std::ref(stop), std::ref(latencies_us),
                              std::ref(latencies_mutex), std::ref(total_ops));
    }

    auto start = steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(opts.duration_seconds));
    stop = true;
    for (auto& t : workers) t.join();
    auto end = steady_clock::now();

    double elapsed_s = duration_cast<duration<double>>(end - start).count();
    uint64_t ops = total_ops.load();
    double throughput = ops / elapsed_s;

    std::sort(latencies_us.begin(), latencies_us.end());
    double p50 = percentile(latencies_us, 0.50);
    double p99 = percentile(latencies_us, 0.99);
    double p999 = percentile(latencies_us, 0.999);

    std::cout << "\n--- results ---\n";
    std::cout << "total ops:      " << ops << "\n";
    std::cout << "elapsed:        " << elapsed_s << "s\n";
    std::cout << "throughput:     " << throughput << " ops/sec\n";
    std::cout << "p50 latency:    " << p50 << " us\n";
    std::cout << "p99 latency:    " << p99 << " us\n";
    std::cout << "p99.9 latency:  " << p999 << " us\n";
    return 0;
}
