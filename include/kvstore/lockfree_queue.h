#pragma once

#include <atomic>
#include <cstddef>
#include <optional>
#include <vector>

namespace kvstore {

// Single-producer/single-consumer lock-free ring buffer. One thread (the
// Raft/apply thread) pushes, one thread (the WAL writer) pops. Capacity is
// rounded up to the next power of two so the index wrap can use a mask
// instead of a modulo. This is the one genuinely lock-free structure in the
// project — see README for how it differs from the ShardedMap's locking.
template <typename T>
class SpscQueue {
public:
    explicit SpscQueue(size_t capacity) : mask_(next_pow2(capacity) - 1), buffer_(mask_ + 1) {}

    bool push(T item) {
        size_t head = head_.load(std::memory_order_relaxed);
        size_t next = (head + 1) & mask_;
        if (next == tail_.load(std::memory_order_acquire)) {
            return false;  // full
        }
        buffer_[head] = std::move(item);
        head_.store(next, std::memory_order_release);
        return true;
    }

    std::optional<T> pop() {
        size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire)) {
            return std::nullopt;  // empty
        }
        T item = std::move(buffer_[tail]);
        tail_.store((tail + 1) & mask_, std::memory_order_release);
        return item;
    }

private:
    static size_t next_pow2(size_t n) {
        size_t p = 1;
        while (p < n) p <<= 1;
        return p;
    }

    size_t mask_;
    std::vector<T> buffer_;
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
};

}  // namespace kvstore
