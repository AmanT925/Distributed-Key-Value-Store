#pragma once

#include <array>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvstore {

// Fixed-size hashmap sharded across N stripes, each guarded by its own
// shared_mutex. Reads take a shared lock on one shard; writes take an
// exclusive lock on one shard. Different keys hashing to different shards
// never contend. This is fine-grained locking, not a lock-free structure —
// see README for why a from-scratch lock-free hashmap wasn't attempted.
class ShardedMap {
public:
    explicit ShardedMap(size_t shard_count = 32);

    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool erase(const std::string& key);
    size_t size() const;

private:
    struct Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, std::string> data;
    };

    size_t shard_index(const std::string& key) const;

    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace kvstore
