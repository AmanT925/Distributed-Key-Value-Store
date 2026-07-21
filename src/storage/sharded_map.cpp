#include "kvstore/sharded_map.h"

namespace kvstore {

ShardedMap::ShardedMap(size_t shard_count) {
    shards_.reserve(shard_count);
    for (size_t i = 0; i < shard_count; ++i) {
        shards_.push_back(std::make_unique<Shard>());
    }
}

size_t ShardedMap::shard_index(const std::string& key) const {
    return std::hash<std::string>{}(key) % shards_.size();
}

void ShardedMap::put(const std::string& key, const std::string& value) {
    Shard& shard = *shards_[shard_index(key)];
    std::unique_lock lock(shard.mutex);
    shard.data[key] = value;
}

std::optional<std::string> ShardedMap::get(const std::string& key) const {
    const Shard& shard = *shards_[shard_index(key)];
    std::shared_lock lock(shard.mutex);
    auto it = shard.data.find(key);
    if (it == shard.data.end()) return std::nullopt;
    return it->second;
}

bool ShardedMap::erase(const std::string& key) {
    Shard& shard = *shards_[shard_index(key)];
    std::unique_lock lock(shard.mutex);
    return shard.data.erase(key) > 0;
}

size_t ShardedMap::size() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
        std::shared_lock lock(shard->mutex);
        total += shard->data.size();
    }
    return total;
}

}  // namespace kvstore
