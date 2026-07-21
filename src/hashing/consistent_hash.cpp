#include "kvstore/consistent_hash.h"

#include <functional>

namespace kvstore {

ConsistentHashRing::ConsistentHashRing(int virtual_nodes_per_node)
    : virtual_nodes_per_node_(virtual_nodes_per_node) {}

uint64_t ConsistentHashRing::hash(const std::string& s) {
    return std::hash<std::string>{}(s);
}

void ConsistentHashRing::add_node(const std::string& node_id) {
    if (nodes_.count(node_id)) return;
    nodes_.insert(node_id);
    for (int i = 0; i < virtual_nodes_per_node_; ++i) {
        std::string vnode_key = node_id + "#" + std::to_string(i);
        ring_[hash(vnode_key)] = node_id;
    }
}

void ConsistentHashRing::remove_node(const std::string& node_id) {
    if (!nodes_.count(node_id)) return;
    nodes_.erase(node_id);
    for (int i = 0; i < virtual_nodes_per_node_; ++i) {
        std::string vnode_key = node_id + "#" + std::to_string(i);
        ring_.erase(hash(vnode_key));
    }
}

std::string ConsistentHashRing::get_node(const std::string& key) const {
    if (ring_.empty()) return "";
    uint64_t h = hash(key);
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) it = ring_.begin();
    return it->second;
}

std::vector<std::string> ConsistentHashRing::get_nodes(const std::string& key, size_t count) const {
    std::vector<std::string> result;
    if (ring_.empty()) return result;

    uint64_t h = hash(key);
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) it = ring_.begin();

    std::set<std::string> seen;
    auto start = it;
    do {
        if (seen.insert(it->second).second) {
            result.push_back(it->second);
            if (result.size() == count) break;
        }
        ++it;
        if (it == ring_.end()) it = ring_.begin();
    } while (it != start);

    return result;
}

}  // namespace kvstore
