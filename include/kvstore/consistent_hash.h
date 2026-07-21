#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace kvstore {

// Standard consistent-hashing ring with virtual nodes. Maps a key to the
// physical node responsible for it. Node add/remove only reassigns the
// keys that fall in the affected arc of the ring (not a full rehash).
//
// NOTE (see README "known limitations"): this ring is a standalone, unit
// tested module for future multi-shard partitioning. The 3-node demo
// cluster wired up in main.cpp runs as a single Raft group replicating the
// full keyspace to all 3 nodes, so this ring does not currently drive live
// data placement in that demo — it answers "which node would own this key"
// but nothing today migrates data in response to it.
class ConsistentHashRing {
public:
    explicit ConsistentHashRing(int virtual_nodes_per_node = 100);

    void add_node(const std::string& node_id);
    void remove_node(const std::string& node_id);

    // Returns the node responsible for `key`, or empty string if the ring
    // has no nodes.
    std::string get_node(const std::string& key) const;

    // Returns up to `count` distinct physical nodes walking clockwise from
    // key's position -- used for picking a replica set.
    std::vector<std::string> get_nodes(const std::string& key, size_t count) const;

    size_t node_count() const { return nodes_.size(); }

private:
    static uint64_t hash(const std::string& s);

    int virtual_nodes_per_node_;
    std::map<uint64_t, std::string> ring_;  // hash -> node_id
    std::set<std::string> nodes_;
};

}  // namespace kvstore
