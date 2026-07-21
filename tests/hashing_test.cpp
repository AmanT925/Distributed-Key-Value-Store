#include "kvstore/consistent_hash.h"

#include <gtest/gtest.h>

#include <map>

using namespace kvstore;

TEST(ConsistentHashRing, EmptyRingReturnsEmptyNode) {
    ConsistentHashRing ring;
    EXPECT_EQ(ring.get_node("anything"), "");
}

TEST(ConsistentHashRing, SingleNodeOwnsEveryKey) {
    ConsistentHashRing ring;
    ring.add_node("node1");
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(ring.get_node("key-" + std::to_string(i)), "node1");
    }
}

TEST(ConsistentHashRing, KeysDistributeAcrossMultipleNodes) {
    ConsistentHashRing ring;
    ring.add_node("node1");
    ring.add_node("node2");
    ring.add_node("node3");

    std::map<std::string, int> counts;
    constexpr int kKeys = 10000;
    for (int i = 0; i < kKeys; ++i) {
        counts[ring.get_node("key-" + std::to_string(i))]++;
    }

    ASSERT_EQ(counts.size(), 3u);
    // With 100 virtual nodes per physical node, distribution should be
    // reasonably even -- allow generous slack rather than asserting exact
    // uniformity (which virtual-node hashing doesn't guarantee).
    for (const auto& [node, count] : counts) {
        double fraction = static_cast<double>(count) / kKeys;
        EXPECT_GT(fraction, 0.15) << node;
        EXPECT_LT(fraction, 0.55) << node;
    }
}

TEST(ConsistentHashRing, RemovingNodeOnlyRemapsItsOwnKeys) {
    ConsistentHashRing ring;
    ring.add_node("node1");
    ring.add_node("node2");
    ring.add_node("node3");

    constexpr int kKeys = 5000;
    std::map<std::string, std::string> before;
    for (int i = 0; i < kKeys; ++i) {
        std::string key = "key-" + std::to_string(i);
        before[key] = ring.get_node(key);
    }

    ring.remove_node("node2");

    int remapped = 0;
    int remapped_to_node2 = 0;
    for (int i = 0; i < kKeys; ++i) {
        std::string key = "key-" + std::to_string(i);
        std::string now = ring.get_node(key);
        EXPECT_NE(now, "node2");
        if (now != before[key]) {
            remapped++;
            if (before[key] == "node2") remapped_to_node2++;
        }
    }

    // Every key that moved must have been owned by node2 before removal --
    // consistent hashing's core guarantee (minimal disruption).
    EXPECT_EQ(remapped, remapped_to_node2);
    EXPECT_GT(remapped, 0);
}

TEST(ConsistentHashRing, GetNodesReturnsDistinctReplicaSet) {
    ConsistentHashRing ring;
    ring.add_node("node1");
    ring.add_node("node2");
    ring.add_node("node3");

    auto nodes = ring.get_nodes("some-key", 3);
    ASSERT_EQ(nodes.size(), 3u);
    EXPECT_NE(nodes[0], nodes[1]);
    EXPECT_NE(nodes[1], nodes[2]);
    EXPECT_NE(nodes[0], nodes[2]);
}
