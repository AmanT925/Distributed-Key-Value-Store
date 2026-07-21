// Exercises RaftNode's election and replication logic using an in-process
// fake transport (direct C++ calls between RaftNode instances instead of
// real sockets). This keeps the tests deterministic and fast in CI while
// still driving the exact same election/replication/commit code paths the
// real TCP transport (net/raft_client.cpp) calls into.
#include "kvstore/raft_node.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace kvstore;
using namespace std::chrono_literals;

namespace {

class FakeTransport : public RaftTransport {
public:
    FakeTransport(std::string id, std::unordered_map<std::string, RaftNode*>& registry,
                  std::set<std::string>& partitioned)
        : id_(std::move(id)), registry_(registry), partitioned_(partitioned) {}

    bool send_request_vote(const std::string& peer_id, const RequestVoteArgs& args,
                            RequestVoteReply& reply) override {
        if (partitioned_.count(id_) || partitioned_.count(peer_id)) return false;
        auto it = registry_.find(peer_id);
        if (it == registry_.end()) return false;
        reply = it->second->handle_request_vote(args);
        return true;
    }

    bool send_append_entries(const std::string& peer_id, const AppendEntriesArgs& args,
                              AppendEntriesReply& reply) override {
        if (partitioned_.count(id_) || partitioned_.count(peer_id)) return false;
        auto it = registry_.find(peer_id);
        if (it == registry_.end()) return false;
        reply = it->second->handle_append_entries(args);
        return true;
    }

private:
    std::string id_;
    std::unordered_map<std::string, RaftNode*>& registry_;
    std::set<std::string>& partitioned_;
};

struct TestNode {
    std::string id;
    std::string wal_path;
    std::unique_ptr<WriteAheadLog> wal;
    std::unique_ptr<ShardedMap> store;
    std::unique_ptr<FakeTransport> transport;
    std::unique_ptr<RaftNode> raft;
};

class TestCluster {
public:
    explicit TestCluster(const std::string& test_name, int node_count = 3) {
        for (int i = 0; i < node_count; ++i) {
            std::string id = "n" + std::to_string(i + 1);
            ids_.push_back(id);
        }
        for (const auto& id : ids_) {
            auto node = std::make_unique<TestNode>();
            node->id = id;
            node->wal_path = (std::filesystem::temp_directory_path() /
                               ("raft_test_" + test_name + "_" + id + ".wal"))
                                  .string();
            std::remove(node->wal_path.c_str());
            node->wal = std::make_unique<WriteAheadLog>(node->wal_path);
            node->store = std::make_unique<ShardedMap>();
            node->transport = std::make_unique<FakeTransport>(id, registry_, partitioned_);

            RaftConfig cfg;
            cfg.node_id = id;
            for (const auto& other : ids_) {
                if (other != id) cfg.peer_ids.push_back(other);
            }
            cfg.election_timeout_min_ms = 60;
            cfg.election_timeout_max_ms = 120;
            cfg.heartbeat_interval_ms = 20;

            node->raft = std::make_unique<RaftNode>(cfg, *node->transport, *node->wal, *node->store);
            registry_[id] = node->raft.get();
            nodes_.push_back(std::move(node));
        }
    }

    ~TestCluster() {
        for (auto& n : nodes_) n->raft->stop();
        for (auto& n : nodes_) std::remove(n->wal_path.c_str());
    }

    void start_all() {
        for (auto& n : nodes_) n->raft->start();
    }

    RaftNode& raft(const std::string& id) {
        for (auto& n : nodes_) {
            if (n->id == id) return *n->raft;
        }
        throw std::runtime_error("no such node: " + id);
    }

    ShardedMap& store(const std::string& id) {
        for (auto& n : nodes_) {
            if (n->id == id) return *n->store;
        }
        throw std::runtime_error("no such node: " + id);
    }

    const std::vector<std::string>& ids() const { return ids_; }

    std::string find_leader(std::chrono::milliseconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            for (const auto& id : ids_) {
                if (partitioned_.count(id)) continue;
                if (raft(id).is_leader()) return id;
            }
            std::this_thread::sleep_for(10ms);
        }
        return "";
    }

    void partition(const std::string& id) { partitioned_.insert(id); }
    void heal(const std::string& id) { partitioned_.erase(id); }

    bool wait_until(std::function<bool()> pred, std::chrono::milliseconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (pred()) return true;
            std::this_thread::sleep_for(10ms);
        }
        return pred();
    }

private:
    std::vector<std::string> ids_;
    std::vector<std::unique_ptr<TestNode>> nodes_;
    std::unordered_map<std::string, RaftNode*> registry_;
    std::set<std::string> partitioned_;
};

}  // namespace

TEST(RaftNode, ThreeNodeClusterElectsExactlyOneLeader) {
    TestCluster cluster("elect");
    cluster.start_all();

    std::string leader = cluster.find_leader(2000ms);
    ASSERT_FALSE(leader.empty()) << "no leader elected within timeout";

    int leader_count = 0;
    for (const auto& id : cluster.ids()) {
        if (cluster.raft(id).is_leader()) leader_count++;
    }
    EXPECT_EQ(leader_count, 1);
}

TEST(RaftNode, LeaderReplicatesWritesToFollowers) {
    TestCluster cluster("replicate");
    cluster.start_all();

    std::string leader = cluster.find_leader(2000ms);
    ASSERT_FALSE(leader.empty());

    std::string redirect;
    ASSERT_TRUE(cluster.raft(leader).propose(Command::kPut, "hello", "world", redirect));

    for (const auto& id : cluster.ids()) {
        bool replicated = cluster.wait_until(
            [&] {
                auto v = cluster.store(id).get("hello");
                return v.has_value() && *v == "world";
            },
            1000ms);
        EXPECT_TRUE(replicated) << "node " << id << " did not receive the committed write";
    }
}

TEST(RaftNode, NonLeaderRejectsWritesAndReportsLeaderHint) {
    TestCluster cluster("redirect");
    cluster.start_all();

    std::string leader = cluster.find_leader(2000ms);
    ASSERT_FALSE(leader.empty());

    std::string follower;
    for (const auto& id : cluster.ids()) {
        if (id != leader) { follower = id; break; }
    }

    // Give the leader's next heartbeat a chance to reach the follower so
    // its leader_hint() is populated before we check the redirect value.
    ASSERT_TRUE(cluster.wait_until(
        [&] { return cluster.raft(follower).leader_hint() == leader; }, 1000ms));

    std::string redirect;
    EXPECT_FALSE(cluster.raft(follower).propose(Command::kPut, "k", "v", redirect));
    EXPECT_EQ(redirect, leader);
}

TEST(RaftNode, ClusterReelectsAfterLeaderPartitioned) {
    TestCluster cluster("failover");
    cluster.start_all();

    std::string leader1 = cluster.find_leader(2000ms);
    ASSERT_FALSE(leader1.empty());

    cluster.partition(leader1);

    std::string leader2;
    bool reelected = cluster.wait_until(
        [&] {
            for (const auto& id : cluster.ids()) {
                if (id != leader1 && cluster.raft(id).is_leader()) {
                    leader2 = id;
                    return true;
                }
            }
            return false;
        },
        3000ms);
    ASSERT_TRUE(reelected) << "no new leader elected after partitioning the old leader";
    EXPECT_NE(leader2, leader1);

    std::string redirect;
    EXPECT_TRUE(cluster.raft(leader2).propose(Command::kPut, "after-failover", "still-works", redirect));
}
