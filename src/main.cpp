#include <atomic>
#include <csignal>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

#include "kvstore/cluster_config.h"
#include "kvstore/net.h"
#include "kvstore/raft_node.h"
#include "kvstore/sharded_map.h"
#include "kvstore/wal.h"

namespace {
std::atomic<bool> g_shutdown{false};
void handle_sigint(int) { g_shutdown = true; }
}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: kv_node <node_id> <cluster_config_path> [data_dir]\n";
        return 1;
    }
    std::string node_id = argv[1];
    std::string config_path = argv[2];
    std::string data_dir = argc > 3 ? argv[3] : ".";

    auto nodes = kvstore::load_cluster_config(config_path);
    auto self_it = nodes.find(node_id);
    if (self_it == nodes.end()) {
        std::cerr << "node_id '" << node_id << "' not found in cluster config\n";
        return 1;
    }
    std::string self_address = self_it->second;

    std::vector<std::string> peer_ids;
    std::unordered_map<std::string, std::string> peer_addresses;
    for (const auto& [id, addr] : nodes) {
        if (id == node_id) continue;
        peer_ids.push_back(id);
        peer_addresses[id] = addr;
    }

    std::string wal_path = data_dir + "/" + node_id + ".wal";
    kvstore::WriteAheadLog wal(wal_path);
    kvstore::ShardedMap store;
    kvstore::TcpRaftTransport transport(peer_addresses);

    kvstore::RaftConfig raft_config;
    raft_config.node_id = node_id;
    raft_config.peer_ids = peer_ids;

    kvstore::RaftNode raft(raft_config, transport, wal, store);
    kvstore::NodeServer server(self_address, raft, store);

    std::signal(SIGINT, handle_sigint);
    std::signal(SIGTERM, handle_sigint);

    std::cout << "[" << node_id << "] starting on " << self_address
              << " with " << peer_ids.size() << " peer(s), wal=" << wal_path << std::endl;

    raft.start();
    server.start();

    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        std::cout << "[" << node_id << "] term=" << raft.current_term()
                  << " role=" << (raft.is_leader() ? "leader" : "follower/candidate")
                  << " leader_hint=" << raft.leader_hint() << std::endl;
    }

    std::cout << "[" << node_id << "] shutting down" << std::endl;
    server.stop();
    raft.stop();
    return 0;
}
