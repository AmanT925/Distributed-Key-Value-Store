#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kvstore/raft_node.h"

namespace kvstore {

// Connects to a "host:port" address, returns a connected fd or -1 on
// failure. Caller owns the fd and must close() it.
int dial(const std::string& address);

// One short-lived TCP connection per RPC -- simple and correct, and at
// 3-node/localhost scale the extra connect() cost is negligible. A
// production system would keep persistent connections; noted in README.
class TcpRaftTransport : public RaftTransport {
public:
    explicit TcpRaftTransport(std::unordered_map<std::string, std::string> peer_addresses);

    bool send_request_vote(const std::string& peer_id, const RequestVoteArgs& args,
                            RequestVoteReply& reply) override;
    bool send_append_entries(const std::string& peer_id, const AppendEntriesArgs& args,
                              AppendEntriesReply& reply) override;

private:
    std::unordered_map<std::string, std::string> peer_addresses_;
};

// Thread-per-connection TCP server. Listens on `bind_address` and serves
// both client KV requests and peer Raft RPCs on the same socket,
// dispatched by message type. Chosen over an epoll reactor for
// correctness/simplicity at this project's scale -- see README.
class NodeServer {
public:
    NodeServer(std::string bind_address, RaftNode& raft, ShardedMap& store);
    ~NodeServer();

    void start();
    void stop();

private:
    void accept_loop();
    void handle_connection(int fd);

    std::string bind_address_;
    RaftNode& raft_;
    ShardedMap& store_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
};

}  // namespace kvstore
