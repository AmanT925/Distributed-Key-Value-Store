#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kvstore/lockfree_queue.h"
#include "kvstore/rpc_messages.h"
#include "kvstore/sharded_map.h"
#include "kvstore/wal.h"

namespace kvstore {

enum class Role { kFollower, kCandidate, kLeader };

// Abstracts how RaftNode talks to its peers. Production code implements
// this over real TCP (see net/raft_client.*); tests implement it with a
// direct in-process call into the peer's RaftNode, so election/replication
// logic can be exercised deterministically without sockets or timing
// flakiness in CI.
class RaftTransport {
public:
    virtual ~RaftTransport() = default;
    virtual bool send_request_vote(const std::string& peer_id, const RequestVoteArgs& args,
                                    RequestVoteReply& reply) = 0;
    virtual bool send_append_entries(const std::string& peer_id, const AppendEntriesArgs& args,
                                      AppendEntriesReply& reply) = 0;
};

struct RaftConfig {
    std::string node_id;
    std::vector<std::string> peer_ids;  // does not include node_id
    int election_timeout_min_ms = 150;
    int election_timeout_max_ms = 300;
    int heartbeat_interval_ms = 50;
};

// Simplified single-Raft-group implementation: leader election with terms
// and randomized timeouts, log replication with the prevLogIndex/Term
// consistency check, commit index advanced on majority match. Not
// implemented: log compaction/snapshotting, cluster membership changes,
// pre-vote. See README known limitations.
class RaftNode {
public:
    RaftNode(RaftConfig config, RaftTransport& transport, WriteAheadLog& wal, ShardedMap& state_machine);
    ~RaftNode();

    void start();
    void stop();

    // RPC handlers, called by the network layer when a peer's request
    // arrives (or directly by tests).
    RequestVoteReply handle_request_vote(const RequestVoteArgs& args);
    AppendEntriesReply handle_append_entries(const AppendEntriesArgs& args);

    // Client entry point for a write. Returns false with `redirect` set to
    // the current known leader if this node isn't the leader. Blocks until
    // the entry is committed (or a leadership change makes that impossible).
    bool propose(Command command, const std::string& key, const std::string& value,
                 std::string& redirect);

    bool is_leader() const;
    std::string leader_hint() const;  // best-known leader id, may be stale
    uint64_t current_term() const;

    // Exposed for tests that need to drive elections deterministically
    // without waiting on real timers.
    void force_election_timeout();
    void tick_once();  // one iteration of the timer loop, for tests

private:
    void run_ticker();
    void become_follower(uint64_t term);
    void start_election();
    void send_heartbeats_locked_release();
    void replicate_to_peer(const std::string& peer_id);
    void advance_commit_index_locked();
    void apply_committed_locked();
    void run_applier();

    uint64_t last_log_index_locked() const;
    uint64_t last_log_term_locked() const;
    uint64_t term_at_locked(uint64_t index) const;

    RaftConfig config_;
    RaftTransport& transport_;
    WriteAheadLog& wal_;
    ShardedMap& state_machine_;

    mutable std::mutex mutex_;
    std::condition_variable commit_cv_;
    Role role_ = Role::kFollower;
    uint64_t current_term_ = 0;
    std::string voted_for_;
    std::string current_leader_;
    std::vector<LogEntry> log_;  // 1-indexed conceptually; log_[i-1] has index i
    uint64_t commit_index_ = 0;
    uint64_t last_applied_ = 0;

    std::unordered_map<std::string, uint64_t> next_index_;
    std::unordered_map<std::string, uint64_t> match_index_;

    std::chrono::steady_clock::time_point last_heartbeat_seen_;
    std::chrono::milliseconds election_timeout_{0};

    SpscQueue<LogEntry> apply_queue_;
    std::thread ticker_thread_;
    std::thread applier_thread_;
    std::atomic<bool> running_{false};
};

}  // namespace kvstore
