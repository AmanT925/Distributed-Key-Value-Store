#include "kvstore/raft_node.h"

#include <algorithm>
#include <chrono>
#include <random>

namespace kvstore {

using namespace std::chrono;

namespace {
std::chrono::milliseconds random_election_timeout(const RaftConfig& cfg) {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(cfg.election_timeout_min_ms, cfg.election_timeout_max_ms);
    return std::chrono::milliseconds(dist(rng));
}
}  // namespace

RaftNode::RaftNode(RaftConfig config, RaftTransport& transport, WriteAheadLog& wal,
                    ShardedMap& state_machine)
    : config_(std::move(config)),
      transport_(transport),
      wal_(wal),
      state_machine_(state_machine),
      apply_queue_(4096) {
    log_ = wal_.read_all();
    if (!log_.empty()) {
        commit_index_ = log_.back().index;
        for (const auto& e : log_) {
            if (e.command == Command::kPut) state_machine_.put(e.key, e.value);
            else state_machine_.erase(e.key);
        }
        last_applied_ = commit_index_;
    }
    last_heartbeat_seen_ = steady_clock::now();
    election_timeout_ = random_election_timeout(config_);
}

RaftNode::~RaftNode() { stop(); }

void RaftNode::start() {
    running_ = true;
    ticker_thread_ = std::thread(&RaftNode::run_ticker, this);
    applier_thread_ = std::thread(&RaftNode::run_applier, this);
}

void RaftNode::stop() {
    if (!running_.exchange(false)) return;
    commit_cv_.notify_all();
    if (ticker_thread_.joinable()) ticker_thread_.join();
    if (applier_thread_.joinable()) applier_thread_.join();
}

uint64_t RaftNode::last_log_index_locked() const {
    return log_.empty() ? 0 : log_.back().index;
}

uint64_t RaftNode::last_log_term_locked() const {
    return log_.empty() ? 0 : log_.back().term;
}

uint64_t RaftNode::term_at_locked(uint64_t index) const {
    if (index == 0 || index > log_.size()) return 0;
    return log_[index - 1].term;
}

void RaftNode::become_follower(uint64_t term) {
    if (term > current_term_) {
        current_term_ = term;
        voted_for_.clear();
    }
    role_ = Role::kFollower;
}

void RaftNode::run_ticker() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (!running_) break;
        tick_once();
    }
}

void RaftNode::tick_once() {
    bool should_send_heartbeats = false;
    bool should_start_election = false;
    {
        std::lock_guard lock(mutex_);
        auto now = steady_clock::now();
        if (role_ == Role::kLeader) {
            should_send_heartbeats = true;
        } else if (now - last_heartbeat_seen_ >= election_timeout_) {
            should_start_election = true;
        }
    }
    if (should_send_heartbeats) send_heartbeats_locked_release();
    if (should_start_election) start_election();
}

void RaftNode::force_election_timeout() {
    std::lock_guard lock(mutex_);
    last_heartbeat_seen_ = steady_clock::now() - election_timeout_ - std::chrono::milliseconds(1);
}

void RaftNode::send_heartbeats_locked_release() {
    for (const auto& peer : config_.peer_ids) {
        replicate_to_peer(peer);
    }
}

void RaftNode::replicate_to_peer(const std::string& peer_id) {
    AppendEntriesArgs args;
    uint64_t my_term;
    {
        std::lock_guard lock(mutex_);
        if (role_ != Role::kLeader) return;
        uint64_t next_idx = next_index_.count(peer_id) ? next_index_[peer_id] : last_log_index_locked() + 1;
        args.term = current_term_;
        args.leader_id = config_.node_id;
        args.prev_log_index = next_idx > 0 ? next_idx - 1 : 0;
        args.prev_log_term = term_at_locked(args.prev_log_index);
        args.leader_commit = commit_index_;
        for (uint64_t idx = next_idx; idx <= last_log_index_locked(); ++idx) {
            args.entries.push_back(log_[idx - 1]);
        }
        my_term = current_term_;
    }

    AppendEntriesReply reply;
    if (!transport_.send_append_entries(peer_id, args, reply)) return;  // peer unreachable, retry next tick

    std::lock_guard lock(mutex_);
    if (role_ != Role::kLeader || current_term_ != my_term) return;
    if (reply.term > current_term_) {
        become_follower(reply.term);
        return;
    }
    if (reply.success) {
        match_index_[peer_id] = reply.match_index;
        next_index_[peer_id] = reply.match_index + 1;
        advance_commit_index_locked();
    } else {
        uint64_t cur = next_index_.count(peer_id) ? next_index_[peer_id] : last_log_index_locked() + 1;
        next_index_[peer_id] = cur > 1 ? cur - 1 : 1;
    }
}

void RaftNode::advance_commit_index_locked() {
    uint64_t last = last_log_index_locked();
    for (uint64_t n = last; n > commit_index_; --n) {
        if (term_at_locked(n) != current_term_) continue;  // only commit current-term entries directly
        size_t count = 1;  // self
        for (const auto& [peer, match] : match_index_) {
            if (match >= n) count++;
        }
        if (count * 2 > config_.peer_ids.size() + 1) {
            commit_index_ = n;
            apply_committed_locked();
            commit_cv_.notify_all();
            break;
        }
    }
}

void RaftNode::apply_committed_locked() {
    while (last_applied_ < commit_index_) {
        last_applied_++;
        apply_queue_.push(log_[last_applied_ - 1]);
    }
}

void RaftNode::run_applier() {
    while (running_) {
        auto entry = apply_queue_.pop();
        if (!entry) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        if (entry->command == Command::kPut) {
            state_machine_.put(entry->key, entry->value);
        } else {
            state_machine_.erase(entry->key);
        }
    }
}

void RaftNode::start_election() {
    uint64_t term;
    uint64_t last_idx, last_term;
    {
        std::lock_guard lock(mutex_);
        role_ = Role::kCandidate;
        current_term_++;
        voted_for_ = config_.node_id;
        last_heartbeat_seen_ = steady_clock::now();
        election_timeout_ = random_election_timeout(config_);
        term = current_term_;
        last_idx = last_log_index_locked();
        last_term = last_log_term_locked();
    }

    RequestVoteArgs args{term, config_.node_id, last_idx, last_term};
    size_t votes = 1;  // vote for self

    for (const auto& peer : config_.peer_ids) {
        RequestVoteReply reply;
        if (!transport_.send_request_vote(peer, args, reply)) continue;

        std::lock_guard lock(mutex_);
        if (reply.term > current_term_) {
            become_follower(reply.term);
            return;
        }
        if (role_ != Role::kCandidate || current_term_ != term) return;
        if (reply.vote_granted) votes++;
    }

    std::lock_guard lock(mutex_);
    if (role_ != Role::kCandidate || current_term_ != term) return;
    if (votes * 2 > config_.peer_ids.size() + 1) {
        role_ = Role::kLeader;
        current_leader_ = config_.node_id;
        uint64_t next = last_log_index_locked() + 1;
        for (const auto& peer : config_.peer_ids) {
            next_index_[peer] = next;
            match_index_[peer] = 0;
        }
    }
}

RequestVoteReply RaftNode::handle_request_vote(const RequestVoteArgs& args) {
    std::lock_guard lock(mutex_);
    if (args.term < current_term_) return {current_term_, false};
    if (args.term > current_term_) become_follower(args.term);

    bool log_ok = (args.last_log_term > last_log_term_locked()) ||
                  (args.last_log_term == last_log_term_locked() && args.last_log_index >= last_log_index_locked());
    bool grant = log_ok && (voted_for_.empty() || voted_for_ == args.candidate_id);
    if (grant) {
        voted_for_ = args.candidate_id;
        last_heartbeat_seen_ = steady_clock::now();
    }
    return {current_term_, grant};
}

AppendEntriesReply RaftNode::handle_append_entries(const AppendEntriesArgs& args) {
    std::lock_guard lock(mutex_);
    if (args.term < current_term_) return {current_term_, false, 0};

    become_follower(args.term);
    current_term_ = std::max(current_term_, args.term);
    current_leader_ = args.leader_id;
    last_heartbeat_seen_ = steady_clock::now();

    if (args.prev_log_index > 0) {
        if (args.prev_log_index > last_log_index_locked()) return {current_term_, false, 0};
        if (term_at_locked(args.prev_log_index) != args.prev_log_term) return {current_term_, false, 0};
    }

    uint64_t idx = args.prev_log_index;
    for (const auto& e : args.entries) {
        idx++;
        if (idx <= last_log_index_locked()) {
            if (term_at_locked(idx) != e.term) {
                log_.resize(idx - 1);
                log_.push_back(e);
                wal_.append(e);
            }
        } else {
            log_.push_back(e);
            wal_.append(e);
        }
    }

    if (args.leader_commit > commit_index_) {
        commit_index_ = std::min(args.leader_commit, last_log_index_locked());
        apply_committed_locked();
        commit_cv_.notify_all();
    }
    return {current_term_, true, last_log_index_locked()};
}

bool RaftNode::propose(Command command, const std::string& key, const std::string& value,
                        std::string& redirect) {
    uint64_t entry_index;
    uint64_t term;
    {
        std::lock_guard lock(mutex_);
        if (role_ != Role::kLeader) {
            redirect = current_leader_;
            return false;
        }
        LogEntry entry;
        entry.term = current_term_;
        entry.index = last_log_index_locked() + 1;
        entry.command = command;
        entry.key = key;
        entry.value = value;
        log_.push_back(entry);
        wal_.append(entry);
        entry_index = entry.index;
        term = current_term_;
    }

    for (const auto& peer : config_.peer_ids) {
        replicate_to_peer(peer);
    }

    std::unique_lock lock(mutex_);
    bool committed = commit_cv_.wait_for(lock, std::chrono::milliseconds(2000), [&] {
        return commit_index_ >= entry_index || role_ != Role::kLeader || current_term_ != term;
    });
    if (committed && commit_index_ >= entry_index) return true;
    redirect = current_leader_;
    return false;
}

bool RaftNode::is_leader() const {
    std::lock_guard lock(mutex_);
    return role_ == Role::kLeader;
}

std::string RaftNode::leader_hint() const {
    std::lock_guard lock(mutex_);
    return current_leader_;
}

uint64_t RaftNode::current_term() const {
    std::lock_guard lock(mutex_);
    return current_term_;
}

}  // namespace kvstore
