#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kvstore/protocol.h"
#include "kvstore/wal.h"

namespace kvstore {

// ---- Client <-> node messages ----

struct GetRequest {
    std::string key;

    std::string serialize() const {
        ByteWriter w;
        w.write_string(key);
        return w.bytes();
    }
    static GetRequest deserialize(const std::string& bytes) {
        ByteReader r(bytes);
        return {r.read_string()};
    }
};

struct PutRequest {
    std::string key;
    std::string value;

    std::string serialize() const {
        ByteWriter w;
        w.write_string(key);
        w.write_string(value);
        return w.bytes();
    }
    static PutRequest deserialize(const std::string& bytes) {
        ByteReader r(bytes);
        PutRequest req;
        req.key = r.read_string();
        req.value = r.read_string();
        return req;
    }
};

struct DeleteRequest {
    std::string key;

    std::string serialize() const {
        ByteWriter w;
        w.write_string(key);
        return w.bytes();
    }
    static DeleteRequest deserialize(const std::string& bytes) {
        ByteReader r(bytes);
        return {r.read_string()};
    }
};

// Generic response for all client operations.
struct KvResponse {
    bool ok = false;
    bool found = false;  // meaningful for GET
    std::string value;
    std::string error;  // e.g. "NOT_LEADER", with redirect info in `value`

    std::string serialize() const {
        ByteWriter w;
        w.write_u8(ok ? 1 : 0);
        w.write_u8(found ? 1 : 0);
        w.write_string(value);
        w.write_string(error);
        return w.bytes();
    }
    static KvResponse deserialize(const std::string& bytes) {
        ByteReader r(bytes);
        KvResponse resp;
        resp.ok = r.read_u8() != 0;
        resp.found = r.read_u8() != 0;
        resp.value = r.read_string();
        resp.error = r.read_string();
        return resp;
    }
};

// ---- Raft inter-node RPCs ----

struct RequestVoteArgs {
    uint64_t term = 0;
    std::string candidate_id;
    uint64_t last_log_index = 0;
    uint64_t last_log_term = 0;

    std::string serialize() const {
        ByteWriter w;
        w.write_u64(term);
        w.write_string(candidate_id);
        w.write_u64(last_log_index);
        w.write_u64(last_log_term);
        return w.bytes();
    }
    static RequestVoteArgs deserialize(const std::string& bytes) {
        ByteReader r(bytes);
        RequestVoteArgs a;
        a.term = r.read_u64();
        a.candidate_id = r.read_string();
        a.last_log_index = r.read_u64();
        a.last_log_term = r.read_u64();
        return a;
    }
};

struct RequestVoteReply {
    uint64_t term = 0;
    bool vote_granted = false;

    std::string serialize() const {
        ByteWriter w;
        w.write_u64(term);
        w.write_u8(vote_granted ? 1 : 0);
        return w.bytes();
    }
    static RequestVoteReply deserialize(const std::string& bytes) {
        ByteReader r(bytes);
        RequestVoteReply reply;
        reply.term = r.read_u64();
        reply.vote_granted = r.read_u8() != 0;
        return reply;
    }
};

struct AppendEntriesArgs {
    uint64_t term = 0;
    std::string leader_id;
    uint64_t prev_log_index = 0;
    uint64_t prev_log_term = 0;
    std::vector<LogEntry> entries;
    uint64_t leader_commit = 0;

    std::string serialize() const {
        ByteWriter w;
        w.write_u64(term);
        w.write_string(leader_id);
        w.write_u64(prev_log_index);
        w.write_u64(prev_log_term);
        w.write_u64(leader_commit);
        w.write_u32(static_cast<uint32_t>(entries.size()));
        for (const auto& e : entries) {
            w.write_u64(e.term);
            w.write_u64(e.index);
            w.write_u8(static_cast<uint8_t>(e.command));
            w.write_string(e.key);
            w.write_string(e.value);
        }
        return w.bytes();
    }
    static AppendEntriesArgs deserialize(const std::string& bytes) {
        ByteReader r(bytes);
        AppendEntriesArgs a;
        a.term = r.read_u64();
        a.leader_id = r.read_string();
        a.prev_log_index = r.read_u64();
        a.prev_log_term = r.read_u64();
        a.leader_commit = r.read_u64();
        uint32_t n = r.read_u32();
        a.entries.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            LogEntry e;
            e.term = r.read_u64();
            e.index = r.read_u64();
            e.command = static_cast<Command>(r.read_u8());
            e.key = r.read_string();
            e.value = r.read_string();
            a.entries.push_back(std::move(e));
        }
        return a;
    }
};

struct AppendEntriesReply {
    uint64_t term = 0;
    bool success = false;
    uint64_t match_index = 0;  // highest index known to match on success

    std::string serialize() const {
        ByteWriter w;
        w.write_u64(term);
        w.write_u8(success ? 1 : 0);
        w.write_u64(match_index);
        return w.bytes();
    }
    static AppendEntriesReply deserialize(const std::string& bytes) {
        ByteReader r(bytes);
        AppendEntriesReply reply;
        reply.term = r.read_u64();
        reply.success = r.read_u8() != 0;
        reply.match_index = r.read_u64();
        return reply;
    }
};

}  // namespace kvstore
