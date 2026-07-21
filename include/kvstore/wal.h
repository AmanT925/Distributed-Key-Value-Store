#pragma once

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

namespace kvstore {

enum class Command : uint8_t { kPut = 1, kDelete = 2 };

struct LogEntry {
    uint64_t term = 0;
    uint64_t index = 0;
    Command command = Command::kPut;
    std::string key;
    std::string value;  // empty for kDelete
};

// Append-only log file. This doubles as both the Raft persistent log and
// the application WAL (the same approach etcd uses) — there's no separate
// redundant log. Every append is fsynced before the caller is told it's
// durable. No compaction/snapshotting yet: the file grows unboundedly
// (see README known limitations).
class WriteAheadLog {
public:
    explicit WriteAheadLog(std::string path);

    // Appends and fsyncs a single entry, returns false on I/O failure.
    bool append(const LogEntry& entry);

    // Replays every entry currently on disk, in order, into the callback.
    // Used both at startup (to rebuild state) and to reload a node's log.
    std::vector<LogEntry> read_all() const;

    // Truncates the on-disk log to a fresh empty file (used by tests only).
    void reset();

    ~WriteAheadLog();

private:
    std::string path_;
    mutable std::mutex mutex_;
    FILE* file_ = nullptr;
};

}  // namespace kvstore
