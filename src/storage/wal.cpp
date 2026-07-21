#include "kvstore/wal.h"

#include <unistd.h>

#include <cstring>

namespace kvstore {

namespace {

bool write_all(FILE* f, const void* data, size_t len) {
    return std::fwrite(data, 1, len, f) == len;
}

template <typename T>
bool write_pod(FILE* f, const T& value) {
    return write_all(f, &value, sizeof(value));
}

template <typename T>
bool read_pod(FILE* f, T& value) {
    return std::fread(&value, 1, sizeof(value), f) == sizeof(value);
}

}  // namespace

WriteAheadLog::WriteAheadLog(std::string path) : path_(std::move(path)) {
    // Open in read+append binary mode, creating the file if absent.
    file_ = std::fopen(path_.c_str(), "a+b");
}

WriteAheadLog::~WriteAheadLog() {
    if (file_) std::fclose(file_);
}

bool WriteAheadLog::append(const LogEntry& entry) {
    std::lock_guard lock(mutex_);
    if (!file_) return false;

    uint32_t key_len = static_cast<uint32_t>(entry.key.size());
    uint32_t val_len = static_cast<uint32_t>(entry.value.size());
    uint8_t cmd = static_cast<uint8_t>(entry.command);

    bool ok = write_pod(file_, entry.term) && write_pod(file_, entry.index) &&
              write_pod(file_, cmd) && write_pod(file_, key_len) &&
              write_all(file_, entry.key.data(), key_len) &&
              write_pod(file_, val_len) &&
              write_all(file_, entry.value.data(), val_len);
    if (!ok) return false;

    if (std::fflush(file_) != 0) return false;
    return ::fsync(fileno(file_)) == 0;
}

std::vector<LogEntry> WriteAheadLog::read_all() const {
    std::lock_guard lock(mutex_);
    std::vector<LogEntry> entries;
    if (!file_) return entries;

    std::fflush(file_);
    FILE* in = std::fopen(path_.c_str(), "rb");
    if (!in) return entries;

    while (true) {
        LogEntry entry;
        uint8_t cmd;
        uint32_t key_len, val_len;
        if (!read_pod(in, entry.term)) break;
        if (!read_pod(in, entry.index)) break;
        if (!read_pod(in, cmd)) break;
        if (!read_pod(in, key_len)) break;
        entry.key.resize(key_len);
        if (key_len && std::fread(entry.key.data(), 1, key_len, in) != key_len) break;
        if (!read_pod(in, val_len)) break;
        entry.value.resize(val_len);
        if (val_len && std::fread(entry.value.data(), 1, val_len, in) != val_len) break;
        entry.command = static_cast<Command>(cmd);
        entries.push_back(std::move(entry));
    }
    std::fclose(in);
    return entries;
}

void WriteAheadLog::reset() {
    std::lock_guard lock(mutex_);
    if (file_) std::fclose(file_);
    file_ = std::fopen(path_.c_str(), "w+b");
}

}  // namespace kvstore
