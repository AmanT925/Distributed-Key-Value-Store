#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>

namespace kvstore {

// Wire message types, shared by the client protocol and the inter-node
// Raft RPCs. Framing on the wire is: [4-byte big-endian length][1-byte
// type][payload], where length covers type+payload.
enum class MsgType : uint8_t {
    kGet = 1,
    kPut = 2,
    kDelete = 3,
    kResponse = 4,
    kRequestVote = 5,
    kRequestVoteReply = 6,
    kAppendEntries = 7,
    kAppendEntriesReply = 8,
};

// Minimal binary serialization helpers used by the RPC message structs.
class ByteWriter {
public:
    void write_u8(uint8_t v) { buf_.push_back(static_cast<char>(v)); }

    void write_u32(uint32_t v) {
        uint32_t be = htonl_(v);
        buf_.append(reinterpret_cast<const char*>(&be), sizeof(be));
    }

    void write_u64(uint64_t v) {
        uint64_t be = htonll_(v);
        buf_.append(reinterpret_cast<const char*>(&be), sizeof(be));
    }

    void write_string(const std::string& s) {
        write_u32(static_cast<uint32_t>(s.size()));
        buf_.append(s);
    }

    const std::string& bytes() const { return buf_; }

private:
    static uint32_t htonl_(uint32_t v) {
        return ((v & 0x000000FFu) << 24) | ((v & 0x0000FF00u) << 8) |
               ((v & 0x00FF0000u) >> 8) | ((v & 0xFF000000u) >> 24);
    }
    static uint64_t htonll_(uint64_t v) {
        return (static_cast<uint64_t>(htonl_(static_cast<uint32_t>(v))) << 32) |
               htonl_(static_cast<uint32_t>(v >> 32));
    }

    std::string buf_;
};

class ByteReader {
public:
    explicit ByteReader(const std::string& data) : data_(data) {}

    uint8_t read_u8() {
        require(1);
        return static_cast<uint8_t>(data_[pos_++]);
    }

    uint32_t read_u32() {
        require(4);
        uint32_t be;
        std::memcpy(&be, data_.data() + pos_, 4);
        pos_ += 4;
        return ntohl_(be);
    }

    uint64_t read_u64() {
        require(8);
        uint64_t be;
        std::memcpy(&be, data_.data() + pos_, 8);
        pos_ += 8;
        return ntohll_(be);
    }

    std::string read_string() {
        uint32_t len = read_u32();
        require(len);
        std::string s = data_.substr(pos_, len);
        pos_ += len;
        return s;
    }

private:
    void require(size_t n) const {
        if (pos_ + n > data_.size()) throw std::runtime_error("ByteReader: truncated message");
    }
    static uint32_t ntohl_(uint32_t v) {
        return ((v & 0x000000FFu) << 24) | ((v & 0x0000FF00u) << 8) |
               ((v & 0x00FF0000u) >> 8) | ((v & 0xFF000000u) >> 24);
    }
    static uint64_t ntohll_(uint64_t v) {
        return (static_cast<uint64_t>(ntohl_(static_cast<uint32_t>(v))) << 32) |
               ntohl_(static_cast<uint32_t>(v >> 32));
    }

    const std::string& data_;
    size_t pos_ = 0;
};

// Blocking, fully-looped read/write over a connected socket fd. Returns
// false on EOF or error. Used by both the server's per-connection thread
// and any outbound RPC client connection.
bool write_exact(int fd, const char* data, size_t len);
bool read_exact(int fd, char* data, size_t len);
bool send_frame(int fd, MsgType type, const std::string& payload);
bool recv_frame(int fd, MsgType& type, std::string& payload);

}  // namespace kvstore
