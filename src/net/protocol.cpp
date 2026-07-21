#include "kvstore/protocol.h"

#include <sys/socket.h>
#include <unistd.h>

namespace kvstore {

bool write_exact(int fd, const char* data, size_t len) {
    size_t written = 0;
    while (written < len) {
        ssize_t n = ::send(fd, data + written, len - written, 0);
        if (n <= 0) return false;
        written += static_cast<size_t>(n);
    }
    return true;
}

bool read_exact(int fd, char* data, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = ::recv(fd, data + got, len - got, 0);
        if (n <= 0) return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

bool send_frame(int fd, MsgType type, const std::string& payload) {
    uint32_t body_len = static_cast<uint32_t>(payload.size() + 1);
    ByteWriter header;
    header.write_u32(body_len);
    header.write_u8(static_cast<uint8_t>(type));
    if (!write_exact(fd, header.bytes().data(), header.bytes().size())) return false;
    if (!payload.empty() && !write_exact(fd, payload.data(), payload.size())) return false;
    return true;
}

bool recv_frame(int fd, MsgType& type, std::string& payload) {
    char len_buf[4];
    if (!read_exact(fd, len_buf, 4)) return false;
    uint32_t body_len = (static_cast<uint8_t>(len_buf[0]) << 24) |
                         (static_cast<uint8_t>(len_buf[1]) << 16) |
                         (static_cast<uint8_t>(len_buf[2]) << 8) |
                         static_cast<uint8_t>(len_buf[3]);
    if (body_len < 1) return false;

    char type_buf;
    if (!read_exact(fd, &type_buf, 1)) return false;
    type = static_cast<MsgType>(static_cast<uint8_t>(type_buf));

    size_t payload_len = body_len - 1;
    payload.resize(payload_len);
    if (payload_len && !read_exact(fd, payload.data(), payload_len)) return false;
    return true;
}

}  // namespace kvstore
