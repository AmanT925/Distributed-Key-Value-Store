#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "kvstore/net.h"
#include "kvstore/protocol.h"

namespace kvstore {

namespace {
bool split_host_port(const std::string& address, std::string& host, std::string& port) {
    auto pos = address.find(':');
    if (pos == std::string::npos) return false;
    host = address.substr(0, pos);
    port = address.substr(pos + 1);
    return true;
}
}  // namespace

int dial(const std::string& address) {
    std::string host, port;
    if (!split_host_port(address, host, port)) return -1;

    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) return -1;

    int fd = -1;
    for (auto* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

TcpRaftTransport::TcpRaftTransport(std::unordered_map<std::string, std::string> peer_addresses)
    : peer_addresses_(std::move(peer_addresses)) {}

bool TcpRaftTransport::send_request_vote(const std::string& peer_id, const RequestVoteArgs& args,
                                          RequestVoteReply& reply) {
    auto it = peer_addresses_.find(peer_id);
    if (it == peer_addresses_.end()) return false;
    int fd = dial(it->second);
    if (fd < 0) return false;

    bool ok = send_frame(fd, MsgType::kRequestVote, args.serialize());
    if (ok) {
        MsgType type;
        std::string payload;
        ok = recv_frame(fd, type, payload) && type == MsgType::kRequestVoteReply;
        if (ok) reply = RequestVoteReply::deserialize(payload);
    }
    ::close(fd);
    return ok;
}

bool TcpRaftTransport::send_append_entries(const std::string& peer_id, const AppendEntriesArgs& args,
                                            AppendEntriesReply& reply) {
    auto it = peer_addresses_.find(peer_id);
    if (it == peer_addresses_.end()) return false;
    int fd = dial(it->second);
    if (fd < 0) return false;

    bool ok = send_frame(fd, MsgType::kAppendEntries, args.serialize());
    if (ok) {
        MsgType type;
        std::string payload;
        ok = recv_frame(fd, type, payload) && type == MsgType::kAppendEntriesReply;
        if (ok) reply = AppendEntriesReply::deserialize(payload);
    }
    ::close(fd);
    return ok;
}

}  // namespace kvstore
