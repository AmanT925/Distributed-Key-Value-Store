#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "kvstore/net.h"
#include "kvstore/protocol.h"

namespace kvstore {

NodeServer::NodeServer(std::string bind_address, RaftNode& raft, ShardedMap& store)
    : bind_address_(std::move(bind_address)), raft_(raft), store_(store) {}

NodeServer::~NodeServer() { stop(); }

void NodeServer::start() {
    std::string host, port_str;
    auto pos = bind_address_.find(':');
    host = bind_address_.substr(0, pos);
    port_str = bind_address_.substr(pos + 1);

    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    struct addrinfo* res = nullptr;
    getaddrinfo(host.empty() ? nullptr : host.c_str(), port_str.c_str(), &hints, &res);

    for (auto* p = res; p != nullptr; p = p->ai_next) {
        listen_fd_ = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (listen_fd_ < 0) continue;
        int opt = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        if (::bind(listen_fd_, p->ai_addr, p->ai_addrlen) == 0) break;
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    freeaddrinfo(res);
    if (listen_fd_ < 0) throw std::runtime_error("NodeServer: failed to bind " + bind_address_);

    ::listen(listen_fd_, 128);
    running_ = true;
    accept_thread_ = std::thread(&NodeServer::accept_loop, this);
}

void NodeServer::stop() {
    if (!running_.exchange(false)) return;
    if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);
    if (listen_fd_ >= 0) ::close(listen_fd_);
    if (accept_thread_.joinable()) accept_thread_.join();
    // Detached per-connection threads exit on their own once recv_frame
    // fails (peer closes, or process exit) -- not joined here.
}

void NodeServer::accept_loop() {
    while (running_) {
        int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) {
            if (!running_) break;
            continue;
        }
        std::thread(&NodeServer::handle_connection, this, fd).detach();
    }
}

void NodeServer::handle_connection(int fd) {
    while (running_) {
        MsgType type;
        std::string payload;
        if (!recv_frame(fd, type, payload)) break;

        switch (type) {
            case MsgType::kGet: {
                auto req = GetRequest::deserialize(payload);
                auto value = store_.get(req.key);
                KvResponse resp;
                resp.ok = true;
                resp.found = value.has_value();
                if (value) resp.value = *value;
                send_frame(fd, MsgType::kResponse, resp.serialize());
                break;
            }
            case MsgType::kPut: {
                auto req = PutRequest::deserialize(payload);
                std::string redirect;
                KvResponse resp;
                resp.ok = raft_.propose(Command::kPut, req.key, req.value, redirect);
                if (!resp.ok) {
                    resp.error = "NOT_LEADER";
                    resp.value = redirect;
                }
                send_frame(fd, MsgType::kResponse, resp.serialize());
                break;
            }
            case MsgType::kDelete: {
                auto req = DeleteRequest::deserialize(payload);
                std::string redirect;
                KvResponse resp;
                resp.ok = raft_.propose(Command::kDelete, req.key, "", redirect);
                if (!resp.ok) {
                    resp.error = "NOT_LEADER";
                    resp.value = redirect;
                }
                send_frame(fd, MsgType::kResponse, resp.serialize());
                break;
            }
            case MsgType::kRequestVote: {
                auto args = RequestVoteArgs::deserialize(payload);
                auto reply = raft_.handle_request_vote(args);
                send_frame(fd, MsgType::kRequestVoteReply, reply.serialize());
                break;
            }
            case MsgType::kAppendEntries: {
                auto args = AppendEntriesArgs::deserialize(payload);
                auto reply = raft_.handle_append_entries(args);
                send_frame(fd, MsgType::kAppendEntriesReply, reply.serialize());
                break;
            }
            default:
                break;
        }
    }
    ::close(fd);
}

}  // namespace kvstore
