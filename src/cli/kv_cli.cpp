#include <unistd.h>

#include <iostream>

#include "kvstore/net.h"
#include "kvstore/protocol.h"
#include "kvstore/rpc_messages.h"

using namespace kvstore;

namespace {

bool do_request(const std::string& address, MsgType type, const std::string& payload,
                 KvResponse& out) {
    int fd = dial(address);
    if (fd < 0) {
        std::cerr << "failed to connect to " << address << std::endl;
        return false;
    }
    bool ok = send_frame(fd, type, payload);
    if (ok) {
        MsgType reply_type;
        std::string reply_payload;
        ok = recv_frame(fd, reply_type, reply_payload);
        if (ok) out = KvResponse::deserialize(reply_payload);
    }
    ::close(fd);
    return ok;
}

void print_usage() {
    std::cerr << "usage:\n"
              << "  kv_cli <host:port> get <key>\n"
              << "  kv_cli <host:port> put <key> <value>\n"
              << "  kv_cli <host:port> delete <key>\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 4) {
        print_usage();
        return 1;
    }
    std::string address = argv[1];
    std::string op = argv[2];
    KvResponse resp;
    bool ok = false;

    if (op == "get" && argc == 4) {
        ok = do_request(address, MsgType::kGet, GetRequest{argv[3]}.serialize(), resp);
    } else if (op == "put" && argc == 5) {
        ok = do_request(address, MsgType::kPut, PutRequest{argv[3], argv[4]}.serialize(), resp);
    } else if (op == "delete" && argc == 4) {
        ok = do_request(address, MsgType::kDelete, DeleteRequest{argv[3]}.serialize(), resp);
    } else {
        print_usage();
        return 1;
    }

    if (!ok) {
        std::cerr << "request failed (connection error)" << std::endl;
        return 1;
    }
    if (!resp.error.empty()) {
        std::cerr << "error: " << resp.error;
        if (!resp.value.empty()) std::cerr << " (leader hint: " << resp.value << ")";
        std::cerr << std::endl;
        return 2;
    }
    if (op == "get") {
        if (resp.found) {
            std::cout << resp.value << std::endl;
        } else {
            std::cout << "(not found)" << std::endl;
        }
    } else {
        std::cout << "OK" << std::endl;
    }
    return 0;
}
