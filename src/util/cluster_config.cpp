#include "kvstore/cluster_config.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

namespace kvstore {

std::unordered_map<std::string, std::string> load_cluster_config(const std::string& path) {
    std::unordered_map<std::string, std::string> nodes;
    std::ifstream in(path);
    if (!in) throw std::runtime_error("cannot open cluster config: " + path);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::istringstream iss(line);
        std::string id, addr;
        if (iss >> id >> addr) nodes[id] = addr;
    }
    return nodes;
}

}  // namespace kvstore
