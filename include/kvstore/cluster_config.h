#pragma once

#include <string>
#include <unordered_map>

namespace kvstore {

// Static cluster membership: node_id -> "host:port". Parsed from a plain
// text file, one "node_id host:port" pair per line (blank lines and lines
// starting with '#' are ignored). No dynamic membership changes -- see
// README known limitations.
std::unordered_map<std::string, std::string> load_cluster_config(const std::string& path);

}  // namespace kvstore
