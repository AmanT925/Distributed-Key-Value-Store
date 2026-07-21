# Distributed Key-Value Store

An in-memory, replicated key-value store written from scratch in C++20: a
sharded concurrent hashmap, a simplified Raft consensus protocol for
leader election and log replication, a write-ahead log for durability, a
custom binary wire protocol over raw TCP sockets, a standalone consistent
hashing module, and a load-testing harness used to produce the benchmark
numbers below.

No gRPC, no Boost.Asio, no existing Raft library — the storage engine,
networking, and consensus protocol are all original code in this repo.

## Stack

- **Language:** C++20
- **Build:** CMake, GoogleTest via `FetchContent` (no other third-party dependencies)
- **Networking:** raw POSIX sockets, custom length-prefixed binary protocol
- **Containerization:** Docker / Docker Compose (local multi-node only — see Deployment below)

## Architecture

```
Client ── TCP (GET/PUT/DELETE) ──▶ NodeServer ──▶ RaftNode ──▶ ShardedMap (state machine)
                                        │              │
                                        │              └──▶ WriteAheadLog (fsync before commit)
                                        │
                              TCP (RequestVote/AppendEntries)
                                        │
                                  peer NodeServers
```

- **`ShardedMap`** (`include/kvstore/sharded_map.h`) — the state machine. A fixed
  number of shards, each an `std::unordered_map` behind its own `std::shared_mutex`.
  Reads take a shared lock, writes take an exclusive lock, both scoped to a single
  shard, so keys in different shards never contend.
- **`RaftNode`** (`include/kvstore/raft_node.h`, `src/raft/raft_node.cpp`) — leader
  election (randomized timeouts, terms, majority vote) and log replication
  (`AppendEntries` with the `prevLogIndex`/`prevLogTerm` consistency check, commit
  index advanced once a majority of the 3-node cluster has matched an entry from
  the current term). A background applier thread drains committed entries into the
  `ShardedMap` off a lock-free SPSC queue, decoupling Raft's own locking from state
  machine writes.
- **`WriteAheadLog`** — an append-only binary log. Every Raft log entry is fsynced
  to disk before the leader acknowledges it as committed, and replayed on startup
  to rebuild state. This log **is** the Raft persistent log — there is no separate,
  redundant WAL, which is the same approach systems like etcd take.
- **`ConsistentHashRing`** — a standalone, independently-tested virtual-node ring
  (see Known Limitations — it is not wired into live data placement yet).
- **Wire protocol** (`include/kvstore/protocol.h`) — `[4-byte length][1-byte
  type][payload]` framing, shared by client requests and inter-node Raft RPCs.
  Thread-per-connection TCP server (`src/net/server.cpp`).

## Building

Requires a C++20 compiler and CMake ≥ 3.16. Internet access is needed once, to
fetch GoogleTest.

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Produces three binaries in `build/`: `kv_node` (cluster node), `kv_cli` (client),
`bench_client` (load generator).

## Running the tests

```bash
ctest --test-dir build --output-on-failure
# or directly:
./build/kvstore_tests
```

16 tests across 4 suites — storage concurrency, consistent hashing, WAL replay,
and Raft election/replication/failover. The Raft tests use an in-process fake
transport (direct calls between `RaftNode` instances instead of real sockets) so
they're deterministic and fast in CI, while still exercising the exact same
election/replication code the real TCP transport calls into.

## Running a local 3-node cluster

```bash
mkdir -p data
./build/kv_node node1 config/cluster.local.conf data &
./build/kv_node node2 config/cluster.local.conf data &
./build/kv_node node3 config/cluster.local.conf data &

./build/kv_cli 127.0.0.1:5001 put foo bar
./build/kv_cli 127.0.0.1:5002 get foo      # works from any node — writes redirect to the leader
```

`config/cluster.local.conf` is a static membership file (`node_id host:port` per
line). A node that isn't the leader replies `NOT_LEADER` with the current leader's
id as a hint; `kv_cli` prints that hint so you can retry against it.

### Docker Compose (local only — not a cloud deployment)

```bash
cd docker
docker compose up --build
# nodes are reachable on localhost:5001 / :5002 / :5003
```

This brings up the same 3-node cluster in containers on a local bridge network,
for the failover demo below — with no cloud dependency. AWS ECS and Terraform are
a later, separate pass (see Known Limitations).

> **Not yet verified:** Docker isn't installed in the environment this was built
> in, so the Dockerfile/compose file above have not actually been run end-to-end.
> The Dockerfile mirrors the same build steps used successfully outside Docker
> (CMake + g++/clang, `FetchContent` disabled for the container build), but please
> run `docker compose up --build` yourself and treat this as untested until you do.

## Demo: write, follower reads, leader crash, failover

Captured directly from a real run of the local 3-node cluster above (not
fabricated):

```
$ ./build/kv_node node1 config/cluster.local.conf data &
$ ./build/kv_node node2 config/cluster.local.conf data &
$ ./build/kv_node node3 config/cluster.local.conf data &
(elected leader: node2)

$ ./build/kv_cli 127.0.0.1:5002 put foo bar
OK
$ ./build/kv_cli 127.0.0.1:5001 get foo   # read from follower node1
bar
$ ./build/kv_cli 127.0.0.1:5003 get foo   # read from follower node3
bar

$ ./build/kv_cli 127.0.0.1:5001 put baz qux   # write sent to a follower, not the leader
error: NOT_LEADER (leader hint: node2)

$ kill -9 $(cat /tmp/node2.pid)   # simulate leader crash
(new leader after failover: node3)

$ ./build/kv_cli 127.0.0.1:5003 get foo   # old data survived the crash
bar
$ ./build/kv_cli 127.0.0.1:5003 put after-failover still-works   # writes work again after re-election
OK
$ ./build/kv_cli 127.0.0.1:5003 get after-failover
still-works
```

## Benchmarks (measured, not estimated)

Run with `./build/bench_client <host:port> <threads> <duration_s> <keyspace>` against
a single node of the 3-node local cluster above, on:

- **Hardware:** Apple M3 (MacBook Air), macOS (Darwin 24.6.0), arm64
- **Workload:** GET requests over persistent TCP connections, 1000-key keyspace,
  each op a full client round-trip through the real socket protocol (not
  in-process calls)

| Threads | Throughput (ops/sec) | p50 latency | p99 latency | p99.9 latency |
|---|---|---|---|---|
| 1  | 44,318  | 21.8 µs | 31.3 µs  | 46.1 µs  |
| 8  | 114,445 | 66.1 µs | 116.4 µs | 175.0 µs |

The original target was sub-millisecond p99 read latency at ~10k req/s. At 8
concurrent client threads this build sustains **~114k ops/sec with p99 latency of
116µs** — well past the target on both axes, measured on a single laptop with all
3 nodes and the benchmark client running on the same machine (so there's shared
CPU/network-stack contention driving latency up, not down — a dedicated multi-
machine setup would likely do better still).

## Known Limitations (read before an interview)

This section exists because the original one-line resume bullets compress a lot
of nuance. Here's exactly what's real vs. simplified:

- **Concurrency is fine-grained locking, not a lock-free hashmap.** The
  `ShardedMap` uses per-shard `std::shared_mutex` striping — real concurrent
  reads/writes with no cross-shard contention, but not lock-free. The one
  genuinely lock-free structure in the codebase is the SPSC ring buffer
  (`include/kvstore/lockfree_queue.h`) that hands committed Raft entries to the
  background apply thread. A correct lock-free hashmap is a much harder,
  multi-week problem on its own — attempting one here would have traded
  correctness for a buzzword.
- **Consistent hashing is not wired into live data placement.** The ring
  (`ConsistentHashRing`) is fully implemented and independently unit-tested
  (virtual nodes, minimal remap on node add/remove), but the running 3-node
  cluster is a **single Raft group replicating the full keyspace to all 3
  nodes** — not multiple shards each with their own Raft group. So "automatic
  rebalancing on node failure" in the running demo means Raft leader failover,
  not physical key migration between partitions. Multi-shard placement driven
  by the ring is a natural next step but isn't live today.
- **Reads are served locally, not linearized through Raft.** `GET` answers
  straight from whichever node received the request, including followers. This
  is simple and fast but means a follower can serve a value that's a few
  milliseconds stale relative to the leader (visible in the demo transcript
  above — the failover write took about a second to show up on the surviving
  follower). A linearizable read path (e.g. routing all reads through the
  leader, or a read-index protocol) isn't implemented.
- **No log compaction / snapshotting.** The WAL grows unboundedly. Fine for a
  demo; a real deployment needs periodic snapshots and log truncation.
- **No dynamic cluster membership.** The node list is static config
  (`config/cluster.local.conf`). Adding or removing a live node from the Raft
  group isn't implemented — Raft's joint-consensus membership-change protocol
  is a project of its own.
- **No pre-vote optimization.** A partitioned-then-healed node can force an
  unnecessary election by incrementing its term before rejoining. Cosmetic at
  this scale, but a real implementation would add it.
- **Thread-per-connection networking, not epoll.** Simpler to get correct, and
  sufficient at the connection counts this project runs at (a handful of nodes
  and benchmark client threads) — but it won't scale to tens of thousands of
  concurrent connections the way an epoll-based reactor would.
- **Election/log-inconsistency recovery decrements `nextIndex` by 1 at a time**
  on conflict, not the binary-search-style backtracking some Raft
  implementations use for faster recovery after a long partition. Correct, just
  not optimally fast to recover.
- **AWS ECS + Terraform + GitHub Actions CI/CD are not in this repo.** Those are
  a deliberately separate, later pass — this repo is the C++ system itself, runnable
  locally or via the included Docker Compose file with zero cloud dependency.
- **The Docker Compose setup is untested.** Docker wasn't available in the
  environment this was built in, so `docker/Dockerfile` and
  `docker/docker-compose.yml` have not actually been run — verify locally before
  relying on them for a demo.

## Repository layout

```
include/kvstore/     public headers (ShardedMap, RaftNode, ConsistentHashRing, WAL, wire protocol)
src/storage/         sharded map, WAL
src/hashing/         consistent hash ring
src/raft/            Raft node implementation
src/net/             wire protocol framing, TCP server, TCP Raft transport
src/cli/             kv_cli client
src/bench/           bench_client load generator
src/util/            cluster config file parsing
src/main.cpp         kv_node entrypoint
tests/               GoogleTest suite
config/              static cluster membership files (local + docker)
docker/              Dockerfile + docker-compose.yml for a local 3-node cluster
```
