cmake_minimum_required(VERSION 3.16)
project(kvstore CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

add_library(kvstore_core
  src/storage/sharded_map.cpp
  src/storage/wal.cpp
  src/hashing/consistent_hash.cpp
  src/raft/raft_node.cpp
  src/net/protocol.cpp
  src/net/raft_client.cpp
  src/net/server.cpp
  src/util/cluster_config.cpp
)
target_include_directories(kvstore_core PUBLIC include)
target_link_libraries(kvstore_core PUBLIC Threads::Threads)

add_executable(kv_node src/main.cpp)
target_link_libraries(kv_node PRIVATE kvstore_core)

add_executable(kv_cli src/cli/kv_cli.cpp)
target_link_libraries(kv_cli PRIVATE kvstore_core)

add_executable(bench_client src/bench/bench_client.cpp)
target_link_libraries(bench_client PRIVATE kvstore_core)

option(KVSTORE_BUILD_TESTS "Build the GoogleTest suite" ON)
if(KVSTORE_BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()
  add_executable(kvstore_tests
    tests/storage_test.cpp
    tests/hashing_test.cpp
    tests/wal_test.cpp
    tests/raft_test.cpp
  )
  target_link_libraries(kvstore_tests PRIVATE kvstore_core GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(kvstore_tests)
endif()
