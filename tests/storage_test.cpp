#include "kvstore/sharded_map.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace kvstore;

TEST(ShardedMap, PutGetErase) {
    ShardedMap map;
    EXPECT_FALSE(map.get("a").has_value());
    map.put("a", "1");
    ASSERT_TRUE(map.get("a").has_value());
    EXPECT_EQ(*map.get("a"), "1");
    map.put("a", "2");
    EXPECT_EQ(*map.get("a"), "2");
    EXPECT_TRUE(map.erase("a"));
    EXPECT_FALSE(map.get("a").has_value());
    EXPECT_FALSE(map.erase("a"));
}

TEST(ShardedMap, SizeTracksDistinctKeys) {
    ShardedMap map;
    map.put("a", "1");
    map.put("b", "2");
    map.put("a", "3");  // overwrite, not a new key
    EXPECT_EQ(map.size(), 2u);
}

TEST(ShardedMap, ConcurrentWritesToDistinctKeysAllSucceed) {
    ShardedMap map;
    constexpr int kThreads = 16;
    constexpr int kPerThread = 2000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&map, t] {
            for (int i = 0; i < kPerThread; ++i) {
                std::string key = "t" + std::to_string(t) + "-" + std::to_string(i);
                map.put(key, "v");
            }
        });
    }
    for (auto& th : threads) th.join();

    EXPECT_EQ(map.size(), static_cast<size_t>(kThreads * kPerThread));
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kPerThread; i += 500) {
            std::string key = "t" + std::to_string(t) + "-" + std::to_string(i);
            EXPECT_TRUE(map.get(key).has_value());
        }
    }
}

TEST(ShardedMap, ConcurrentReadersAndWriterOnSameKeyStayConsistent) {
    ShardedMap map;
    map.put("shared", "0");
    std::atomic<bool> stop{false};
    std::atomic<int> bad_reads{0};

    std::thread writer([&] {
        for (int i = 1; i <= 5000; ++i) {
            map.put("shared", std::to_string(i));
        }
        stop = true;
    });

    std::vector<std::thread> readers;
    for (int i = 0; i < 8; ++i) {
        readers.emplace_back([&] {
            while (!stop.load()) {
                auto v = map.get("shared");
                if (!v.has_value()) bad_reads++;  // key must never appear absent once written
            }
        });
    }

    writer.join();
    for (auto& r : readers) r.join();
    EXPECT_EQ(bad_reads.load(), 0);
}
