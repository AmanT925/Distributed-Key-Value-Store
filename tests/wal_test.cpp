#include "kvstore/wal.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>

using namespace kvstore;

namespace {
std::string temp_wal_path(const char* name) {
    return (std::filesystem::temp_directory_path() / name).string();
}
}  // namespace

TEST(WriteAheadLog, AppendAndReadBack) {
    std::string path = temp_wal_path("kvstore_wal_test1.wal");
    std::remove(path.c_str());
    {
        WriteAheadLog wal(path);
        wal.append({1, 1, Command::kPut, "a", "1"});
        wal.append({1, 2, Command::kPut, "b", "2"});
        wal.append({2, 3, Command::kDelete, "a", ""});
    }

    WriteAheadLog wal2(path);
    auto entries = wal2.read_all();
    ASSERT_EQ(entries.size(), 3u);
    EXPECT_EQ(entries[0].key, "a");
    EXPECT_EQ(entries[0].value, "1");
    EXPECT_EQ(entries[1].key, "b");
    EXPECT_EQ(entries[2].command, Command::kDelete);
    EXPECT_EQ(entries[2].term, 2u);
    std::remove(path.c_str());
}

TEST(WriteAheadLog, SurvivesRestartLikeReopen) {
    std::string path = temp_wal_path("kvstore_wal_test2.wal");
    std::remove(path.c_str());

    {
        WriteAheadLog wal(path);
        for (uint64_t i = 1; i <= 50; ++i) {
            wal.append({1, i, Command::kPut, "key" + std::to_string(i), "value" + std::to_string(i)});
        }
    }
    {
        // Simulates a process restart: fresh WriteAheadLog instance over the same file.
        WriteAheadLog wal(path);
        auto entries = wal.read_all();
        ASSERT_EQ(entries.size(), 50u);
        EXPECT_EQ(entries[49].index, 50u);
        EXPECT_EQ(entries[49].key, "key50");
    }
    std::remove(path.c_str());
}

TEST(WriteAheadLog, EmptyFileReadsBackNothing) {
    std::string path = temp_wal_path("kvstore_wal_test3.wal");
    std::remove(path.c_str());
    WriteAheadLog wal(path);
    EXPECT_TRUE(wal.read_all().empty());
    std::remove(path.c_str());
}
