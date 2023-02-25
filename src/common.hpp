/*
 * Copyright 2021 Database Group, Nagoya University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef INDEX_BENCHMARK_COMMON_HPP
#define INDEX_BENCHMARK_COMMON_HPP

// C++ standard libraries
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// external sources
#include "nlohmann/json.hpp"

// local sources
#include "key.hpp"

namespace dbgroup
{

/*######################################################################################
 * Global enum and constants
 *####################################################################################*/

/**
 * @brief A list of index read/write operations.
 *
 */
enum IndexOperation {
  kUndefinedOperation = -1,
  kRead,
  kScan,
  kFullScan,
  kWrite,
  kInsert,
  kUpdate,
  kDelete,
  kInsertOrUpdate,
  kDeleteAndInsert,
  kDeleteOrInsert,
  kInsertAndDelete,
};

// mapping for operation strings
NLOHMANN_JSON_SERIALIZE_ENUM(IndexOperation,
                             {
                                 {kUndefinedOperation, nullptr},
                                 {kRead, "read"},
                                 {kScan, "scan"},
                                 {kFullScan, "full scan"},
                                 {kWrite, "write"},
                                 {kInsert, "insert"},
                                 {kUpdate, "update"},
                                 {kDelete, "delete"},
                                 {kInsertOrUpdate, "insert or update"},
                                 {kDeleteAndInsert, "delete and insert"},
                                 {kDeleteOrInsert, "delete or insert"},
                                 {kInsertAndDelete, "insert and delete"},
                             })

enum AccessPattern {
  kUndefinedAccessPattern = -1,
  kRandom,
  kAscending,
  kDescending,
};

// mapping for access pattern strings
NLOHMANN_JSON_SERIALIZE_ENUM(AccessPattern,
                             {
                                 {kUndefinedAccessPattern, nullptr},
                                 {kRandom, "random"},
                                 {kAscending, "ascending"},
                                 {kDescending, "descending"},
                             })

enum Partitioning {
  kUndefinedPartitioning = -1,
  kNone,
  kRange,
  kStripe,
};

// mapping for access pattern strings
NLOHMANN_JSON_SERIALIZE_ENUM(Partitioning,
                             {
                                 {kUndefinedPartitioning, nullptr},
                                 {kNone, "none"},
                                 {kRange, "range"},
                                 {kStripe, "stripe"},
                             })

/**
 * @brief A list of the size of target keys.
 *
 */
enum KeySize {
  k8 = 8,
  k16 = 16,
  k32 = 32,
  k64 = 64,
  k128 = 128,
};

constexpr int kSuccess = 0;

constexpr int kFailed = -1;

constexpr size_t kMaxCoreNum = INDEX_BENCH_MAX_CORES;

constexpr size_t kGCInterval = 100000;

constexpr size_t kGCThreadNum = 8;

constexpr size_t kScanSize = 128;

constexpr bool kClosed = true;

constexpr bool kUseBulkload = true;

#ifdef INDEX_BENCH_BUILD_LONG_KEYS
constexpr bool kBuildLongKeys = true;
#else
constexpr bool kBuildLongKeys = false;
#endif

#ifdef INDEX_BENCH_COMPARE_WITH_SOTA
constexpr bool kUseIntegerKeys = true;
#else
constexpr bool kUseIntegerKeys = false;
#endif

constexpr double kEpsilon = 0.001;

/*######################################################################################
 * Global utilities
 *####################################################################################*/

constexpr auto
AlmostEqual(  //
    const double a,
    const double b)  //
    -> bool
{
  return fabs(a - b) <= kEpsilon;
}

/**
 * @brief Create key/value entries for bulkloading.
 *
 * @tparam Key
 * @tparam Payload
 * @param size
 * @param thread_num
 * @return std::vector<Entry<Key, Payload>>
 */
template <class Key, class Payload>
auto
PrepareBulkLoadEntries(  //
    const size_t size,
    const size_t thread_num,
    const int64_t seed = -1)  //
    -> std::vector<std::pair<Key, Payload>>
{
  std::vector<std::pair<Key, Payload>> entries{size};

  // a lambda function for creating bulkload entries
  auto f = [&](const size_t begin_pos, const size_t n, const size_t thread_id) {
    const size_t end_pos = begin_pos + n;
    if (seed < 0) {
      for (uint32_t i = begin_pos; i < end_pos; ++i) {
        entries.at(i) = {Key{i}, Payload{i}};
      }
    } else {
      for (uint32_t i = begin_pos, k = thread_id; i < end_pos; ++i, k += thread_num) {
        entries.at(i) = {Key{k}, Payload{k}};
      }
      auto &&begin_it = std::next(entries.begin(), begin_pos);
      auto &&end_it = std::next(begin_it, n);
      std::shuffle(begin_it, end_it, std::mt19937_64{static_cast<size_t>(seed)});
    }
  };

  // prepare bulkload entries
  std::vector<std::thread> threads{};
  size_t begin_pos = 0;
  for (size_t i = 0; i < thread_num; ++i) {
    const size_t n = (size + ((thread_num - 1) - i)) / thread_num;
    threads.emplace_back(f, begin_pos, n, i);
    begin_pos += n;
  }
  for (auto &&thread : threads) {
    thread.join();
  }

  return entries;
}

template <template <class K, class V> class Index>
constexpr auto
HasSetUpTearDown()  //
    -> bool
{
  return false;
}

}  // namespace dbgroup

#endif  // INDEX_BENCHMARK_COMMON_HPP
