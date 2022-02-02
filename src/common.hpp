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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "key.hpp"
#include "value.hpp"

/*######################################################################################
 * Global enum and constants
 *####################################################################################*/

/**
 * @brief A list of index read/write operations.
 *
 */
enum class IndexOperation : uint32_t
{
  kRead,
  kScan,
  kWrite,
  kInsert,
  kUpdate,
  kDelete
};

/**
 * @brief A list of the size of target keys.
 *
 */
enum KeySize
{
  k8 = 8,
  k16 = 16,
  k32 = 32,
  k64 = 64,
  k128 = 128
};

constexpr size_t kGCInterval = 100000;

constexpr size_t kGCThreadNum = 8;

constexpr bool kClosed = true;

constexpr bool kUseBulkload = true;

/*######################################################################################
 * Global utilities
 *####################################################################################*/

/**
 * @brief A class to represent bulkload entries.
 *
 */
template <class Key, class Payload>
struct Entry {
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Entry() = default;

  constexpr Entry(  //
      uint32_t k,
      uint32_t v)
      : key_{k}, payload_{v}
  {
  }

  constexpr Entry(const Entry &obj) = default;
  constexpr Entry(Entry &&obj) = default;

  constexpr auto operator=(const Entry &obj) -> Entry & = default;
  constexpr auto operator=(Entry &&obj) -> Entry & = default;

  ~Entry() = default;

  /*####################################################################################
   * Public getters
   *##################################################################################*/

  [[nodiscard]] constexpr auto
  GetKey() const  //
      -> Key
  {
    return Key{key_};
  }

  [[nodiscard]] constexpr auto
  GetPayload() const  //
      -> Payload
  {
    return Payload{payload_};
  }

  [[nodiscard]] constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return sizeof(Key);
  }

  [[nodiscard]] constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return sizeof(Payload);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a target key of this operation
  uint32_t key_{};

  /// a target data of this operation
  uint32_t payload_{};
};

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
    const size_t thread_num)  //
    -> std::vector<Entry<Key, Payload>>
{
  using Entry_t = Entry<Key, Payload>;

  std::vector<Entry_t> entries{size};

  // a lambda function for creating bulkload entries
  auto f = [&](const size_t begin_pos, const size_t end_pos) {
    for (uint32_t i = begin_pos; i < end_pos; ++i) {
      entries.at(i) = Entry_t{i, i};
    }
  };

  // prepare bulkload entries
  std::vector<std::thread> threads;
  size_t begin_pos = 0;
  for (size_t i = 0; i < thread_num; ++i) {
    const size_t exec_num = (size + i) / thread_num;
    const size_t end_pos = begin_pos + exec_num;
    threads.emplace_back(f, begin_pos, end_pos);
    begin_pos = end_pos;
  }
  for (auto &&thread : threads) {
    thread.join();
  }

  return entries;
}

#endif  // INDEX_BENCHMARK_COMMON_HPP
