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

#ifndef INDEX_BENCHMARK_INDEXES_YAKUSHIMA_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_YAKUSHIMA_WRAPPER_HPP

#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common.hpp"
#include "yakushima/include/kvs.h"

template <class Key, class Payload>
class YakushimaWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Entry_t = Entry<Key, Payload>;
  using status = ::yakushima::status;
  using Token = ::yakushima::Token;

 public:
  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  explicit YakushimaWrapper([[maybe_unused]] const size_t worker_num)
  {
    ::yakushima::init();
    ::yakushima::create_storage(table_name_);
  }

  ~YakushimaWrapper() { ::yakushima::fin(); }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  constexpr void
  SetUp()
  {
    ::yakushima::enter(token_);
  }

  constexpr void
  TearDown()
  {
    ::yakushima::leave(token_);
  }

  constexpr auto
  Bulkload(  //
      [[maybe_unused]] const std::vector<Entry_t> &entries,
      [[maybe_unused]] const size_t thread_num)  //
      -> bool
  {
    return false;
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    // get a value/size pair
    std::pair<Payload *, size_t> ret{};
    const auto rc = ::yakushima::get(table_name_, ToStrView(key), ret);

    // copy a gotten value if exist
    if (rc != status::OK) return std::nullopt;
    Payload value{};
    memcpy(&value, ret.first, sizeof(Payload));

    return value;
  }

  void
  Scan(  //
      [[maybe_unused]] const Key &begin_key,
      [[maybe_unused]] const size_t scan_range)
  {
    // prepare begin/end keys
    const auto &begin_k = ToStrView(begin_key);
    const auto &end_k = ToStrView(begin_key + scan_range);

    // scan target tuples
    std::vector<std::tuple<std::string, Payload *, size_t>> tuples{};
    tuples.reserve(scan_range);
    ::yakushima::scan(table_name_, begin_k, kInclusive, end_k, kInclusive, tuples);

    // summarize scan results
    size_t sum{0};
    for (const auto &tuple : tuples) {
      sum += std::get<1>(tuple)->GetValue();
    }
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)
  {
    // put a key/value pair
    auto *value_v = const_cast<Payload *>(&value);
    const auto rc = ::yakushima::put(token_, table_name_, ToStrView(key), value_v);

    return (rc == status::OK) ? 0 : 1;
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)
  {
    // ues a write API for inserting
    return Write(key, value);
  }

  auto
  Update(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)
  {
    // ues a write API for updating
    return Write(key, value);
  }

  auto
  Delete([[maybe_unused]] const Key &key)
  {
    // delete a tuple by a given key
    const auto rc = ::yakushima::remove(token_, table_name_, ToStrView(key));

    return (rc == status::OK) ? 0 : 1;
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr auto kInclusive = ::yakushima::scan_endpoint::INCLUSIVE;

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  template <class T>
  static constexpr auto
  ToStrView(const T &data)  //
      -> std::string_view
  {
    return std::string_view{reinterpret_cast<const char *>(&data), sizeof(T)};
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a table name for identifying a unique storage
  inline static const std::string table_name_{"index_bench"};

  /// a token for each thread
  Token token_{};
};

#endif  // INDEX_BENCHMARK_INDEXES_YAKUSHIMA_WRAPPER_HPP
