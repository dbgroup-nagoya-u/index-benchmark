/*
 * Copyright 2023 Database Group, Nagoya University
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

#ifndef INDEX_BENCHMARK_INDEXES_ALEXOL_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_ALEXOL_WRAPPER_HPP

// C++ standard libraries
#include <algorithm>
#include <atomic>
#include <iostream>
#include <iterator>
#include <optional>
#include <string>
#include <utility>

// external sources
#include "GRE/src/competitor/alexol/src/alex.h"

// local sources
#include "common.hpp"

/*######################################################################################
 * Class definition
 *####################################################################################*/

template <class Key, class Payload>
class AlexolWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Index_t = alexol::
      Alex<Key, Payload, alexol::AlexCompare, std::allocator<std::pair<Key, Payload>>, false>;

 public:
  /*####################################################################################
   * Public type aliases
   *##################################################################################*/

  using K = Key;
  using V = Payload;

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  AlexolWrapper([[maybe_unused]] const size_t worker_num) { index_ = std::make_unique<Index_t>(); }

  ~AlexolWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  SetUp()
  {
  }

  void
  TearDown()
  {
  }

  constexpr auto
  Bulkload(  //
      const std::vector<std::pair<K, Payload>> &entries,
      [[maybe_unused]] const size_t thread_num)  //
      -> bool
  {
    // switch buffer to ignore messages
    auto *tmp_cout_buf = std::cout.rdbuf();
    std::ofstream out_null{"/dev/null"};
    std::cout.rdbuf(out_null.rdbuf());

    // call bulkload API of ALEX
    auto &&non_const_entries = const_cast<std::vector<std::pair<K, Payload>> &>(entries);
    index_->bulk_load(non_const_entries.data(), static_cast<int>(non_const_entries.size()));

    // reset output buffer
    std::cout.rdbuf(tmp_cout_buf);
    return true;
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const K &key)  //
      -> std::optional<Payload>
  {
    Payload value{};
    if (index_->get_payload(key, &value)) return value;
    return std::nullopt;
  }

  auto
  Scan(  //
      [[maybe_unused]] const K &begin_key,
      [[maybe_unused]] const size_t scan_range)  //
      -> size_t
  {
    throw std::runtime_error{"ERROR: the scan operation is not implemented."};
    return 0;
  }

  auto
  FullScan()  //
      -> size_t
  {
    throw std::runtime_error{"ERROR: the full scan operation is not implemented."};
    return 0;
  }

  auto
  Write(  //
      const K &key,
      const Payload &value)  //
      -> int64_t
  {
    if (index_->insert(key, value)) return 0;
    return (index_->update(key, value)) ? 0 : 1;
  }

  auto
  Insert(  //
      const K &key,
      const Payload &value)  //
      -> int64_t
  {
    return (index_->insert(key, value)) ? 0 : 1;
  }

  auto
  Update(  //
      const K &key,
      const Payload &value)  //
      -> int64_t
  {
    return (index_->update(key, value)) ? 0 : 1;
  }

  auto
  Delete(const K &key)  //
      -> int64_t
  {
    return (index_->erase(key) > 0) ? 0 : 1;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::unique_ptr<Index_t> index_{nullptr};
};

#endif  // INDEX_BENCHMARK_INDEXES_ALEXOL_WRAPPER_HPP
