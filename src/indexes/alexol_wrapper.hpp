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

#ifndef INDEX_BENCHMARK_INDEXES_ALEXOL_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_ALEXOL_WRAPPER_HPP

#include <algorithm>
#include <atomic>
#include <iterator>
#include <optional>
#include <string>
#include <utility>

#include "GRE/src/competitor/alexol/alex.h"
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

  using Index_t = alexolInterface<uint64_t, Payload>;

 public:
  /*####################################################################################
   * Public type aliases
   *##################################################################################*/

  using K = Key;
  using V = Payload;

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  AlexolWrapper([[maybe_unused]] const size_t worker_num) {}

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
      [[maybe_unused]] const std::vector<std::pair<Key, Payload>> &entries,
      [[maybe_unused]] const size_t thread_num)  //
      -> bool
  {
    return true;
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    return std::nullopt;
  }

  void
  Scan(  //
      [[maybe_unused]] const Key &begin_key,
      [[maybe_unused]] const size_t scan_range)
  {
    throw std::runtime_error{"ERROR: the scan operation is not implemented."};
  }

  void
  FullScan()
  {
    throw std::runtime_error{"ERROR: the scan operation is not implemented."};
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)  //
      -> int64_t
  {
    throw std::runtime_error{"ERROR: the write operation is not implemented."};
    return 0;
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)  //
      -> int64_t
  {
    throw std::runtime_error{"ERROR: the insert operation is not implemented."};
    return 1;
  }

  auto
  Update(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)  //
      -> int64_t
  {
    throw std::runtime_error{"ERROR: the update operation is not implemented."};
    return 1;
  }

  auto
  Delete(const Key &key)  //
      -> int64_t
  {
    return 0;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/
  std::unique_ptr<Index_t> index_{nullptr};
};

#endif  // INDEX_BENCHMARK_INDEXES_ALEXOL_WRAPPER_HPP
