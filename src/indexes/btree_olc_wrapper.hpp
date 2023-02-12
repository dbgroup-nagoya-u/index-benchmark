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

#ifndef INDEX_BENCHMARK_INDEXES_BTREE_OLC_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_BTREE_OLC_WRAPPER_HPP

#include <optional>
#include <utility>

#include "common.hpp"
#include "open_bwtree/BTreeOLC/BTreeOLC.h"

template <class Key, class Payload>
class BTreeOLCWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using BTreeOLC_t = btreeolc::BTree<Key, Payload>;

 public:
  /*####################################################################################
   * Public type aliases
   *##################################################################################*/

  using K = Key;
  using V = Payload;

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  explicit BTreeOLCWrapper([[maybe_unused]] const size_t worker_num) {}

  ~BTreeOLCWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  constexpr void
  SetUp()
  {
  }

  constexpr void
  TearDown()
  {
  }

  constexpr auto
  Bulkload(  //
      [[maybe_unused]] const std::vector<std::pair<Key, Payload>> &entries,
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
    Payload value{};
    if (index_.lookup(key, value)) return value;
    return std::nullopt;
  }

  auto
  Scan(  //
      [[maybe_unused]] const Key &begin_key,
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
    throw std::runtime_error{"ERROR: the scan operation is not implemented."};
    return 0;
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)
  {
    index_.insert(key, value);
    return 0;
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)
  {
    throw std::runtime_error{"ERROR: the insert operation is not implemented."};
    return 1;
  }

  auto
  Update(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)
  {
    throw std::runtime_error{"ERROR: the update operation is not implemented."};
    return 1;
  }

  auto
  Delete([[maybe_unused]] const Key &key)
  {
    throw std::runtime_error{"ERROR: the delete operation is not implemented."};
    return 1;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  BTreeOLC_t index_{};
};

#endif  // INDEX_BENCHMARK_INDEXES_BTREE_OLC_WRAPPER_HPP
