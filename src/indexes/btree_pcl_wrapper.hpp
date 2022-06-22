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

#ifndef INDEX_BENCHMARK_INDEXES_BTREE_PCL_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_BTREE_PCL_WRAPPER_HPP

#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "b_tree/b_tree_pcl.hpp"
#include "common.hpp"

template <class Key, class Payload>
class BTreePCLWrapper
{
 public:
  /*####################################################################################
   * Public type aliases
   *##################################################################################*/

  using K = Key;
  using V = Payload;

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/
  explicit BTreePCLWrapper([[maybe_unused]] const size_t worker_num) {}

  ~BTreePCLWrapper() = default;

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
      [[maybe_unused]] const std::vector<Entry<Key, Payload>> &entries,
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
    return index_.Read(key);
  }

  void
  Scan(  //
      const Key &begin_key,
      const size_t scan_range)
  {
    const auto &begin_k = std::make_pair(begin_key, kClosed);
    const auto &end_k = std::make_pair(begin_key + scan_range, !kClosed);

    size_t sum{0};
    for (auto &&iter = index_.Scan(begin_k, end_k); iter.HasNext(); ++iter) {
      sum += iter.GetPayload();
    }
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)
  {
    return index_.Write(key, value);
  }

  auto
  Insert(  //
      const Key &key,
      const Payload &value)
  {
    return index_.Insert(key, value);
  }

  auto
  Update(  //
      const Key &key,
      const Payload &value)
  {
    return index_.Update(key, value);
  }

  auto
  Delete(const Key &key)
  {
    return index_.Delete(key);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  dbgroup::index::b_tree::BTreePCL<Key, Payload> index_{};
};

#endif  // INDEX_BENCHMARK_INDEXES_BTREE_PCL_WRAPPER_HPP