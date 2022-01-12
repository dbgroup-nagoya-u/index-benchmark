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

#ifndef INDEX_BENCHMARK_INDEXES_OPEN_BW_TREE_HPP
#define INDEX_BENCHMARK_INDEXES_OPEN_BW_TREE_HPP

#include <atomic>
#include <optional>
#include <utility>
#include <vector>

#include "../common.hpp"
#include "open_bwtree/BwTree/bwtree.h"

/*######################################################################################
 * Specification for OpenBw-Tree
 *####################################################################################*/

namespace wangziqi2013::bwtree
{
/// disable OpenBw-Tree's debug logs
bool print_flag = false;

/// initialize GC ID for each thread
thread_local int BwTreeBase::gc_id = 0;

/// initialize the counter of the total number of entering threads
std::atomic<size_t> BwTreeBase::total_thread_num = 1;

}  // namespace wangziqi2013::bwtree

/*######################################################################################
 * Class definition
 *####################################################################################*/

template <class Key, class Value>
class OpenBwTreeWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using BwTree_t = wangziqi2013::bwtree::BwTree<Key, Value>;
  using ForwardIterator = typename BwTree_t::ForwardIterator;

 public:
  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  explicit OpenBwTreeWrapper(const size_t worker_num)
  {
    open_bw_.UpdateThreadLocal(worker_num + 1);
  }

  ~OpenBwTreeWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  SetUp()
  {
    open_bw_thread_id_ = open_bw_thread_counter_.fetch_add(1);
    open_bw_.AssignGCID(open_bw_thread_id_);
  }

  void
  TearDown()
  {
    open_bw_.UnregisterThread(open_bw_thread_id_);
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const Key &key)  //
      -> std::optional<Value>
  {
    std::vector<Value> read_results;
    open_bw_.GetValue(key, read_results);

    if (read_results.empty()) return std::nullopt;
    return read_results[0];
  }

  void
  Scan(  //
      const Key &begin_key,
      const Key &scan_range)
  {
    const auto end_key = begin_key + scan_range;
    Value sum = 0;

    ForwardIterator tree_iterator{&open_bw_, begin_key};
    for (; !tree_iterator.IsEnd(); ++tree_iterator) {
      auto &&[key, value] = *tree_iterator;
      if (key > end_key) break;

      sum += value;
    }
  }

  auto
  Write(  //
      const Key &key,
      const Value &value)  //
      -> int64_t
  {
    open_bw_.Upsert(key, value);
    return 0;
  }

  auto
  Insert(  //
      const Key &key,
      const Value &value)  //
      -> int64_t
  {
    return !open_bw_.Insert(key, value);
  }

  auto
  Update(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Value &value)  //
      -> int64_t
  {
    // this operation is not implemented
    assert(false);
    return 1;
  }

  auto
  Delete(const Key &key)  //
      -> int64_t
  {
    // a delete operation in Open-Bw-tree requrires a key-value pair
    return !open_bw_.Delete(key, key);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an atomic counter to count the number of worker threads
  static inline std::atomic_size_t open_bw_thread_counter_ = 1;

  /// a thread id for each worker thread
  static thread_local inline size_t open_bw_thread_id_ = 0;

  BwTree_t open_bw_{};
};

#endif  // INDEX_BENCHMARK_INDEXES_OPEN_BW_TREE_HPP
