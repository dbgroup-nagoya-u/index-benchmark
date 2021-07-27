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

#pragma once

#include <atomic>
#include <memory>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "../external/open_bwtree/src/bwtree.h"  // NOLINT
#include "common.hpp"

/// disable OpenBw-Tree's debug logs
bool wangziqi2013::bwtree::print_flag = false;

/// initialize GC ID for each thread
thread_local int wangziqi2013::bwtree::BwTreeBase::gc_id = -1;

/// initialize the counter of the total number of entering threads
std::atomic<size_t> wangziqi2013::bwtree::BwTreeBase::total_thread_num = 0;

template <class Key, class Value>
class OpenBwTreeWrapper
{
  using BwTree_t = wangziqi2013::bwtree::BwTree<Key, Value>;
  using ForwardIterator = typename BwTree_t::ForwardIterator;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/
  BwTree_t bwtree_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  OpenBwTreeWrapper() : bwtree_{} {}

  ~OpenBwTreeWrapper() = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  ConstructIndex(  //
      const size_t thread_num,
      const size_t insert_num)
  {
    const size_t insert_num_per_thread = insert_num / thread_num;

    // lambda function to insert key-value pairs in a certain thread
    auto f = [&](BwTree_t* index, const size_t begin, const size_t end, const size_t thread_id) {
      index->AssignGCID(thread_id);
      for (size_t i = begin; i < end; ++i) {
        index->Upsert(i, i);
      }
      index->UnregisterThread(thread_id);
    };

    // reserve threads for initialization
    ReserveThreads(thread_num);

    // insert initial key-value pairs in multi-threads
    std::vector<std::thread> threads;
    auto begin = 0UL, end = insert_num_per_thread;
    for (size_t i = 0; i < thread_num; ++i) {
      if (i == thread_num - 1) {
        end = insert_num;
      }
      threads.emplace_back(f, &bwtree_, begin, end, i);
      begin = end;
      end += insert_num_per_thread;
    }
    for (auto&& t : threads) t.join();

    // release reserved threads
    ReserveThreads(0);
  }

  void
  ReserveThreads(const size_t total_thread_num)
  {
    bwtree_.UpdateThreadLocal(total_thread_num);
  }

  void
  RegisterThread(const size_t thread_id)
  {
    bwtree_.AssignGCID(thread_id);
  }

  void
  UnregisterThread(const size_t thread_id)
  {
    bwtree_.UnregisterThread(thread_id);
  }

  /*################################################################################################
   * Public read/write APIs
   *##############################################################################################*/

  std::pair<int64_t, Value>
  Read(const Key key)
  {
    std::vector<Value> read_results;
    bwtree_.GetValue(key, read_results);

    if (read_results.empty()) {
      return {1, Value{}};
    }
    return {0, read_results[0]};
  }

  void
  Scan(  //
      const Key begin_key,
      const Key scan_range)
  {
    const auto end_key = begin_key + scan_range;
    Value sum = 0;

    ForwardIterator tree_iterator{&bwtree_, begin_key};
    for (; !tree_iterator.IsEnd(); ++tree_iterator) {
      auto&& [key, value] = *tree_iterator;
      if (key > end_key) break;

      sum += value;
    }
  }

  void
  Write(  //
      const Key key,
      const Value value)
  {
    bwtree_.Upsert(key, value);
  }

  int64_t
  Insert(  //
      const Key key,
      const Value value)
  {
    return !bwtree_.Insert(key, value);
  }

  int64_t
  Update(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
    // an update operation is not implemented in Open-Bw-tree
    assert(false);
    return 1;
  }

  int64_t
  Delete(const Key key)
  {
    // a delete operation in Open-Bw-tree requrires a key-value pair
    return !bwtree_.Delete(key, key);
  }
};
