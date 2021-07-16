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
#include <random>
#include <utility>
#include <vector>

#include "../external/open_bwtree/src/bwtree.h"  // NOLINT
#include "common.hpp"

/// always disable print
bool wangziqi2013::bwtree::print_flag = false;

thread_local int wangziqi2013::bwtree::BwTreeBase::gc_id = -1;

std::atomic<size_t> wangziqi2013::bwtree::BwTreeBase::total_thread_num{0};

class WorkerKeyComparator
{
 public:
  constexpr WorkerKeyComparator() {}

  constexpr bool
  operator()(const Key k1, const Key k2) const
  {
    return k1 < k2;
  }
};

class WorkerKeyEqualityChecker
{
 public:
  constexpr WorkerKeyEqualityChecker() {}

  constexpr bool
  operator()(const Key k1, const Key k2) const
  {
    return k1 == k2;
  }
};

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

  ~OpenBwTreeWrapper() { ReserveThreads(0); }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  constexpr void
  ReserveThreads(const size_t total_thread_num)
  {
    bwtree_.UpdateThreadLocal(total_thread_num);
  }

  constexpr void
  RegisterThread(const size_t thread_id)
  {
    bwtree_.AssignGCID(static_cast<int>(thread_id));
  }

  constexpr void
  UnregisterThread(const size_t thread_id)
  {
    bwtree_.UnregisterThread(static_cast<int>(thread_id));
  }

  /*################################################################################################
   * Public read/write APIs
   *##############################################################################################*/

  constexpr void
  Read(const Key key)
  {
    bwtree_.GetValue(key);
  }

  constexpr void
  Scan(  //
      const Key begin_key,
      const Key end_key)
  {
    auto tree_iterator = new ForwardIterator{bwtree_, begin_key};

    while (tree_iterator->IsEnd() == false) {
      if ((*tree_iterator)->first > end_key) break;
      ++(*tree_iterator);
    }

    delete tree_iterator;
  }

  constexpr void
  Write(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
    // a write (upsert) operation is not implemented in Open-Bw-tree
  }

  constexpr void
  Insert(  //
      const Key key,
      const Value value)
  {
    bwtree_.Insert(key, value);
  }

  constexpr void
  Update(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
    // an update operation is not implemented in Open-Bw-tree
  }

  constexpr void
  Delete(const Key key)
  {
    // a delete operation in Open-Bw-tree requrires a key-value pair
    bwtree_.Delete(key, key);
  }
};
