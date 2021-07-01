// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <random>
#include <utility>
#include <vector>

#include "../external/open_bwtree/src/bwtree.cpp"  // NOLINT
#include "common.hpp"
#include "worker.hpp"

class WorkerKeyComparator
{
 public:
  constexpr bool
  operator()(const Key k1, const Key k2) const
  {
    return k1 < k2;
  }

  explicit constexpr WorkerKeyComparator([[maybe_unused]] int32_t dummy) {}

  WorkerKeyComparator() = delete;
};

class WorkerKeyEqualityChecker
{
 public:
  constexpr bool
  operator()(const Key k1, const Key k2) const
  {
    return k1 == k2;
  }

  explicit constexpr WorkerKeyEqualityChecker([[maybe_unused]] int32_t dummy) {}

  WorkerKeyEqualityChecker() = delete;
};

class WorkerOpenBwTree : public Worker
{
  using BwTree_t =
      wangziqi2013::bwtree::BwTree<Key, Value, WorkerKeyComparator, WorkerKeyEqualityChecker>;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  BwTree_t* bwtree_;

 protected:
  /*################################################################################################
   * Inherited utility functions
   *##############################################################################################*/

  void
  Read(const Key key) override
  {
    bwtree_->GetValue(key);
  }

  void
  Scan(const Key begin_key, const Key end_key) override
  {
    BwTree_t::ForwardIterator* tree_iterator = new BwTree_t::ForwardIterator(bwtree_, begin_key);

    while (tree_iterator->IsEnd() == false) {
      if ((*tree_iterator)->first > end_key) break;
      ++(*tree_iterator);
    }
  }

  void
  Write([[maybe_unused]] const Key key, [[maybe_unused]] const Value value) override
  {
  }

  void
  Insert(const Key key, const Value value) override
  {
    bwtree_->Insert(key, value);
  }

  void
  Update([[maybe_unused]] const Key key, [[maybe_unused]] const Value value) override
  {
  }

  void
  Delete(const Key key) override
  {
    bwtree_->Delete(key, key);
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  WorkerOpenBwTree(  //
      ZipfGenerator& zipf_engine,
      const Workload workload,
      const size_t operation_counts,
      const size_t random_seed = 0)
      : Worker{zipf_engine, workload, operation_counts, random_seed}
  {
    wangziqi2013::bwtree::print_flag = false;
    bwtree_ = new BwTree_t{true, WorkerKeyComparator{1}, WorkerKeyEqualityChecker{1}};
    bwtree_->UpdateThreadLocal(1);
    bwtree_->AssignGCID(0);
  }
};
