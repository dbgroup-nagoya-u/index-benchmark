// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <random>
#include <utility>
#include <vector>

#include "common.hpp"
#include "open_bwtree_utils.hpp"
#include "worker.hpp"

class WorkerKeyComparator
{
 public:
  inline bool
  operator()(const Key k1, const Key k2) const
  {
    return k1 < k2;
  }

  WorkerKeyComparator(int dummy)
  {
    (void)dummy;

    return;
  }
  WorkerKeyComparator() = delete;
};

class WorkerKeyEqualityChecker
{
 public:
  inline bool
  operator()(const Key k1, const Key k2) const
  {
    return k1 == k2;
  }

  WorkerKeyEqualityChecker(int dummy)
  {
    (void)dummy;

    return;
  }

  WorkerKeyEqualityChecker() = delete;
};

using BwTree_ =
    wangziqi2013::bwtree::BwTree<Key, Value, WorkerKeyComparator, WorkerKeyEqualityChecker>;
class WorkerOpenBwTree : public Worker
{
  BwTree_* bwtree_;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

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
    BwTree_::ForwardIterator* tree_iterator = new BwTree_::ForwardIterator(bwtree_, begin_key);

    while (tree_iterator->IsEnd() == false) {
      if ((*tree_iterator)->first > end_key) break;
      ++(*tree_iterator);
    }
  }

  void
  Write(const Key key, const Value value) override
  {
  }

  void
  Insert(const Key key, const Value value) override
  {
    bwtree_->Insert(key, value);
  }

  void
  Update(const Key key, const Value value) override
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
      ZipfGenerator &zipf_engine,
      const Workload workload,
      const size_t operation_counts,
      const size_t random_seed = 0)
      : Worker{zipf_engine, workload, operation_counts, random_seed}
  {
    bwtree_ = new BwTree_{true, WorkerKeyComparator{1}, WorkerKeyEqualityChecker{1}};
    bwtree_->UpdateThreadLocal(1);
    bwtree_->AssignGCID(0);
    wangziqi2013::bwtree::print_flag = false;
  }
};
