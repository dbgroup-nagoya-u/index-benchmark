// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <random>
#include <utility>
#include <vector>

#include "bztree/bztree.hpp"
#include "common.hpp"
#include "worker.hpp"

class WorkerBzTree : public Worker
{
  using NUBzTree = ::dbgroup::index::bztree::BzTree<Key, Value>;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  NUBzTree *bztree_;

 protected:
  /*################################################################################################
   * Inherited utility functions
   *##############################################################################################*/

  void
  Read(const Key key) override
  {
    bztree_->Read(key);
  }

  void
  Scan(const Key begin_key, const Key end_key) override
  {
    bztree_->Scan(begin_key, true, end_key, false);
  }

  void
  Write(const Key key, const Value value) override
  {
    bztree_->Write(key, value);
  }

  void
  Insert(const Key key, const Value value) override
  {
    bztree_->Insert(key, value);
  }

  void
  Update(const Key key, const Value value) override
  {
    bztree_->Update(key, value);
  }

  void
  Delete(const Key key) override
  {
    bztree_->Delete(key);
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  WorkerBzTree(  //
      NUBzTree *bztree,
      const Workload workload,
      const size_t total_key_num,
      const double skew_parameter,
      const size_t operation_counts,
      const size_t random_seed = 0)
      : Worker{workload, total_key_num, skew_parameter, operation_counts, random_seed},
        bztree_{bztree}
  {
  }
};
