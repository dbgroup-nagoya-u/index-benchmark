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
  using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value>;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  BzTree_t *bztree_;

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
  Scan([[maybe_unused]] const Key begin_key, [[maybe_unused]] const Key end_key) override
  {
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
      void *bztree,
      ZipfGenerator &zipf_engine,
      const Workload workload,
      const size_t operation_counts,
      const size_t random_seed = 0)
      : Worker{zipf_engine, workload, operation_counts, random_seed},
        bztree_{reinterpret_cast<BzTree_t *>(bztree)}
  {
  }
};
