// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <random>
#include <utility>
#include <vector>

#include "ptree_utils.hpp"
#include "common.hpp"
#include "worker.hpp"

class WorkerPTree : public Worker
{
  using PTree_t = pam_map<ptree_entry<Value>>;
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/
  PTree_t *ptree_;

 protected:
  /*################################################################################################
   * Inherited utility functions
   *##############################################################################################*/

  void
  Read(const Key key) override
  {
    ptree_->find(key);
  }

  void
  Scan(const Key begin_key, const Key end_key) override
  {
    PTree_t::entries(PTree_t::range(*ptree_, begin_key, end_key));
  }

  void
  Write(const Key key, const Value value) override
  {
    // ptree_->insert means "upsert"
    ptree_->insert(std::make_pair(key, value));
  }

  void
  Insert(const Key key, const Value value) override
  {
    // ptree_->insert means "upsert"
    ptree_->insert(std::make_pair(key, value));
  }

  void
  Update(const Key key, const Value value) override
  {
    auto f = [&] (std::pair<uint64_t, uint64_t>) {return value;}; // 値を更新する関数定義
    ptree_->update(key, f); // 指定したkeyが存在しない場合は何もしない
  }

  void
  Delete(const Key key) override
  {
    PTree_t::remove(ptree_, key);
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  WorkerPTree(  //
      void *ptree,
      const Workload workload,
      const size_t total_key_num,
      const double skew_parameter,
      const size_t operation_counts,
      const size_t random_seed = 0)
      : Worker{workload, total_key_num, skew_parameter, operation_counts, random_seed},
        ptree_{reinterpret_cast<PTree_t *>(ptree)}
  {
  }
};
