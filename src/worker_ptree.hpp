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
  using PTree_t = keyed_map<Key>;
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
    // 範囲内のノードを持つ木を作成
    PTree_t extracted_tree = PTree_t::range(*ptree_, begin_key, end_key);

    // 範囲内のノードを抽出（pbbs::sequence型）
    auto extracted_nodes = extracted_tree.entries(extracted_tree);
    extracted_nodes.clear();
  }

  void
  Write(const Key key, const Value value) override
  {
  }

  void
  Insert(const Key key, const Value value) override
  {
    ptree_->insert(std::make_pair(key, value));
  }

  void
  Update(const Key key, const Value value) override
  {
  }

  void
  Delete(const Key key) override
  {
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
