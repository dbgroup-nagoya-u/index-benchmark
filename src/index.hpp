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

#ifndef INDEX_BENCHMARK_INDEX_HPP
#define INDEX_BENCHMARK_INDEX_HPP

#include <gflags/gflags.h>

#include <memory>
#include <thread>
#include <vector>

#include "common.hpp"
#include "indexes/index_wrapper.hpp"
#include "workload/operation.hpp"

/*######################################################################################
 * Target indexes and its pre-definitions
 *####################################################################################*/

#include "bw_tree/bw_tree.hpp"
#include "bztree/bztree.hpp"
#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
#include "indexes/yakushima_wrapper.hpp"
#endif
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
#include "indexes/btree_olc_wrapper.hpp"
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
#include "indexes/open_bw_tree_wrapper.hpp"
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
#include "indexes/masstree_wrapper.hpp"
#endif

DEFINE_bool(bw, false, "Use Bw-tree with variable-length data as a benchmark target");
DEFINE_bool(bw_opt, false, "Use Bw-tree with fixed-length data as a benchmark target");
DEFINE_bool(bz_in_place, false, "Use BzTree with in-place based update as a benchmark target");
DEFINE_bool(bz_append, false, "Use BzTree with append based update as a benchmark target");
#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
DEFINE_bool(yakushima, false, "Use yakushima as a benchmark target");
#else
DEFINE_bool(yakushima, false, "yakushima is not built as a benchmark target.");
#endif
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
DEFINE_bool(b_olc, false, "Use OLC based B-tree as a benchmark target");
#else
DEFINE_bool(b_olc, false, "OLC based B-tree is not built as a benchmark target.");
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
DEFINE_bool(open_bw, false, "Use Open-BwTree as a benchmark target");
#else
DEFINE_bool(open_bw, false, "OpenBw-Tree is not built as a benchmark target.");
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
DEFINE_bool(mass, false, "Use Masstree as a benchmark target");
#else
DEFINE_bool(mass, false, "Massree is not built as a benchmark target. ");
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
DEFINE_bool(p, false, "Use PTree as a benchmark target");
#else
DEFINE_bool(p, false, "PTree is not built as a benchmark target.");
#endif

/*######################################################################################
 * Class definition
 *####################################################################################*/

/**
 * @brief A class for dealing with target indexes.
 *
 * @tparam Implementation A certain implementation of thread-safe indexes.
 */
template <class Key, class Payload, class Implementation>
class Index
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Operation_t = Operation<Key, Payload>;
  using Entry_t = Entry<Key, Payload>;
  using ConstIter_t = typename std::vector<Entry_t>::const_iterator;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  Index(const size_t total_thread_num)
  {
    index_ = std::make_unique<Implementation>(total_thread_num);
  }

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Index() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  void
  SetUpForWorker()
  {
    index_->SetUp();
  }

  void
  TearDownForWorker()
  {
    index_->TearDown();
  }

  void
  Construct(  //
      const std::vector<Entry_t> &entries,
      const size_t thread_num,
      const bool use_bulkload)
  {
    // if the target index has a bulkload function, use it
    if (use_bulkload && index_->Bulkload(entries, thread_num)) return;

    // otherwise, construct an index with one-by-one writing
    auto f = [&](ConstIter_t iter, const ConstIter_t &end_it) {
      // lambda function to insert key-value pairs in a certain thread
      index_->SetUp();
      for (; iter != end_it; ++iter) {
        index_->Write(iter->GetKey(), iter->GetPayload());
      }
      index_->TearDown();
    };

    // insert initial key-value pairs in multi-threads
    const auto size = entries.size();
    std::vector<std::thread> threads;
    auto &&begin = entries.cbegin();
    for (size_t i = 0; i < thread_num; ++i) {
      size_t n = (size + i) / thread_num;
      const auto &end = std::next(begin, n);
      threads.emplace_back(f, begin, end);
      begin = end;
    }
    for (auto &&t : threads) {
      t.join();
    }
  }

  void
  Execute(const Operation_t &ops)
  {
    switch (ops.type) {
      case IndexOperation::kScan:
        index_->Scan(ops.GetKey(), ops.GetPayload());
        break;
      case IndexOperation::kWrite:
        index_->Write(ops.GetKey(), ops.GetPayload());
        break;
      case IndexOperation::kInsert:
        index_->Insert(ops.GetKey(), ops.GetPayload());
        break;
      case IndexOperation::kUpdate:
        index_->Update(ops.GetKey(), ops.GetPayload());
        break;
      case IndexOperation::kDelete:
        index_->Delete(ops.GetKey());
        break;
      case IndexOperation::kRead:
      default:
        index_->Read(ops.GetKey());
        break;
    }
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an actual target implementation
  std::unique_ptr<Implementation> index_{nullptr};
};

#endif  // INDEX_BENCHMARK_INDEX_HPP
