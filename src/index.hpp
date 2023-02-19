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

// C++ standard libraries
#include <memory>
#include <thread>
#include <vector>

// external system libraries
#include <gflags/gflags.h>

// local sources
#include "common.hpp"
#include "workload/operation.hpp"

/*######################################################################################
 * Target indexes
 *####################################################################################*/

/*------------------------------------------------------------------------------------*
 * B+trees
 *------------------------------------------------------------------------------------*/

#include "b_tree/b_tree.hpp"

DEFINE_bool(b_pml, false, "Use BTreePML with variable-length data as a benchmark target");
DEFINE_bool(b_pml_opt, false, "Use BTreePML with fixed-length data as a benchmark target");
DEFINE_bool(b_psl, false, "Use BTreePSL with variable-length data as a benchmark target");
DEFINE_bool(b_psl_opt, false, "Use BTreePSL with fixed-length data as a benchmark target");
DEFINE_bool(b_oml, false, "Use BTreeOML with variable-length data as a benchmark target");
DEFINE_bool(b_oml_opt, false, "Use BTreeOML with fixed-length data as a benchmark target");
DEFINE_bool(b_osl, false, "Use BTreeOSL with variable-length data as a benchmark target");
DEFINE_bool(b_osl_opt, false, "Use BTreeOSL with fixed-length data as a benchmark target");

#ifdef INDEX_BENCH_BUILD_BTREE_OLC
#include "indexes/btree_olc_wrapper.hpp"
DEFINE_bool(b_olc, false, "Use OLC based B-tree as a benchmark target");
#else
DEFINE_bool(b_olc, false, "OLC based B-tree is not built as a benchmark target.");
#endif

/*------------------------------------------------------------------------------------*
 * Bw-trees
 *------------------------------------------------------------------------------------*/

#include "bw_tree/bw_tree.hpp"

DEFINE_bool(bw, false, "Use Bw-tree with variable-length data as a benchmark target");
DEFINE_bool(bw_opt, false, "Use Bw-tree with fixed-length data as a benchmark target");

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
#include "indexes/open_bw_tree_wrapper.hpp"
DEFINE_bool(open_bw, false, "Use Open-BwTree as a benchmark target");
#else
DEFINE_bool(open_bw, false, "OpenBw-Tree is not built as a benchmark target.");
#endif

/*------------------------------------------------------------------------------------*
 * BzTrees
 *------------------------------------------------------------------------------------*/

#include "bztree/bztree.hpp"

DEFINE_bool(bz, false, "Use BzTree with in-place based update as a benchmark target");
DEFINE_bool(bz_append, false, "Use BzTree with append based update as a benchmark target");

/*------------------------------------------------------------------------------------*
 * Masstrees
 *------------------------------------------------------------------------------------*/

#ifdef INDEX_BENCH_BUILD_MASSTREE
#include "indexes/masstree_wrapper.hpp"
DEFINE_bool(mass_beta, false, "Use Masstree as a benchmark target");
#else
DEFINE_bool(mass_beta, false, "Massree is not built as a benchmark target. ");
#endif

#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
#include "indexes/yakushima_wrapper.hpp"
DEFINE_bool(yakushima, false, "Use yakushima as a benchmark target");
#else
DEFINE_bool(yakushima, false, "yakushima is not built as a benchmark target.");
#endif

/*------------------------------------------------------------------------------------*
 * Adaptive radix tres.
 *------------------------------------------------------------------------------------*/

#ifdef INDEX_BENCH_BUILD_ART_OLC
#include "indexes/art_olc_wrapper.hpp"
DEFINE_bool(art_olc, false, "Use OLC based ART as a benchmark target");
#else
DEFINE_bool(art_olc, false, "OLC based ART is not built as a benchmark target.");
#endif

#ifdef INDEX_BENCH_BUILD_HYDRALIST
#include "indexes/hydralist_wrapper.hpp"
DEFINE_bool(hydralist, false, "Use HydraList as a benchmark target");
#else
DEFINE_bool(hydralist, false, "HydraList is not built as a benchmark target.");
#endif

/*------------------------------------------------------------------------------------*
 * Learned indexes
 *------------------------------------------------------------------------------------*/

#ifdef INDEX_BENCH_BUILD_ALEX_OLC
#include "indexes/alex_olc_wrapper.hpp"
DEFINE_bool(alex_olc, false, "Use OLC based ALEX as a benchmark target");
#else
DEFINE_bool(alex_olc, false, "OLC based Alex is not built as a benchmark target.");
#endif

namespace dbgroup
{

/*######################################################################################
 * Class definition
 *####################################################################################*/

/**
 * @brief A class for dealing with target indexes.
 *
 * @tparam Implementation A certain implementation of thread-safe indexes.
 */
template <class Key, class Payload, template <class K, class V> class Implementation>
class Index
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Index_t = Implementation<Key, Payload>;
  using Operation_t = Operation<Key, Payload>;
  using ConstIter_t = typename std::vector<std::pair<Key, Payload>>::const_iterator;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  Index() { index_ = std::make_unique<Index_t>(kGCInterval, kGCThreadNum); }

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
    if constexpr (HasSetUpTearDown<Implementation>()) {
      index_->SetUp();
    }
  }

  void
  TearDownForWorker()
  {
    if constexpr (HasSetUpTearDown<Implementation>()) {
      index_->TearDown();
    }
  }

  void
  Construct(  //
      const std::vector<std::pair<Key, Payload>> &entries,
      const size_t thread_num,
      const bool use_bulkload)
  {
    // if the target index has a bulkload function, use it
    if (use_bulkload && (index_->Bulkload(entries, thread_num) == kSuccess)) return;

    // otherwise, construct an index with one-by-one writing
    auto f = [&](ConstIter_t iter, const ConstIter_t &end_it) {
      // lambda function to insert key-value pairs in a certain thread
      SetUpForWorker();
      for (; iter != end_it; ++iter) {
        const auto &[key, payload] = *iter;
        index_->Write(key, payload);
      }
      TearDownForWorker();
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

  auto
  Execute(const Operation_t &ops)  //
      -> size_t
  {
    switch (ops.type) {
      case kScan: {
        const auto &begin_k = std::make_tuple(ops.GetKey(), sizeof(Key), kClosed);
        const size_t scan_size = ops.GetPayload();
        size_t sum{0};
        size_t count{0};
        for (auto &&iter = index_->Scan(begin_k); iter && count < scan_size; ++iter, ++count) {
          sum += iter.GetPayload();
        }

        return count;
      }

      case kFullScan: {
        size_t sum{0};
        size_t count{0};
        for (auto &&iter = index_->Scan(); iter; ++iter, ++count) {
          sum += iter.GetPayload();
        }

        return count;
      }

      case kRead:
        index_->Read(ops.GetKey());
        break;

      case kWrite:
        index_->Write(ops.GetKey(), ops.GetPayload());
        break;

      case kInsert:
        index_->Insert(ops.GetKey(), ops.GetPayload());
        break;

      case kUpdate:
        index_->Update(ops.GetKey(), ops.GetPayload());
        break;

      case kDelete:
        index_->Delete(ops.GetKey());
        break;

      case kInsertOrUpdate:
        if (index_->Insert(ops.GetKey(), ops.GetPayload())) {
          index_->Update(ops.GetKey(), ops.GetPayload());
        }
        break;

      case kDeleteAndInsert:
        index_->Delete(ops.GetKey());
        index_->Insert(ops.GetKey(), ops.GetPayload());
        break;

      case kDeleteOrInsert:
        if (index_->Delete(ops.GetKey())) {
          index_->Insert(ops.GetKey(), ops.GetPayload());
        }
        break;

      case kInsertAndDelete:
        index_->Insert(ops.GetKey(), ops.GetPayload());
        index_->Delete(ops.GetKey());
        break;

      default:
        std::string err_msg = "ERROR: an undefined operation is about to be executed.";
        throw std::runtime_error{err_msg};
    }

    return 1;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an actual target implementation
  std::unique_ptr<Index_t> index_{nullptr};
};

}  // namespace dbgroup

#endif  // INDEX_BENCHMARK_INDEX_HPP
