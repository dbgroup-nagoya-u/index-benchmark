/*
 * Copyright 2025 Database Group, Nagoya University
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

#ifndef INDEX_BENCHMARK_COMPETITORS_HPP_
#define INDEX_BENCHMARK_COMPETITORS_HPP_

// external system libraries
#include <gflags/gflags.h>

/*----------------------------------------------------------------------------*
 * B+tree relevant indexes
 *----------------------------------------------------------------------------*/

#ifdef INDEX_BENCH_BUILD_B_TREE
#include "dbgroup/b_tree/b_tree.hpp"
DEFINE_bool(b_tree, false, "Use dbgroup::b_tree as a competitor");
#endif

#ifdef INDEX_BENCH_BUILD_B_TREE_OPTIQL
#include "wrappers/b_tree_optiql_wrapper.hpp"
DEFINE_bool(b_tree_optiql, false, "Use sfu_dis::b_tree_optiql as a competitor");
#endif

#ifdef INDEX_BENCH_BUILD_BW_TREE
#include "dbgroup/bw_tree/bw_tree.hpp"
DEFINE_bool(bw_tree, false, "Use dbgroup::bw_tree as a competitor");
#endif

/*----------------------------------------------------------------------------*
 * Trie relevant indexes
 *----------------------------------------------------------------------------*/

#ifdef INDEX_BENCH_BUILD_MASSTREE
#include "wrappers/masstree_wrapper.hpp"
DEFINE_bool(masstree, false, "Use dbgroup::masstree as a competitor");
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE_BETA
#include "wrappers/masstree_beta_wrapper.hpp"
DEFINE_bool(masstree_beta, false, "Use kohler::masstree_beta as a competitor");
#endif

#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
#include "wrappers/yakushima_wrapper.hpp"
DEFINE_bool(yakushima, false, "Use tsurugi::yakushima as a competitor");
#endif

#ifdef INDEX_BENCH_BUILD_ART_OLC
#include "wrappers/art_olc_wrapper.hpp"
DEFINE_bool(art_olc, false, "Use microbench::art_olc as a competitor");
#endif

/*----------------------------------------------------------------------------*
 * Skip lists
 *----------------------------------------------------------------------------*/

#ifdef INDEX_BENCH_BUILD_SKIP_LIST
#include "dbgroup/skip_list/skip_list.hpp"
DEFINE_bool(skip_list, false, "Use dbgroup::skip_list as a competitor");
#endif

namespace dbgroup::index_bench
{

template <typename Bench>
void
SetCompetitor(  //
    Bench&& bench)
{
#ifdef INDEX_BENCH_BUILD_B_TREE
  if (FLAGS_b_tree) {
    bench.template operator()<dbgroup::index::b_tree::BTree>("dbgroup::b_tree");
  }
#endif

#ifdef INDEX_BENCH_BUILD_BW_TREE
  if (FLAGS_bw_tree) {
    bench.template operator()<dbgroup::index::bw_tree::BwTree>("dbgroup::bw_tree");
  }
#endif

#ifdef INDEX_BENCH_BUILD_B_TREE_OPTIQL
  if (FLAGS_b_tree_optiql) {
    bench.template operator()<BTreeOptiQLWrapper>("sfu_dis::b_tree_optiql");
  }
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE
  if (FLAGS_masstree) {
    bench.template operator()<MasstreeWrapper>("dbgroup::masstree");
  }
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE_BETA
  if (FLAGS_masstree_beta) {
    bench.template operator()<MasstreeBetaWrapper>("kohler::masstree_beta");
  }
#endif

#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
  if (FLAGS_yakushima) {
    bench.template operator()<YakushimaWrapper>("tsurugi::yakushima");
  }
#endif

#ifdef INDEX_BENCH_BUILD_ART_OLC
  if (FLAGS_art_olc) {
    bench.template operator()<ARTOLCWrapper>("microbench::art_olc");
  }
#endif

#ifdef INDEX_BENCH_BUILD_SKIP_LIST
  if (FLAGS_skip_list) {
    bench.template operator()<dbgroup::index::skip_list::SkipList>("dbgroup::skip_list");
  }
#endif
}

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_COMPETITORS_HPP_
