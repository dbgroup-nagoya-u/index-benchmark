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

// external system libraries
#include <gflags/gflags.h>

// external sources
#include "benchmark/benchmarker.hpp"

// local sources
#include "cla_validator.hpp"
#include "index.hpp"
#include "workload/operation_engine.hpp"

/*######################################################################################
 * Command line arguments
 *####################################################################################*/

DEFINE_uint64(num_exec, 10000000, "The number of executions of each worker");
DEFINE_uint64(num_thread, 1, "The number of worker threads");
DEFINE_uint64(key_size, 8, "The size of target keys (only 8, 16, 32, 64, and 128 can be used)");
DEFINE_uint64(timeout, 10, "Seconds to timeout");
DEFINE_string(seed, "", "A random seed to control reproducibility");
DEFINE_string(workload, "", "The path to a JSON file that contains a target workload");
DEFINE_bool(csv, false, "Output benchmark results as CSV format");
DEFINE_bool(throughput, true, "true: measure throughput, false: measure latency");

DEFINE_validator(num_exec, &ValidateNonZero);
DEFINE_validator(num_thread, &ValidateNonZero);
DEFINE_validator(key_size, &ValidateKeySize);
DEFINE_validator(timeout, &ValidateNonZero);
DEFINE_validator(seed, &ValidateRandomSeed);
DEFINE_validator(workload, &ValidateWorkload);

/*######################################################################################
 * Utility functions
 *####################################################################################*/

namespace dbgroup
{

template <class Key, class Payload, class Index_t>
auto
Run(  //
    const std::string &target_name,
    const bool force_use_bulkload = false)  //
    -> bool
{
  using Operation_t = Operation<Key, Payload>;
  using OperationEngine_t = OperationEngine<Key, Payload>;
  using Bench_t = ::dbgroup::benchmark::Benchmarker<Index_t, Operation_t, OperationEngine_t>;
  using Json_t = ::nlohmann::json;

  constexpr auto kPercentile = "0.01,0.05,0.10,0.20,0.30,0.40,0.50,0.60,0.70,0.80,0.90,0.95,0.99";

  // create an operation engine
  OperationEngine_t ops_engine{FLAGS_num_thread};
  std::ifstream workload_in{FLAGS_workload};
  Json_t parsed_json{};
  workload_in >> parsed_json;
  ops_engine.ParseJson(parsed_json);

  // prepare random seed if needed
  auto random_seed = (FLAGS_seed.empty()) ? std::random_device{}() : std::stoul(FLAGS_seed);

  // create a target index
  auto [init_size, use_all_thread, use_bulkload] = ops_engine.GetInitParameters();
  if (force_use_bulkload) {
    use_bulkload = true;
  }
  const auto init_thread = (use_all_thread) ? kMaxCoreNum : 1;
  const auto &entries = PrepareBulkLoadEntries<Key, Payload>(init_size, init_thread);
  Index_t index{};
  index.Construct(entries, init_thread, use_bulkload);

  // run benchmark
  Bench_t bench{index,       target_name,      ops_engine, FLAGS_num_exec, FLAGS_num_thread,
                random_seed, FLAGS_throughput, FLAGS_csv,  FLAGS_timeout,  kPercentile};
  bench.Run();

  return true;
}

template <class K>
void
RunWithMultipleIndexes()
{
  // run benchmark for each implementaton
  using V = uint64_t;
  auto run_any = false;  // check any indexes are specified as benchmarking targets

  /*----------------------------------------------------------------------------------*
   * Basic B+tree implementations
   *----------------------------------------------------------------------------------*/

  if (FLAGS_b_pml) {
    using BTreePML_t = Index<K, V, ::dbgroup::index::b_tree::BTreePMLVarLen>;
    Run<K, V, BTreePML_t>("B+tree based on PML", kUseBulkload);
    run_any = true;
  }

  if (FLAGS_b_psl) {
    using BTreePSL_t = Index<K, V, ::dbgroup::index::b_tree::BTreePSLVarLen>;
    Run<K, V, BTreePSL_t>("B+tree based on PSL", kUseBulkload);
    run_any = true;
  }

  if (FLAGS_b_oml) {
    using BTreeOML_t = Index<K, V, ::dbgroup::index::b_tree::BTreeOMLVarLen>;
    Run<K, V, BTreeOML_t>("B+tree based on OML");
    run_any = true;
  }

  if (FLAGS_b_osl) {
    using BTreeOSL_t = Index<K, V, ::dbgroup::index::b_tree::BTreeOSLVarLen>;
    Run<K, V, BTreeOSL_t>("B+tree based on OSL");
    run_any = true;
  }

  if (FLAGS_bw) {
    using BwTree_t = Index<K, V, ::dbgroup::index::bw_tree::BwTreeVarLen>;
    Run<K, V, BwTree_t>("Bw-tree");
    run_any = true;
  }

  if (FLAGS_bz) {
    using BzInPlace_t = Index<K, V, ::dbgroup::index::bztree::BzTree>;
    Run<K, V, BzInPlace_t>("BzTree in-place mode");
    run_any = true;
  }

  if (FLAGS_bz_append) {
    using V_FOR_APPEND = int64_t;
    using BzAppend_t = Index<K, V_FOR_APPEND, ::dbgroup::index::bztree::BzTree>;
    Run<K, V_FOR_APPEND, BzAppend_t>("BzTree append mode");
    run_any = true;
  }

  /*----------------------------------------------------------------------------------*
   * B+tree implementations optimized for fixed-length data
   *----------------------------------------------------------------------------------*/

  if (FLAGS_b_pml_opt) {
    using BTreePMLOpt_t = Index<K, V, ::dbgroup::index::b_tree::BTreePMLFixLen>;
    Run<K, V, BTreePMLOpt_t>("Optimized B+tree based on PML", kUseBulkload);
    run_any = true;
  }

  if (FLAGS_b_psl_opt) {
    using BTreePSLOpt_t = Index<K, V, ::dbgroup::index::b_tree::BTreePSLFixLen>;
    Run<K, V, BTreePSLOpt_t>("Optimized B+tree based on PSL", kUseBulkload);
    run_any = true;
  }

  if (FLAGS_b_oml_opt) {
    using BTreeOMLOpt_t = Index<K, V, ::dbgroup::index::b_tree::BTreeOMLFixLen>;
    Run<K, V, BTreeOMLOpt_t>("Optimized B+tree based on OML");
    run_any = true;
  }

  if (FLAGS_b_osl_opt) {
    using BTreeOSLOpt_t = Index<K, V, ::dbgroup::index::b_tree::BTreeOSLFixLen>;
    Run<K, V, BTreeOSLOpt_t>("Optimized B+tree based on OSL");
    run_any = true;
  }

  if (FLAGS_bw_opt) {
    using BwTreeOpt_t = Index<K, V, ::dbgroup::index::bw_tree::BwTreeFixLen>;
    Run<K, V, BwTreeOpt_t>("Optimized Bw-tree");
    run_any = true;
  }

  /*----------------------------------------------------------------------------------*
   * Other thread-safe index implementations
   *----------------------------------------------------------------------------------*/

#ifdef INDEX_BENCH_BUILD_B_TREE_OLC
  if (FLAGS_b_olc) {
    using BTreeOLC_t = Index<K, V, BTreeOLCWrapper>;
    Run<K, V, BTreeOLC_t>("B-tree based on OLC");
    run_any = true;
  }
#endif

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  if (FLAGS_open_bw) {
    using OpenBw_t = Index<K, V, OpenBwTreeWrapper>;
    Run<K, V, OpenBw_t>("OpenBw-Tree");
    run_any = true;
  }
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE
  if (FLAGS_mass_beta) {
    using Mass_t = Index<K, V, MasstreeWrapper>;
    Run<K, V, Mass_t>("masstree-beta");
    run_any = true;
  }
#endif

#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
  if (FLAGS_yakushima) {
    using Yakushima_t = Index<K, V, YakushimaWrapper>;
    Run<K, V, Yakushima_t>("yakushima");
    run_any = true;
  }
#endif

#ifdef INDEX_BENCH_BUILD_ART_OLC
  if (FLAGS_art_olc) {
    using ArtOLC_t = Index<K, V, ArtOLCWrapper>;
    Run<K, V, ArtOLC_t>("ART based on OLC");
    run_any = true;
  }
#endif

#ifdef INDEX_BENCH_BUILD_HYDRALIST
  if (FLAGS_hydralist) {
    using HydraList_t = Index<K, V, HydraListWrapper>;
    Run<K, V, HydraList_t>("HydraList");
    run_any = true;
  }
#endif

#ifdef INDEX_BENCH_BUILD_ALEX_OLC
  if (FLAGS_alex_olc) {
    using AlexOLC_t = Index<K, V, AlexOLCWrapper>;
    Run<K, V, AlexOLC_t>("ALEX based on OLC");
    run_any = true;
  }
#endif

  if (!run_any) {
    std::cout << "NOTE: benchmark targets are not specified." << std::endl;
  }
}

void
RunWithSelectedKey()
{
  if constexpr (kUseIntegerKeys) {
    RunWithMultipleIndexes<uint64_t>();
  } else if constexpr (!kBuildLongKeys) {
    RunWithMultipleIndexes<Key<k8>>();
  } else {
    switch (FLAGS_key_size) {
      case k8:
        RunWithMultipleIndexes<Key<k8>>();
        break;
      case k16:
        RunWithMultipleIndexes<Key<k16>>();
        break;
      case k32:
        RunWithMultipleIndexes<Key<k32>>();
        break;
      case k64:
        RunWithMultipleIndexes<Key<k64>>();
        break;
      case k128:
        RunWithMultipleIndexes<Key<k128>>();
        break;
      default:
        break;
    }
  }
}

}  // namespace dbgroup

/*######################################################################################
 * Main function
 *####################################################################################*/

auto
main(int argc, char *argv[])  //
    -> int
{
  // parse command line options
  gflags::SetUsageMessage("measures throughput/latency for thread-safe index implementations.");
  gflags::ParseCommandLineFlags(&argc, &argv, false);

  dbgroup::RunWithSelectedKey();

  return 0;
}
