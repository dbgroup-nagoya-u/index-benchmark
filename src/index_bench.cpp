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

#include <gflags/gflags.h>

#include "benchmark/benchmarker.hpp"
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

template <class Implementation>
auto
Run(  //
    const std::string &target_name,
    const bool force_use_bulkload = false)  //
    -> bool
{
  using Key = typename Implementation::K;
  using Payload = typename Implementation::V;
  using Operation_t = Operation<Key, Payload>;
  using OperationEngine_t = OperationEngine<Key, Payload>;
  using Index_t = Index<Key, Payload, Implementation>;
  using Bench_t = ::dbgroup::benchmark::Benchmarker<Index_t, Operation_t, OperationEngine_t>;
  using Json_t = ::nlohmann::json;

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
  Index_t index{FLAGS_num_thread + init_thread};
  const auto &entries = PrepareBulkLoadEntries<Key, Payload>(init_size, init_thread);
  index.Construct(entries, init_thread, use_bulkload);

  // run benchmark
  Bench_t bench{index,       target_name,      ops_engine, FLAGS_num_exec, FLAGS_num_thread,
                random_seed, FLAGS_throughput, FLAGS_csv,  FLAGS_timeout};
  bench.Run();

  return true;
}

template <class Key>
void
ForwardKeyForBench()
{
  // run benchmark for each implementaton
  using Payload = uint64_t;
  auto run_any = false;  // check any indexes are specified as benchmarking targets

  /*----------------------------------------------------------------------------------*
   * Basic B+tree implementations
   *----------------------------------------------------------------------------------*/

  if (FLAGS_b_pml) {
    using BTreePML_t = IndexWrapper<Key, Payload, ::dbgroup::index::b_tree::BTreePMLVarLen>;
    run_any = Run<BTreePML_t>("B+tree based on PML", kUseBulkload);
  }

  if (FLAGS_b_psl) {
    using BTreePSL_t = IndexWrapper<Key, Payload, ::dbgroup::index::b_tree::BTreePSLVarLen>;
    run_any = Run<BTreePSL_t>("B+tree based on PSL", kUseBulkload);
  }

  if (FLAGS_b_oml) {
    using BTreeOML_t = IndexWrapper<Key, Payload, ::dbgroup::index::b_tree::BTreeOMLVarLen>;
    run_any = Run<BTreeOML_t>("B+tree based on OML");
  }

  if (FLAGS_b_osl) {
    using BTreeOSL_t = IndexWrapper<Key, Payload, ::dbgroup::index::b_tree::BTreeOSLVarLen>;
    run_any = Run<BTreeOSL_t>("B+tree based on OSL");
  }

  if (FLAGS_bw) {
    using BwTree_t = IndexWrapper<Key, Payload, ::dbgroup::index::bw_tree::BwTreeVarLen>;
    run_any = Run<BwTree_t>("Bw-tree");
  }

  if (FLAGS_bz_in_place) {
    using BzInPlace_t = IndexWrapper<Key, Payload, ::dbgroup::index::bztree::BzTree>;
    run_any = Run<BzInPlace_t>("BzTree in-place mode");
  }

  if (FLAGS_bz_append) {
    using BzAppend_t = IndexWrapper<Key, int64_t, ::dbgroup::index::bztree::BzTree>;
    run_any = Run<BzAppend_t>("BzTree append mode");
  }

  /*----------------------------------------------------------------------------------*
   * B+tree implementations optimized for fixed-length data
   *----------------------------------------------------------------------------------*/

  if (FLAGS_b_pml_opt) {
    using BTreePMLOpt_t = IndexWrapper<Key, Payload, ::dbgroup::index::b_tree::BTreePMLFixLen>;
    run_any = Run<BTreePMLOpt_t>("Optimized B+tree based on PML");
  }

  if (FLAGS_b_psl_opt) {
    using BTreePSLOpt_t = IndexWrapper<Key, Payload, ::dbgroup::index::b_tree::BTreePSLFixLen>;
    run_any = Run<BTreePSLOpt_t>("Optimized B+tree based on PSL");
  }

  if (FLAGS_b_oml_opt) {
    using BTreeOMLOpt_t = IndexWrapper<Key, Payload, ::dbgroup::index::b_tree::BTreeOMLFixLen>;
    run_any = Run<BTreeOMLOpt_t>("Optimized B+tree based on OML");
  }

  if (FLAGS_b_osl_opt) {
    using BTreeOSLOpt_t = IndexWrapper<Key, Payload, ::dbgroup::index::b_tree::BTreeOSLFixLen>;
    run_any = Run<BTreeOSLOpt_t>("Optimized B+tree based on OSL");
  }

  if (FLAGS_bw_opt) {
    using BwTreeOpt_t = IndexWrapper<Key, Payload, ::dbgroup::index::bw_tree::BwTreeFixLen>;
    run_any = Run<BwTreeOpt_t>("Optimized Bw-tree");
  }

  /*----------------------------------------------------------------------------------*
   * Other thread-safe index implementations
   *----------------------------------------------------------------------------------*/

#ifdef INDEX_BENCH_BUILD_BTREE_OLC
  if (FLAGS_b_olc) {
    using BTreeOLC_t = BTreeOLCWrapper<Key, Payload>;
    run_any = Run<BTreeOLC_t>("B-tree based on OLC");
  }
#endif

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  if (FLAGS_open_bw) {
    using OpenBw_t = OpenBwTreeWrapper<Key, Payload>;
    run_any = Run<OpenBw_t>("OpenBw-Tree");
  }
#endif

#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
  if (FLAGS_yakushima) {
    using Yakushima_t = YakushimaWrapper<Key, Payload>;
    run_any = Run<Yakushima_t>("yakushima");
  }
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE
  if (FLAGS_mass) {
    using Mass_t = MasstreeWrapper<Key, Payload>;
    run_any = Run<Mass_t>("masstree-beta");
  }
#endif

  if (!run_any) {
    std::cout << "NOTE: benchmark targets are not specified." << std::endl;
  }
}

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

  switch (FLAGS_key_size) {
    case k8:
      ForwardKeyForBench<Key<k8>>();
      break;
    case k16:
      ForwardKeyForBench<Key<k16>>();
      break;
    case k32:
      ForwardKeyForBench<Key<k32>>();
      break;
    case k64:
      ForwardKeyForBench<Key<k64>>();
      break;
    case k128:
      ForwardKeyForBench<Key<k128>>();
      break;
    default:
      break;
  }

  return 0;
}
