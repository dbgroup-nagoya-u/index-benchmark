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
#include "operation_engine.hpp"

/*######################################################################################
 * Command line arguments
 *####################################################################################*/

DEFINE_uint64(num_exec, 10000, "The total number of operations for benchmarking");
DEFINE_validator(num_exec, &ValidateNonZero);
DEFINE_uint64(num_key, 10000, "The total number of keys");
DEFINE_validator(num_key, &ValidateNonZero);
DEFINE_uint64(num_thread, 1, "The number of worker threads");
DEFINE_validator(num_thread, &ValidateNonZero);
DEFINE_uint64(num_init_insert, 10000, "The number of insert operations for initialization");
DEFINE_uint64(num_init_thread, 1, "The number of worker threads for initialization");
DEFINE_validator(num_init_thread, &ValidateNonZero);
DEFINE_uint64(key_size, 8, "The size of target keys (only 8, 16, 32, 64, and 128 can be used)");
DEFINE_double(skew_parameter, 0, "A skew parameter (based on Zipf's law)");
DEFINE_validator(skew_parameter, &ValidatePositiveVal);
DEFINE_string(seed, "", "A random seed to control reproducibility");
DEFINE_validator(seed, &ValidateRandomSeed);
DEFINE_string(workload, "", "The path to a JSON file that contains a target workload");
DEFINE_bool(csv, false, "Output benchmark results as CSV format");
DEFINE_bool(throughput, true, "true: measure throughput, false: measure latency");

/*######################################################################################
 * Utility functions
 *####################################################################################*/

template <class Key, class Payload, class Implementation>
void
Run(  //
    const std::string &target_name,
    const Workload &workload)
{
  using Operation_t = Operation<Key, Payload>;
  using OperationEngine_t = OperationEngine<Key, Payload>;
  using Index_t = Index<Key, Payload, Implementation>;
  using Bench_t = ::dbgroup::benchmark::Benchmarker<Index_t, Operation_t, OperationEngine_t>;

  const size_t init_size = FLAGS_num_init_insert;
  const size_t init_thread = FLAGS_num_init_thread;

  // create a target index
  Index_t index{FLAGS_num_thread + init_thread};
  const auto &entries = PrepareBulkLoadEntries<Key, Payload>(init_size, init_thread);
  index.Construct(entries, init_thread, kUseBulkload);

  // create an operation engine
  OperationEngine_t ops_engine{workload, FLAGS_num_key, FLAGS_skew_parameter};
  auto random_seed = (FLAGS_seed.empty()) ? std::random_device{}() : std::stoul(FLAGS_seed);

  // run benchmark
  Bench_t bench{index,       ops_engine,       FLAGS_num_exec, FLAGS_num_thread,
                random_seed, FLAGS_throughput, FLAGS_csv,      target_name};
  bench.Run();
}

template <class Key>
void
ForwardKeyForBench()
{
  using BwTreeVarLen_t = IndexWrapper<Key, InPlaceVal, ::dbgroup::index::bw_tree::BwTreeVarLen>;
  using BwTreeFixLen_t = IndexWrapper<Key, InPlaceVal, ::dbgroup::index::bw_tree::BwTreeFixLen>;
  using BzInPlace_t = IndexWrapper<Key, InPlaceVal, ::dbgroup::index::bztree::BzTree>;
  using BzAppend_t = IndexWrapper<Key, AppendVal, ::dbgroup::index::bztree::BzTree>;
#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
  using Yakushima_t = YakushimaWrapper<Key, InPlaceVal>;
#endif
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
  using BTreeOLC_t = BTreeOLCWrapper<Key, InPlaceVal>;
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  using OpenBw_t = OpenBwTreeWrapper<Key, InPlaceVal>;
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
  using Mass_t = MasstreeWrapper<Key, InPlaceVal>;
#endif

  if (!FLAGS_bw && !FLAGS_bw_opt && !FLAGS_bz_in_place && !FLAGS_bz_append && !FLAGS_yakushima
      && !FLAGS_b_olc && !FLAGS_open_bw && !FLAGS_mass && !FLAGS_p) {
    std::cout << "NOTE: benchmark targets are not specified." << std::endl;
    return;
  }

  // load a target workload from a JSON file
  std::string workload_json{FLAGS_workload};
  auto &&workload = (ValidateWorkload(workload_json))
                        ? Workload::CreateWorkloadFromJson(FLAGS_workload)
                        : Workload{};

  // run benchmark for each implementaton
  if (FLAGS_bw) Run<Key, InPlaceVal, BwTreeVarLen_t>("Bw-tree", workload);
  if (FLAGS_bw_opt) Run<Key, InPlaceVal, BwTreeFixLen_t>("Optimized Bw-tree", workload);
  if (FLAGS_bz_in_place) Run<Key, InPlaceVal, BzInPlace_t>("BzTree in-place mode", workload);
  if (FLAGS_bz_append) Run<Key, AppendVal, BzAppend_t>("BzTree append mode", workload);
#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
  if (FLAGS_yakushima) Run<Key, InPlaceVal, Yakushima_t>("yakushima", workload);
#endif
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
  if (FLAGS_b_olc) Run<Key, InPlaceVal, BTreeOLC_t>("B-tree based on OLC", workload);
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  if (FLAGS_open_bw) Run<Key, InPlaceVal, OpenBw_t>("OpenBw-Tree", workload);
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
  if (FLAGS_mass) Run<Key, InPlaceVal, Mass_t>("Masstree", workload);
#endif
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
      ForwardKeyForBench<Key<8>>();
      break;
    case k16:
      ForwardKeyForBench<Key<16>>();
      break;
    case k32:
      ForwardKeyForBench<Key<32>>();
      break;
    case k64:
      ForwardKeyForBench<Key<64>>();
      break;
    case k128:
      ForwardKeyForBench<Key<128>>();
      break;
    default:
      std::cout << "WARN: the input key size is invalid." << std::endl;
      break;
  }

  return 0;
}
