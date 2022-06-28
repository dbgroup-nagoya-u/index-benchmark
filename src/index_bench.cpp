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

template <class Implementation>
void
Run(const std::string &target_name)
{
  using Key = typename Implementation::K;
  using Payload = typename Implementation::V;
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
  std::string workload_json{FLAGS_workload};
  auto ops_engine =
      (ValidateWorkload(workload_json)) ? OperationEngine_t{workload_json} : OperationEngine_t{};
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
  using Payload = uint64_t;

  using BwTreeVarLen_t = IndexWrapper<Key, Payload, ::dbgroup::index::bw_tree::BwTreeVarLen>;
  using BwTreeFixLen_t = IndexWrapper<Key, Payload, ::dbgroup::index::bw_tree::BwTreeFixLen>;
  using BzInPlace_t = IndexWrapper<Key, Payload, ::dbgroup::index::bztree::BzTree>;
  using BzAppend_t = IndexWrapper<Key, int64_t, ::dbgroup::index::bztree::BzTree>;
#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
  using Yakushima_t = YakushimaWrapper<Key, Payload>;
#endif
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
  using BTreeOLC_t = BTreeOLCWrapper<Key, Payload>;
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  using OpenBw_t = OpenBwTreeWrapper<Key, Payload>;
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
  using Mass_t = MasstreeWrapper<Key, Payload>;
#endif

  if (!FLAGS_bw && !FLAGS_bw_opt && !FLAGS_bz_in_place && !FLAGS_bz_append && !FLAGS_yakushima
      && !FLAGS_b_olc && !FLAGS_open_bw && !FLAGS_mass && !FLAGS_p) {
    std::cerr << "NOTE: benchmark targets are not specified, so use BzTree." << std::endl;
    FLAGS_bz_append = true;
  }

  // run benchmark for each implementaton
  if (FLAGS_bw) Run<BwTreeVarLen_t>("Bw-tree");
  if (FLAGS_bw_opt) Run<BwTreeFixLen_t>("Optimized Bw-tree");
  if (FLAGS_bz_in_place) Run<BzInPlace_t>("BzTree in-place mode");
  if (FLAGS_bz_append) Run<BzAppend_t>("BzTree append mode");
#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
  if (FLAGS_yakushima) Run<Yakushima_t>("yakushima");
#endif
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
  if (FLAGS_b_olc) Run<BTreeOLC_t>("B-tree based on OLC");
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  if (FLAGS_open_bw) Run<OpenBw_t>("OpenBw-Tree");
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
  if (FLAGS_mass) Run<Mass_t>("Masstree");
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
      std::cout << "WARN: the input key size is invalid." << std::endl;
      break;
  }

  return 0;
}
