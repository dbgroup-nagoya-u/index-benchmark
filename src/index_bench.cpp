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

#include <filesystem>
#include <string>

#include "benchmark/benchmarker.hpp"
#include "index.hpp"
#include "indexes/index_wrapper.hpp"
#include "operation_engine.hpp"

/*######################################################################################
 * Target indexes
 *####################################################################################*/

#include "bw_tree/bw_tree.hpp"
#include "bztree/bztree.hpp"

using BwTree_t = IndexWrapper<Key, Value, ::dbgroup::index::bw_tree::BwTree>;
using BzTree_t = IndexWrapper<Key, Value, ::dbgroup::index::bztree::BzTree>;

#ifdef INDEX_BENCH_BUILD_BTREE_OLC
#include "indexes/btree_olc_wrapper.hpp"
using BTreeOLC_t = BTreeOLCWrapper<Key, Value>;
#endif

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
#include "indexes/open_bw_tree_wrapper.hpp"
using OpenBw_t = OpenBwTreeWrapper<Key, Value>;
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE
#include "indexes/masstree_wrapper.hpp"
using Mass_t = MasstreeWrapper<Key, Value>;
#endif

/*######################################################################################
 * CLI validators
 *####################################################################################*/

template <class Number>
static auto
ValidatePositiveVal(const char *flagname, const Number value)  //
    -> bool
{
  if (value >= 0) {
    return true;
  }
  std::cout << "A value must be positive for " << flagname << std::endl;
  return false;
}

template <class Number>
static auto
ValidateNonZero(const char *flagname, const Number value)  //
    -> bool
{
  if (value != 0) {
    return true;
  }
  std::cout << "A value must be not zero for " << flagname << std::endl;
  return false;
}

static auto
ValidateRandomSeed([[maybe_unused]] const char *flagname, const std::string &seed)  //
    -> bool
{
  if (seed.empty()) {
    return true;
  }

  for (size_t i = 0; i < seed.size(); ++i) {
    if (!std::isdigit(seed[i])) {
      std::cout << "A random seed must be unsigned integer type" << std::endl;
      return false;
    }
  }
  return true;
}

/*######################################################################################
 * CLI arguments
 *####################################################################################*/

DEFINE_uint64(num_exec, 10000, "The total number of operations for benchmarking");
DEFINE_validator(num_exec, &ValidateNonZero);
DEFINE_uint64(num_thread, 1, "The number of worker threads");
DEFINE_validator(num_thread, &ValidateNonZero);
DEFINE_uint64(num_init_thread, 1, "The number of worker threads for initialization");
DEFINE_validator(num_init_thread, &ValidateNonZero);
DEFINE_uint64(num_key, 10000, "The total number of keys");
DEFINE_validator(num_key, &ValidateNonZero);
DEFINE_uint64(num_init_insert, 10000, "The number of insert operations for initialization");
DEFINE_double(skew_parameter, 0, "A skew parameter (based on Zipf's law)");
DEFINE_validator(skew_parameter, &ValidatePositiveVal);
DEFINE_string(seed, "", "A random seed to control reproducibility");
DEFINE_validator(seed, &ValidateRandomSeed);
DEFINE_string(workload, "", "The path to a JSON file that contains a target workload");
DEFINE_bool(bw, true, "Use Bw-tree as a benchmark target");
DEFINE_bool(bz, true, "Use BzTree as a benchmark target");
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
DEFINE_bool(b_olc, true, "Use OLC based B-tree as a benchmark target");
#else
DEFINE_bool(b_olc, false, "OLC based B-tree is not built as a benchmark target.");
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
DEFINE_bool(open_bw, true, "Use Open-BwTree as a benchmark target");
#else
DEFINE_bool(open_bw, false, "OpenBw-Tree is not built as a benchmark target.");
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
DEFINE_bool(mass, true, "Use Masstree as a benchmark target");
#else
DEFINE_bool(mass, false, "Massree is not built as a benchmark target. ");
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
DEFINE_bool(p, true, "Use PTree as a benchmark target");
#else
DEFINE_bool(p, false, "PTree is not built as a benchmark target.");
#endif
DEFINE_bool(csv, false, "Output benchmark results as CSV format");
DEFINE_bool(throughput, true, "true: measure throughput, false: measure latency");

/*######################################################################################
 * Utility functions
 *####################################################################################*/

static auto
ValidateWorkload(const std::string &workload)  //
    -> bool
{
  if (workload.empty()) {
    std::cout << "NOTE: A workload file is not specified." << std::endl;
    std::cout << "NOTE: Use a read-only workload." << std::endl << std::endl;
    return false;
  }

  const auto abs_path = std::filesystem::absolute(workload);
  if (!std::filesystem::exists(abs_path)) {
    std::cout << "NOTE: The specified file does not exist." << std::endl;
    std::cout << "NOTE: Use a read-only workload." << std::endl << std::endl;
    return false;
  }

  return true;
}

template <class Implementation>
void
RunBenchmark(  //
    const std::string &target_name,
    const Workload &workload)
{
  using Index_t = Index<Implementation>;
  using Bench_t = ::dbgroup::benchmark::Benchmarker<Index_t, Operation, OperationEngine>;

  // create a target index
  Index_t index{FLAGS_num_thread, FLAGS_num_init_thread, FLAGS_num_init_insert};

  // create an operation engine
  OperationEngine ops_engine{workload, FLAGS_num_key, FLAGS_skew_parameter};
  auto random_seed = (FLAGS_seed.empty()) ? std::random_device{}() : std::stoul(FLAGS_seed);

  // run benchmark
  Bench_t bench{index,       ops_engine,       FLAGS_num_exec, FLAGS_num_thread,
                random_seed, FLAGS_throughput, FLAGS_csv,      target_name};
  bench.Run();
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

  // load a target workload from a JSON file
  std::string workload_json{FLAGS_workload};
  auto &&workload = (ValidateWorkload(workload_json))
                        ? Workload::CreateWorkloadFromJson(FLAGS_workload)
                        : Workload{};

  // run benchmark for each implementaton
  if (FLAGS_bw) RunBenchmark<BwTree_t>("Bw-tree", workload);
  if (FLAGS_bz) RunBenchmark<BzTree_t>("BzTree", workload);
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
  if (FLAGS_b_olc) RunBenchmark<BTreeOLC_t>("B-tree based on OLC", workload);
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  if (FLAGS_open_bw) RunBenchmark<OpenBw_t>("OpenBw-Tree", workload);
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
  if (FLAGS_mass) RunBenchmark<Mass_t>("Masstree", workload);
#endif
  return 0;
}
