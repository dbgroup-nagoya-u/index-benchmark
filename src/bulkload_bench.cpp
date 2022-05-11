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
#include <tbb/parallel_sort.h>
#include <tbb/task_arena.h>
#include <tbb/task_group.h>

#include <algorithm>
#include <random>

#include "benchmark/component/stopwatch.hpp"
#include "cla_validator.hpp"
#include "index.hpp"

/*######################################################################################
 * Command line arguments
 *####################################################################################*/

DEFINE_uint64(num_exec, 10000, "The total number of operations for benchmarking");
DEFINE_validator(num_exec, &ValidateNonZero);
DEFINE_uint64(num_thread, 1, "The number of worker threads");
DEFINE_validator(num_thread, &ValidateNonZero);
DEFINE_uint64(key_size, 8, "The size of target keys (only 8, 16, 32, 64, and 128 can be used)");
DEFINE_bool(use_bulkload, true, "Use bulkload functions if possible");
DEFINE_bool(use_shuffled_entries, false, "Use shuffled key/value entries if true");
DEFINE_string(seed, "", "A random seed to control reproducibility");
DEFINE_validator(seed, &ValidateRandomSeed);
DEFINE_bool(csv, false, "Output benchmark results as CSV format");

/*######################################################################################
 * Utility functions
 *####################################################################################*/

/**
 * @brief Log a message to stdout if the output mode is `text`.
 *
 * @param message an output message
 */
void
Log(const std::string &message)
{
  if (!FLAGS_csv) {
    std::cout << message << std::endl;
  }
}

template <class Implementation>
void
Run(const std::string &target_name)
{
  using Key = typename Implementation::K;
  using Payload = typename Implementation::V;
  using Index_t = Index<Key, Payload, Implementation>;
  using Entry_t = Entry<Key, Payload>;

  Log("*** START " + target_name + " ***");

  const size_t total_exec_num = FLAGS_num_exec;
  const size_t thread_num = FLAGS_num_thread;
  ::dbgroup::benchmark::component::StopWatch timer{};

  // create a container of bulkload entries
  Log("...Prepare bulkload entries for benchmarking.");
  std::vector<Entry_t> entries;

  // shuffle entries if needed
  if (FLAGS_use_shuffled_entries) {
    const auto seed = (FLAGS_seed.empty()) ? std::random_device{}() : std::stoul(FLAGS_seed);
    entries = PrepareBulkLoadEntries<Key, Payload>(total_exec_num, thread_num, seed);
  } else {
    entries = PrepareBulkLoadEntries<Key, Payload>(total_exec_num, thread_num);
  }

  // sorting if needed
  size_t sort_time = 0;
  if (FLAGS_use_bulkload && FLAGS_use_shuffled_entries) {
    Log("...Sorting bulkload entries.");

    // use Intel TBB library for parallel sorting
    ::tbb::task_arena tbb_arena{static_cast<int>(thread_num)};
    ::tbb::task_group tbb_group{};

    timer.Start();
    tbb_arena.execute([&] {         //
      tbb_group.run_and_wait([&] {  //
        ::tbb::parallel_sort(entries.begin(), entries.end());
      });
    });
    timer.Stop();

    sort_time = timer.GetNanoDuration();
  } else {
    Log("...Skip sorting bulkload entries.");
  }

  // run benchmark
  Log("...Construct a target index.");
  Index_t index{thread_num};
  timer.Start();
  index.Construct(entries, thread_num, FLAGS_use_bulkload);
  timer.Stop();

  // output execution time for sorting bulkload entries
  const auto construction_time = timer.GetNanoDuration();
  const auto throughput = total_exec_num / ((sort_time + construction_time) / 1e9);
  if (FLAGS_csv) {
    std::cout << sort_time / 1e6 << ","          //
              << construction_time / 1e6 << ","  //
              << throughput << std::endl;
  } else {
    std::cout << "Sorting time [ms]: " << sort_time / 1e6 << std::endl
              << "Construction time [ms]: " << construction_time / 1e6 << std::endl
              << "Throughput [Ops/s]: " << throughput << std::endl;
  }

  Log("...Finish running.");
  Log("*** FINISH ***\n");
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
    std::cout << "NOTE: benchmark targets are not specified." << std::endl;
    return;
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
  gflags::SetUsageMessage("measures throughput of bulkload for thread-safe index implementations.");
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
      std::cout << "NOTE: the input key size " << FLAGS_key_size << " is invalid." << std::endl;
      break;
  }

  return 0;
}
