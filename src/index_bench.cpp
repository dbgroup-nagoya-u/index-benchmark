// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "index_bench.hpp"

/*##################################################################################################
 * CLI validators
 *################################################################################################*/

template <class Number>
static bool
ValidatePositiveVal(const char *flagname, const Number value)
{
  if (value >= 0) {
    return true;
  }
  std::cout << "A value must be positive for " << flagname << std::endl;
  return false;
}

template <class Number>
static bool
ValidateNonZero(const char *flagname, const Number value)
{
  if (value != 0) {
    return true;
  }
  std::cout << "A value must be not zero for " << flagname << std::endl;
  return false;
}

static bool
ValidateRandomSeed([[maybe_unused]] const char *flagname, const std::string &seed)
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

/*##################################################################################################
 * CLI arguments
 *################################################################################################*/

DEFINE_uint64(num_exec, 10000, "The total number of operations for benchmarking");
DEFINE_validator(num_exec, &ValidateNonZero);
DEFINE_uint64(num_thread, 1, "The number of worker threads");
DEFINE_validator(num_thread, &ValidateNonZero);
DEFINE_uint64(num_key, 10000, "The total number of keys");
DEFINE_validator(num_key, &ValidateNonZero);
DEFINE_uint64(num_init_insert, 1000, "The number of insert operations for initialization");
DEFINE_validator(num_init_insert, &ValidateNonZero);
DEFINE_double(skew_parameter, 0, "A skew parameter (based on Zipf's law)");
DEFINE_validator(skew_parameter, &ValidatePositiveVal);
DEFINE_string(seed, "", "A random seed to control reproducibility");
DEFINE_validator(seed, &ValidateRandomSeed);
DEFINE_bool(bz, true, "Use BzTree as a benchmark target");
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
DEFINE_bool(open_bw, true, "Use Open-BwTree as a benchmark target");
#else
DEFINE_bool(open_bw,
            false,
            "OpenBw-Tree is not built as a benchmark target. If you want to measure OpenBw-Tree's "
            "performance, set 'INDEX_BENCH_BUILD_OPEN_BWTREE' as a compile option.");
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
DEFINE_bool(p, true, "Use PTree as a benchmark target");
#else
DEFINE_bool(p,
            false,
            "PTree is not built as a benchmark target. If you want to measure PTree's performance, "
            "set 'INDEX_BENCH_BUILD_PTREE' as a compile option.");
#endif
DEFINE_bool(csv, false, "Output benchmark results as CSV format");
DEFINE_bool(throughput, true, "true: measure throughput, false: measure latency");

/*##################################################################################################
 * Main function
 *################################################################################################*/

int
main(int argc, char *argv[])
{
  // parse command line options
  gflags::SetUsageMessage("measures throughput/latency for thread-safe index implementations.");
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  output_format_is_text = !FLAGS_csv;
  const auto random_seed = (FLAGS_seed.empty()) ? std::random_device{}() : std::stoul(FLAGS_seed);

  // temporary workload
  Workload workload{100, 0, 0, 0, 0, 0};

  Log("=== Start Benchmark ===");
  if (FLAGS_bz) {
    auto bench = IndexBench<BzTree_t>{workload,      FLAGS_num_exec,        FLAGS_num_thread,
                                      FLAGS_num_key, FLAGS_num_init_insert, FLAGS_skew_parameter,
                                      random_seed,   FLAGS_throughput};
    Log("** Run BzTree **");
    bench.Run();
    Log("** Finish **");
  }
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  if (FLAGS_open_bw) {
    auto bench =
        IndexBench<OpenBwTree_t>{workload,      FLAGS_num_exec,        FLAGS_num_thread,
                                 FLAGS_num_key, FLAGS_num_init_insert, FLAGS_skew_parameter,
                                 random_seed,   FLAGS_throughput};
    Log("** Run Open-BwTree **");
    bench.Run();
    Log("** Finish **");
  }
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
  if (FLAGS_p) {
    auto bench = IndexBench<PTree_t>{workload,      FLAGS_num_exec,        FLAGS_num_thread,
                                     FLAGS_num_key, FLAGS_num_init_insert, FLAGS_skew_parameter,
                                     random_seed,   FLAGS_throughput};
    Log("** Run PTree **");
    bench.Run();
    Log("** Finish **");
  }
#endif
  Log("==== End Benchmark ====");

  return 0;
}
