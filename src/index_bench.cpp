// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "mwcas_bench.hpp"

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
ValidateTargetNum([[maybe_unused]] const char *flagname, const uint64_t value)
{
  if (value > 0 && value <= kMaxTargetNum) {
    return true;
  }
  std::cout << "The number of MwCAS targets must be between [1, " << kMaxTargetNum << "]"
            << std::endl;
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

DEFINE_uint64(read_ratio, 0, "The ratio of MwCAS read operations [%]");
DEFINE_uint64(num_exec, 10000, "The number of MwCAS operations executed in each thread");
DEFINE_validator(num_exec, &ValidateNonZero);
DEFINE_uint64(num_loop, 1, "The number of loops to measure performance");
DEFINE_validator(num_loop, &ValidateNonZero);
DEFINE_uint64(num_thread, 1, "The number of execution threads");
DEFINE_validator(num_thread, &ValidateNonZero);
DEFINE_uint64(num_field, 10000, "The number of total target fields");
DEFINE_validator(num_field, &ValidateNonZero);
DEFINE_uint64(num_target, 2, "The number of target fields for each MwCAS");
DEFINE_validator(num_target, &ValidateTargetNum);
DEFINE_double(skew_parameter, 0, "A skew parameter (based on Zipf's law)");
DEFINE_validator(skew_parameter, &ValidatePositiveVal);
DEFINE_string(seed, "", "A random seed to control reproducibility");
DEFINE_validator(seed, &ValidateRandomSeed);
DEFINE_bool(ours, true, "Use MwCAS library (DB Group @ Nagoya Univ.) as a benchmark target");
DEFINE_bool(pmwcas, true, "Use PMwCAS library (Microsoft) as a benchmark target");
DEFINE_bool(single, false, "Use Single CAS as a benchmark target");
DEFINE_bool(csv, false, "Output benchmark results as CSV format");
DEFINE_bool(throughput, true, "Measure throughput");
DEFINE_bool(latency, true, "Measure latency");

/*##################################################################################################
 * Main function
 *################################################################################################*/

int
main(int argc, char *argv[])
{
  // parse command line options
  gflags::SetUsageMessage("measures throughput/latency for MwCAS implementations.");
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  output_format_is_text = !FLAGS_csv;
  const auto random_seed = (FLAGS_seed.empty()) ? std::random_device{}() : std::stoul(FLAGS_seed);

  Log("=== Start MwCAS Benchmark ===");
  auto bench = MwCASBench{
      FLAGS_read_ratio, FLAGS_num_exec,       FLAGS_num_loop, FLAGS_num_thread, FLAGS_num_field,
      FLAGS_num_target, FLAGS_skew_parameter, random_seed,    FLAGS_throughput, FLAGS_latency};
  if (FLAGS_ours) {
    Log("** Run our MwCAS...");
    bench.RunMwCASBench(BenchTarget::kOurs);
    Log("** Finish.");
  }
  if (FLAGS_pmwcas) {
    Log("** Run Microsoft's PMwCAS...");
    bench.RunMwCASBench(BenchTarget::kPMwCAS);
    Log("** Finish.");
  }
  if (FLAGS_single) {
    Log("** Run Single CAS...");
    bench.RunMwCASBench(BenchTarget::kSingleCAS);
    Log("** Finish.");
  }
  Log("==== End MwCAS Benchmark ====");

  return 0;
}
