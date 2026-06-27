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

// C++ standard libraries
#include <cstddef>
#include <iostream>
#include <locale>
#include <random>
#include <string>

// external system libraries
#include <gflags/gflags.h>

// external libraries
#include "dbgroup/benchmark/benchmarker.hpp"
#include "dbgroup/benchmark/validator.hpp"

// local sources
#include "common.hpp"
#include "competitors.hpp"
#include "index.hpp"
#include "workload/construct_destruct_workload.hpp"
#include "workload/operation_engine.hpp"
#include "workload/timestamp_workload.hpp"
#include "workload/zipf_workload.hpp"

/*############################################################################*
 * Command line arguments
 *############################################################################*/

DEFINE_uint64(  //
    num_thread,
    1,
    "The number of worker threads");
DEFINE_validator(num_thread, &::dbgroup::benchmark::ValidatePositiveValue);

DEFINE_uint64(  //
    timeout,
    600,
    "Seconds to timeout");
DEFINE_validator(timeout, &::dbgroup::benchmark::ValidatePositiveValue);

DEFINE_string(  //
    seed,
    "",
    "A random seed to control reproducibility");
DEFINE_validator(seed, &::dbgroup::benchmark::ValidateStr2UInt);

DEFINE_bool(  //
    csv,
    false,
    "Output benchmark results as CSV format");

DEFINE_bool(  //
    throughput,
    true,
    "true: measure throughput, false: measure latency");

DEFINE_bool(  //
    mem_usage,
    false,
    "true: output memory usage instead of throughput/latency");

namespace dbgroup
{

namespace index
{
template <>
constexpr auto
IsVarLenData<dbgroup::index_bench::StrKey>() noexcept  //
    -> bool
{
  return true;
}

}  // namespace index

namespace index_bench
{
/*############################################################################*
 * Global variables
 *############################################################################*/
// NOLINTBEGIN

bool _run_any{};

const size_t _seed{FLAGS_seed.empty() ? std::random_device{}() : std::stoul(FLAGS_seed)};

// NOLINTEND
/*############################################################################*
 * Utility functions
 *############################################################################*/

template <class OPEngine>
void
AddOperationEngine(  //
    OPEngine& op_engine)
{
  SetCompetitor([&]<template <class, class, class...> class Competitor>(  //
                    const std::string& target_name) {
    using Index_t = Index<Competitor, OPEngine>;
    using Builder = typename benchmark::Benchmarker<Index_t, OPEngine>::Builder;

    Index_t index{};
    index.Construct(op_engine);
    if (FLAGS_mem_usage) {
      constexpr size_t kDigits = 17;
      const auto& [used, allocated] = index.MemoryUsage();
      if (FLAGS_csv) {
        std::cout << used << "," << allocated << "\n";
      } else {
        std::cout.imbue(std::locale(""));
        std::cout << std::right << target_name << ":\n"
                  << "  used size: " << std::setw(kDigits) << used << "\n"
                  << "  allocated: " << std::setw(kDigits) << allocated << "\n";
      }
      return;
    }

    Builder builder{index, target_name, op_engine};
    builder.SetThreadNum(FLAGS_num_thread);
    builder.SetTargetLatency(                                        //
        {0.01, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999, 0.9999});  // NOLINT
    builder.SetTimeOut(FLAGS_timeout);
    builder.SetRandomSeed(_seed);
    if (FLAGS_csv) {
      builder.OutputAsCSV(FLAGS_throughput);
    }
    auto&& bench = builder.Build();

    bench->Run();
  });

  _run_any = true;
}

void
Run(  //
    const YAML::Node& workload)
{
  const auto worker_num = FLAGS_num_thread;
  const auto& workload_type = workload["workload"].as<std::string>();

  const auto& dataset = workload["dataset"];
  std::unique_ptr<KeySpace<UIntKey>> int_keys{};
  std::unique_ptr<KeySpace<StrKey>> str_keys{};
  if (dataset) {
    const auto key_num = static_cast<size_t>(dataset["num"].as<double>());
    std::optional<size_t> seed{};
    if (!dataset["has_locality"].as<bool>()) {
      seed = _seed;
    }
    const auto& type = dataset["type"].as<std::string>();
    if (type == "integer") {
      int_keys = std::make_unique<KeySpace<UIntKey>>(key_num, seed);
    } else if (type == "string") {
      const auto& src = dataset["src"];
      if (src["type"].as<std::string>() == "simulation") {
        str_keys = std::make_unique<KeySpace<StrKey>>(key_num, seed);
      } else {
        const auto& path = src["path"].as<std::string>();
        str_keys = std::make_unique<KeySpace<StrKey>>(key_num, seed, path);
      }
    }
  }

  if (workload_type == "zipf") {
    const auto& type = dataset["type"].as<std::string>();
    if (type == "integer") {
      using Workload = ZipfWorkload<UIntKey>;
      Workload zipf{workload, worker_num, _seed, std::move(int_keys)};
      OperationEngine<Workload> op_eng{std::move(zipf)};
      AddOperationEngine(op_eng);
    } else if (type == "string") {
      using Workload = ZipfWorkload<StrKey>;
      Workload zipf{workload, worker_num, _seed, std::move(str_keys)};
      OperationEngine<Workload> op_eng{std::move(zipf)};
      AddOperationEngine(op_eng);
    }
  } else if (workload_type == "construct" || workload_type == "destruct") {
    const auto& type = dataset["type"].as<std::string>();
    const auto is_construct = workload_type == "construct";
    if (type == "integer") {
      using Workload = ConstructDestructWorkload<UIntKey>;
      Workload const_dest{workload, is_construct, worker_num, std::move(int_keys)};
      OperationEngine<Workload> op_eng{std::move(const_dest)};
      AddOperationEngine(op_eng);
    } else if (type == "string") {
      using Workload = ConstructDestructWorkload<StrKey>;
      Workload const_dest{workload, is_construct, worker_num, std::move(str_keys)};
      OperationEngine<Workload> op_eng{std::move(const_dest)};
      AddOperationEngine(op_eng);
    }
  } else if (workload_type == "timestamp") {
    TimestampWorkload timestamp{workload, worker_num};
    OperationEngine<TimestampWorkload> op_eng{std::move(timestamp)};
    AddOperationEngine(op_eng);
  }
}

}  // namespace index_bench
}  // namespace dbgroup

/*############################################################################*
 * Main function
 *############################################################################*/

auto
main(  //
    int argc,
    char** argv)  //
    -> int
{
  // parse command line options
  constexpr auto kRemoveFlags = true;
  gflags::SetUsageMessage("Measure throughput/latency of thread-safe indexes.");
  gflags::ParseCommandLineFlags(&argc, &argv, kRemoveFlags);

  dbgroup::index_bench::_run_any = false;
  dbgroup::index_bench::Run(YAML::LoadFile(argv[1]));  // NOLINT
  if (!dbgroup::index_bench::_run_any) {
    std::cout << "NOTE: benchmark targets are not specified.\n";
  }

  return 0;
}
