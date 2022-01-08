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

#ifndef INDEX_BENCHMARK_OPERATION_ENGINE_HPP
#define INDEX_BENCHMARK_OPERATION_ENGINE_HPP

#include <random>

#include "common.hpp"
#include "operation.hpp"
#include "random/zipf.hpp"
#include "workload.hpp"

/**
 * @brief A class to represent index read/write operations.
 *
 */
class OperationEngine
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using ZipfGenerator = ::dbgroup::random::zipf::ZipfGenerator;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  OperationEngine(  //
      Workload workload,
      const size_t key_num,
      const double skew_parameter)
      : workload_{std::move(workload)}, zipf_engine_{key_num, skew_parameter}
  {
  }

  OperationEngine(const OperationEngine &) = default;
  OperationEngine(OperationEngine &&) = default;

  auto operator=(const OperationEngine &) -> OperationEngine & = default;
  auto operator=(OperationEngine &&) -> OperationEngine & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~OperationEngine() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  auto
  Generate(  //
      const size_t n,
      const size_t random_seed)  //
      -> std::vector<Operation>
  {
    std::mt19937_64 rand_engine{random_seed};

    // generate an operation-queue for benchmarking
    std::vector<Operation> operations;
    operations.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      auto key = zipf_engine_(rand_engine);
      auto value = range_generator_(rand_engine);
      auto rand = percent_generator_(rand_engine);
      operations.emplace_back(GetOperationType(rand), key, value);
    }

    return operations;
  }

 private:
  /*####################################################################################
   * Internal utilities
   *##################################################################################*/

  auto
  GetOperationType(const size_t rand) const  //
      -> IndexOperation
  {
    if (rand < workload_.read_ratio) return IndexOperation::kRead;
    if (rand < workload_.scan_ratio) return IndexOperation::kScan;
    if (rand < workload_.write_ratio) return IndexOperation::kWrite;
    if (rand < workload_.insert_ratio) return IndexOperation::kInsert;
    if (rand < workload_.update_ratio) return IndexOperation::kUpdate;
    return IndexOperation::kDelete;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  Workload workload_{};

  ZipfGenerator zipf_engine_{};

  std::uniform_int_distribution<size_t> percent_generator_{0, 99};

  std::uniform_int_distribution<size_t> range_generator_{50, 150};
};

#endif  // INDEX_BENCHMARK_OPERATION_ENGINE_HPP
