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

#ifndef INDEX_BENCHMARK_WORKLOAD_OPERATION_ENGINE_HPP
#define INDEX_BENCHMARK_WORKLOAD_OPERATION_ENGINE_HPP

// C++ standard libraries
#include <atomic>
#include <fstream>
#include <random>
#include <tuple>

// local sources
#include "operation.hpp"
#include "workload.hpp"

namespace dbgroup
{

/**
 * @brief A class to represent index read/write operations.
 *
 */
template <class Key, class Payload>
class OperationEngine
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Json_t = ::nlohmann::json;
  using Operation_t = Operation<Key, Payload>;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  OperationEngine() = default;

  explicit OperationEngine(const size_t worker_num) : worker_num_{worker_num}
  {
    worker_count_.store(0);
  }

  constexpr OperationEngine(const OperationEngine &) = default;
  constexpr OperationEngine(OperationEngine &&) = default;

  constexpr auto operator=(const OperationEngine &) -> OperationEngine & = default;
  constexpr auto operator=(OperationEngine &&) -> OperationEngine & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~OperationEngine() = default;

  /*####################################################################################
   * Public getters
   *##################################################################################*/

  auto
  GetInitParameters() const  //
      -> std::tuple<size_t, bool, bool>
  {
    return {init_key_num_, use_all_cores_for_init_, use_bulkload_if_possible_};
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  void
  ParseJson(const Json_t &json)
  {
    // set initialization settings
    const auto &init_json = json.at("initialization");
    init_key_num_ = init_json.at("# of keys");
    use_all_cores_for_init_ = init_json.value("use all cores", true);
    use_bulkload_if_possible_ = init_json.value("use bulkload if possible", true);

    // set workloads
    workloads_.clear();
    const auto &workloads_json = json.at("workloads");
    double cum_val = 0;
    for (const auto &w_json : workloads_json) {
      workloads_.emplace_back(w_json);
      cum_val += workloads_.back().GetExecutionRatio();
    }

    if (!AlmostEqual(cum_val, 1.0)) {
      throw std::runtime_error{"ERROR: the total execution ratios is not one."};
    }
  }

  auto
  Generate(  //
      const size_t total_num,
      const size_t random_seed)  //
      -> std::vector<Operation_t>
  {
    const auto worker_id = worker_count_.fetch_add(1);
    const auto phase_num = workloads_.size();
    std::mt19937_64 rand_engine{random_seed};

    // generate an operation-queue for benchmarking
    std::vector<Operation_t> operations{};
    operations.reserve(total_num);
    size_t exec_num = 0;
    for (size_t i = 0; i < phase_num; ++i) {
      auto &&phase = workloads_.at(i);
      const auto exec_ratio = phase.GetExecutionRatio();
      const size_t n = (i == phase_num - 1) ? total_num - exec_num : total_num * exec_ratio;

      phase.AddOperations(operations, n, worker_id, worker_num_, rand_engine());

      exec_num += n;
    }

    return operations;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  static inline std::atomic_size_t worker_count_{0};

  size_t init_key_num_{0};

  bool use_all_cores_for_init_{true};

  bool use_bulkload_if_possible_{true};

  size_t worker_num_{1};

  std::vector<Workload> workloads_{Workload{}};
};

}  // namespace dbgroup

#endif  // INDEX_BENCHMARK_WORKLOAD_OPERATION_ENGINE_HPP
