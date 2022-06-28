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

#ifndef INDEX_BENCHMARK_WORKLOAD_WORKLOAD_HPP
#define INDEX_BENCHMARK_WORKLOAD_WORKLOAD_HPP

#include <random>

#include "common.hpp"
#include "nlohmann/json.hpp"
#include "random/zipf.hpp"

/**
 * @brief A class for representing a certain phase in workloads.
 *
 */
class Workload
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Json_t = ::nlohmann::json;
  using ZipfGenerator = ::dbgroup::random::zipf::ZipfGenerator;

 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  Workload() : zipf_engine_{key_num_, skew_parameter_} { ops_cum_dist_.emplace_back(kRead, 1.0); }

  explicit Workload(const Json_t &json)
      : key_num_{json.at("# of keys")},
        access_pattern_{json.at("access pattern").get<AccessPattern>()},
        skew_parameter_{json.at("skew parameter")},
        execution_ratio_{json.at("execution ratio")}
  {
    // check access pattern and create the Zipf's law engine if needed
    if (access_pattern_ == kRandom) {
      zipf_engine_ = ZipfGenerator{key_num_, skew_parameter_};
    } else if (access_pattern_ == kUndefinedAccessPattern) {
      std::string err_msg = "ERROR: an undefined access pattern (";
      err_msg += json.at("access pattern");
      err_msg += ") is given.";
      throw std::runtime_error{err_msg};
    }

    // compute cumulative distribution for operations
    const auto &ops_ratios = json.at("operation ratios");
    ParseOperationsJson(ops_ratios);
    if (ops_ratios.contains("scan") && ops_ratios.at("scan") > 0) {
      scan_length_ = json.at("scan length");
    }
  }

  Workload(const Workload &) = default;
  Workload(Workload &&) = default;

  auto operator=(const Workload &) -> Workload & = default;
  auto operator=(Workload &&) -> Workload & = default;

  /*####################################################################################
   * Public getters/setters
   *##################################################################################*/

  constexpr auto
  GetKeyNum() const  //
      -> size_t
  {
    return key_num_;
  }

  constexpr auto
  GetExecutionRatio() const  //
      -> double
  {
    return execution_ratio_;
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  template <class Operation>
  void
  AddOperations(  //
      std::vector<Operation> &operations,
      const size_t n,
      const size_t random_seed)
  {
    std::uniform_int_distribution<size_t> value_dist{0, 256};
    std::uniform_real_distribution<double> ratio_dist{0.0, 1.0};
    std::mt19937_64 rand_engine{random_seed};

    // generate an operation-queue for benchmarking
    for (size_t i = 0; i < n; ++i) {
      const auto ops = GetOperationType(ratio_dist(rand_engine));
      const auto key = (access_pattern_ == kRandom) ? zipf_engine_(rand_engine) : i % key_num_;
      const auto val = (ops == kScan) ? scan_length_ : value_dist(rand_engine);
      operations.emplace_back(ops, key, val);
    }
  }

 private:
  /*####################################################################################
   * Internal utilities
   *##################################################################################*/

  void
  ParseOperationsJson(const Json_t &ops_ratios)
  {
    double cum_val = 0;
    for (const auto &[key, val] : ops_ratios.items()) {
      // check the given key is a valid operation
      Json_t ops_j = key;  // parse a key string to JSON
      const auto ops = ops_j.get<IndexOperation>();
      if (ops == kUndefinedOperation) {
        std::string err_msg = "ERROR: an undefined operation (";
        err_msg += key;
        err_msg += ") is given.";
        throw std::runtime_error{err_msg};
      }

      // compute a cumulative value for simplicity
      cum_val += val.get<double>();
      ops_cum_dist_.emplace_back(ops, cum_val);
    }

    // check the given workload is valid
    if (!AlmostEqual(cum_val, 1.0)) {
      throw std::runtime_error{"ERROR: the sum of operation ratios is not one."};
    }
  }

  auto
  GetOperationType(const double rand_val) const  //
      -> IndexOperation
  {
    for (const auto [ops, cum_val] : ops_cum_dist_) {
      if (rand_val < cum_val) return ops;
    }
    return ops_cum_dist_.back().first;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::vector<std::pair<IndexOperation, double>> ops_cum_dist_{};

  size_t key_num_{1000000};

  AccessPattern access_pattern_{kRandom};

  double skew_parameter_{0};

  ZipfGenerator zipf_engine_{};

  size_t scan_length_{1000};

  double execution_ratio_{1.0};
};

#endif  // INDEX_BENCHMARK_WORKLOAD_WORKLOAD_HPP
