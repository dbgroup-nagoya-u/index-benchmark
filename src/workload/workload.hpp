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

// C++ standard libraries
#include <random>
#include <variant>

// external sources
#include "nlohmann/json.hpp"
#include "random/zipf.hpp"

// local sources
#include "common.hpp"

namespace dbgroup
{

/**
 * @brief A class for representing a certain phase in workloads.
 *
 */
class Workload
{
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using Json_t = ::nlohmann::json;
  using ExactZipf_t = ::dbgroup::random::ZipfDistribution<uint32_t>;
  using ApproxZipf_t = ::dbgroup::random::ApproxZipfDistribution<uint32_t>;
  using KeyDist = std::variant<ExactZipf_t, ApproxZipf_t, std::uniform_int_distribution<uint32_t>>;

 public:
  /*############################################################################
   * Public constructors and assignment operators
   *##########################################################################*/

  Workload() = default;

  explicit Workload(const Json_t &json)
      : ops_cum_dist_{},
        key_num_{json.at("# of keys")},
        access_pattern_{json.at("access pattern").get<AccessPattern>()},
        partition_{json.at("partitioning policy").get<Partitioning>()},
        execution_ratio_{json.value("execution ratio", 1.0)},
        skew_parameter_{json.value("skew parameter", 0.0)}
  {
    // check access pattern and create the Zipf's law engine if needed
    if (access_pattern_ == kUndefinedAccessPattern) {
      std::string err_msg = "ERROR: an undefined access pattern (";
      err_msg += json.at("access pattern");
      err_msg += ") is given.";
      throw std::runtime_error{err_msg};
    }

    // check partitioning policy
    if (partition_ == kUndefinedPartitioning) {
      std::string err_msg = "ERROR: an undefined partitioning policy (";
      err_msg += json.at("partitioning policy");
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

  /*############################################################################
   * Public getters/setters
   *##########################################################################*/

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

  /*############################################################################
   * Public utilities
   *##########################################################################*/

  template <class Operation>
  void
  AddOperations(  //
      std::vector<Operation> &operations,
      const size_t ops_num,
      const size_t worker_id,
      const size_t worker_num,
      const size_t random_seed)
  {
    std::mt19937_64 rand_engine{random_seed};

    auto key_dist = GetKeyDistribution(worker_id, worker_num);
    std::uniform_int_distribution<size_t> value_dist{0, 256};
    std::uniform_real_distribution<double> ratio_dist{0.0, 1.0};

    // generate an operation-queue for benchmarking
    for (size_t i = 0; i < ops_num; ++i) {
      const auto ops = GetOperationType(ratio_dist(rand_engine));
      const auto key = GetKeyID(key_dist, rand_engine, i, worker_id, worker_num);
      const auto val = (ops == kScan) ? scan_length_ : value_dist(rand_engine);
      operations.emplace_back(ops, key, val);
    }
  }

 private:
  /*############################################################################
   * Internal utilities
   *##########################################################################*/

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
  GetKeyDistribution(  //
      const size_t w_id,
      const size_t w_num) const  //
      -> KeyDist
  {
    const uint32_t key_num = (partition_ == kNone) ? key_num_ : (key_num_ - (w_id + 1)) / w_num + 1;

    if (skew_parameter_ == 0) return std::uniform_int_distribution<uint32_t>{0, key_num - 1};
    return ApproxZipf_t{0, key_num - 1, skew_parameter_};
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

  auto
  GetKeyID(  //
      KeyDist &key_dist,
      std::mt19937_64 &rand_engine,
      const uint32_t i,
      const size_t w_id,
      const size_t w_num) const  //
      -> uint32_t
  {
    const uint32_t key_num = (partition_ == kNone) ? key_num_ : (key_num_ - (w_id + 1)) / w_num + 1;
    uint32_t key_id{};
    if (access_pattern_ == kRandom && partition_ == kNone) {
      key_id = std::visit([&](auto &dist) { return dist(rand_engine); }, key_dist);
    } else if (access_pattern_ == kRandom) {  // the stripe or range partitioning
      thread_local const auto &rand_keys = CreateRandomKeyIDs(key_num, rand_engine);
      key_id = rand_keys.at(i % key_num);
    } else if (access_pattern_ == kAscending) {
      key_id = i % key_num;
    } else {  // access_pattern_ == kDescending
      key_id = key_num - 1 - (i % key_num);
    }

    if (partition_ == kNone) return key_id;
    if (partition_ == kStripe) return key_id * w_num + w_id;

    // partition_ == kRange
    const uint32_t pad = key_num_ % w_num;
    const uint32_t begin_pos = (key_num_ / w_num) * w_id + ((w_id < pad) ? w_id : pad);
    return begin_pos + key_id;
  }

  static auto
  CreateRandomKeyIDs(  //
      const size_t key_num,
      std::mt19937_64 &rand_engine)  //
      -> std::vector<uint32_t>
  {
    std::vector<uint32_t> key_ids{};
    key_ids.reserve(key_num);
    for (size_t i = 0; i < key_num; ++i) {
      key_ids.emplace_back(i);
    }
    std::shuffle(key_ids.begin(), key_ids.end(), rand_engine);

    return key_ids;
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  std::vector<std::pair<IndexOperation, double>> ops_cum_dist_{std::make_pair(kRead, 1.0)};

  size_t key_num_{1000000};

  AccessPattern access_pattern_{kRandom};

  Partitioning partition_{kNone};

  double execution_ratio_{1.0};

  double skew_parameter_{0};

  size_t scan_length_{1000};
};

}  // namespace dbgroup

#endif  // INDEX_BENCHMARK_WORKLOAD_WORKLOAD_HPP
