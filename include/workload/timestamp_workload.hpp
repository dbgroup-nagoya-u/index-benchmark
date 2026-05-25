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

#ifndef INDEX_BENCHMARK_WORKLOAD_TIMESTAMP_WORKLOAD_HPP_
#define INDEX_BENCHMARK_WORKLOAD_TIMESTAMP_WORKLOAD_HPP_

// C++ standard libraries
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <random>
#include <tuple>
#include <utility>
#include <vector>

// external libraries
#include "dbgroup/random/zipf.hpp"
#include "yaml-cpp/yaml.h"

// local sources
#include "common.hpp"
#include "workload/key_space.hpp"
#include "workload/operation_selector.hpp"

namespace dbgroup::index_bench
{
class TimestampWorkload
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Key = int64_t;
  using Clock = std::chrono::high_resolution_clock;

 public:
  /*##########################################################################*
   * Public types
   *##########################################################################*/

  using Key_t = Key;

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  TimestampWorkload() = default;

  TimestampWorkload(  //
      const YAML::Node &workload,
      const size_t worker_num);

  TimestampWorkload(const TimestampWorkload &) = default;
  TimestampWorkload(TimestampWorkload &&) noexcept = default;

  auto operator=(const TimestampWorkload &) -> TimestampWorkload & = default;
  auto operator=(TimestampWorkload &&) noexcept -> TimestampWorkload & = default;

  /*##########################################################################*
   * Public operators
   *##########################################################################*/

  [[nodiscard]] explicit operator bool() const;

  /*##########################################################################*
   * Public getters/setters
   *##########################################################################*/

  [[nodiscard]] auto GetType(  //
      const size_t thread_id,
      std::mt19937_64 &rand_eng) const  //
      -> OPType;

  [[nodiscard]] auto GetOps(            //
      std::mt19937_64 &rand_eng) const  //
      -> std::tuple<Key, size_t, size_t>;

  [[nodiscard]] auto CreateInitData() const  //
      -> std::tuple<size_t, bool, std::vector<std::tuple<const Key &, Payload, size_t>>>;

 private:
  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  size_t exec_num_{};

  OPSelector op_selector_{};

  int64_t reverse_{};

  size_t scan_size_{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WORKLOAD_TIMESTAMP_WORKLOAD_HPP_
