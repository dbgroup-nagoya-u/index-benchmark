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

#ifndef INDEX_BENCHMARK_WORKLOAD_OPS_SELECTOR_HPP_
#define INDEX_BENCHMARK_WORKLOAD_OPS_SELECTOR_HPP_

// C++ standard libraries
#include <random>
#include <utility>
#include <vector>

// external libraries
#include "yaml-cpp/yaml.h"

// local sources
#include "common.hpp"

namespace dbgroup::index_bench
{
/**
 * @brief A class to represent index read/write operations.
 *
 */
class OPSelector
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  OPSelector() = default;

  explicit OPSelector(  //
      const YAML::Node &ratios,
      const size_t worker_num,
      const bool per_thread);

  OPSelector(const OPSelector &) = default;
  OPSelector(OPSelector &&) noexcept = default;

  auto operator=(const OPSelector &) -> OPSelector & = default;
  auto operator=(OPSelector &&) noexcept -> OPSelector & = default;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~OPSelector() = default;

  /*##########################################################################*
   * Public getters
   *##########################################################################*/

  [[nodiscard]] auto Select(  //
      const size_t thread_id,
      std::mt19937_64 &rand) const  //
      -> OPType;

 private:
  /*##########################################################################*
   * Internal member variables
   *###########
   ###############################################################*/

  std::vector<std::pair<OPType, double>> cum_dist_{};

  double worker_num_{};

  bool per_thread_{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WORKLOAD_OPS_SELECTOR_HPP_
