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

#ifndef INDEX_BENCHMARK_WORKLOAD_ZIPF_WORKLOAD_HPP_
#define INDEX_BENCHMARK_WORKLOAD_ZIPF_WORKLOAD_HPP_

// C++ standard libraries
#include <chrono>
#include <cstddef>
#include <memory>
#include <random>
#include <tuple>
#include <vector>

// external libraries
#include <yaml-cpp/yaml.h>

// external C++ libraries
#include <dbgroup/random/zipf.hpp>

// local sources
#include "common.hpp"
#include "workload/key_space.hpp"
#include "workload/operation_selector.hpp"

namespace dbgroup::index_bench
{
template <class Key>
class ZipfWorkload
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Zipf = dbgroup::random::ApproxZipfDistribution<size_t>;
  using KeySpace = dbgroup::index_bench::KeySpace<Key>;

 public:
  /*##########################################################################*
   * Public types
   *##########################################################################*/

  using Key_t = Key;

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  ZipfWorkload() = default;

  ZipfWorkload(  //
      const YAML::Node& workload,
      size_t worker_num,
      size_t seed,
      std::unique_ptr<KeySpace> keys);

  ZipfWorkload(ZipfWorkload&&) noexcept = default;
  auto operator=(ZipfWorkload&&) noexcept -> ZipfWorkload& = default;

  // disable copying
  ZipfWorkload(const ZipfWorkload&) = delete;
  auto operator=(const ZipfWorkload&) -> ZipfWorkload& = delete;

  /*##########################################################################*
   * Public destructor
   *##########################################################################*/

  ~ZipfWorkload() = default;

  /*##########################################################################*
   * Public operators
   *##########################################################################*/

  [[nodiscard]]
  explicit operator bool() const;

  /*##########################################################################*
   * Public operators
   *##########################################################################*/

  [[nodiscard]]
  auto GetType(  //
      size_t thread_id,
      std::mt19937_64& rand_eng) const  //
      -> OPType;

  [[nodiscard]]
  auto GetOps(                          //
      std::mt19937_64& rand_eng) const  //
      -> std::tuple<Key, size_t, Payload, size_t>;

  [[nodiscard]]
  auto CreateInitData() const  //
      -> std::tuple<size_t, bool, std::vector<std::tuple<Key, Payload, size_t>>>;

 private:
  /*##########################################################################*
   * Internal types
   *##########################################################################*/

  struct Phase {
    std::chrono::seconds duration{};

    OPSelector op_selector{};

    size_t scan_size{};

    size_t begin_pos{};

    Zipf zipf{};
  };

  struct InitParameter {
    bool use_all_cores{};

    bool use_bulkload{};
  };

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  std::vector<Phase> phases_{};

  InitParameter init_{};

  std::unique_ptr<KeySpace> keys_{};

  size_t rec_num_{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WORKLOAD_ZIPF_WORKLOAD_HPP_
