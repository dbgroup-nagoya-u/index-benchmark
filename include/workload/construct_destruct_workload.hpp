/*
 * Copyright 2026 Database Group, Nagoya University
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

#ifndef INDEX_BENCHMARK_WORKLOAD_CONSTRUCT_DESTRUCT_WORKLOAD_HPP_
#define INDEX_BENCHMARK_WORKLOAD_CONSTRUCT_DESTRUCT_WORKLOAD_HPP_

// C++ standard libraries
#include <cstddef>
#include <cstdint>
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

namespace dbgroup::index_bench
{
template <class Key>
class ConstructDestructWorkload
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using KeySpace = dbgroup::index_bench::KeySpace<Key>;

 public:
  /*##########################################################################*
   * Public types
   *##########################################################################*/

  using Key_t = Key;

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  ConstructDestructWorkload() = default;

  ConstructDestructWorkload(  //
      const YAML::Node& workload,
      bool is_construct,
      size_t worker_num,
      std::unique_ptr<KeySpace> keys);

  ConstructDestructWorkload(ConstructDestructWorkload&&) noexcept = default;
  auto operator=(ConstructDestructWorkload&&) noexcept -> ConstructDestructWorkload& = default;

  // disable copying
  ConstructDestructWorkload(const ConstructDestructWorkload&) = delete;
  auto operator=(const ConstructDestructWorkload&) -> ConstructDestructWorkload& = delete;

  /*##########################################################################*
   * Public destructor
   *##########################################################################*/

  ~ConstructDestructWorkload() = default;

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

  struct InitParameter {
    bool use_all_cores{};

    bool use_bulkload{};
  };

  /*##########################################################################*
   * Internal utilities
   *##########################################################################*/

  [[nodiscard]]
  auto GetBeginPosition() const noexcept  //
      -> size_t;

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  size_t rec_num_{};

  bool partitioned_{};

  bool reversed_{};

  int32_t diff_{};

  OPType op_type_{};

  std::vector<size_t> exec_nums_{};

  InitParameter init_{};

  std::unique_ptr<KeySpace> keys_{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WORKLOAD_CONSTRUCT_DESTRUCT_WORKLOAD_HPP_
