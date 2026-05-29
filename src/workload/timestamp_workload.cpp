/*
 * Copyright 2025 Database Group, Nagoya University
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

// the corresponding header
#include "workload/timestamp_workload.hpp"

// C++ standard libraries
#include <chrono>
#include <cstddef>
#include <random>
#include <tuple>
#include <vector>

// external libraries
#include <yaml-cpp/yaml.h>

// local sources
#include "common.hpp"
#include "workload/operation_selector.hpp"

namespace dbgroup::index_bench
{
namespace
{
/*############################################################################*
 * Local constants
 *############################################################################*/

/// @brief The most significant bit.
constexpr UIntKey kMSB = 1UL << 63UL;

}  // namespace

/*############################################################################*
 * Constructors
 *############################################################################*/

TimestampWorkload::TimestampWorkload(  //
    const YAML::Node& workload,
    const size_t worker_num)
    : reverse_{workload["reversed"].as<bool>() ? kMSB : 0}
{
  const auto& node = workload["operations"];
  exec_num_ = static_cast<size_t>(node["num"].as<double>());
  op_selector_ = OPSelector{node["ratios"], worker_num, node["per_thread"].as<bool>()};
  if (const auto& scan_size = node["scan_size"]; !scan_size.IsNull()) {
    scan_size_ = scan_size.as<size_t>();
  }
}

/*############################################################################*
 * Public operators
 *############################################################################*/

TimestampWorkload::operator bool() const
{
  thread_local size_t cnt = 0;
  return cnt++ < exec_num_;
}

/*############################################################################*
 * Public APIs
 *############################################################################*/

auto
TimestampWorkload::GetType(  //
    const size_t thread_id,
    std::mt19937_64& rand_eng) const  //
    -> OPType
{
  return op_selector_.Select(thread_id, rand_eng);
}

auto
TimestampWorkload::GetOps(                             //
    [[maybe_unused]] std::mt19937_64& rand_eng) const  //
    -> std::tuple<Key, size_t, Payload, size_t>
{
  const auto ts = reverse_ ^ static_cast<Key>(Clock::now().time_since_epoch().count());
  return {ts, sizeof(Key), ts, scan_size_};
}

auto
TimestampWorkload::CreateInitData()  //
    -> std::tuple<size_t, bool, std::vector<std::tuple<Key, Payload, size_t>>>
{
  std::vector<std::tuple<Key, Payload, size_t>> entries{};
  return {1, false, entries};
}

}  // namespace dbgroup::index_bench
