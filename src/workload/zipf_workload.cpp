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
#include "workload/zipf_workload.hpp"

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
#include "workload/var_len_data.hpp"

namespace dbgroup::index_bench
{
namespace
{
/*############################################################################*
 * Thread local variables
 *############################################################################*/
// NOLINTBEGIN

thread_local size_t _id{};

// NOLINTEND
}  // namespace

/*############################################################################*
 * Constructors
 *############################################################################*/

template <class Key>
ZipfWorkload<Key>::ZipfWorkload(  //
    const YAML::Node &workload,
    const size_t worker_num,
    KeySpace keys)
    : keys_{std::move(keys)}
{
  const auto &operations = workload["operations"];
  phases_.reserve(operations.size());
  for (const auto &node : operations) {
    phases_.emplace_back(                                                       //
        std::chrono::seconds{node["duration"].as<size_t>()},                    //
        OPSelector{node["ratios"], worker_num, node["per_thread"].as<bool>()},  //
        node["scan_size"].IsNull() ? 0 : node["scan_size"].as<size_t>(),        //
        Zipf{0, keys_.Size() - 1, node["skew_parameter"].as<double>()});
  }

  const auto &init_param = workload["initialization"];
  init_.key_num = static_cast<size_t>(init_param["num"].as<double>());
  init_.use_all_cores = init_param["use_all_cores"].as<bool>();
  init_.use_bulkload = init_param["use_bulkload"].as<bool>();
}

/*############################################################################*
 * Public operators
 *############################################################################*/

template <class Key>
ZipfWorkload<Key>::operator bool() const
{
  using Clock = std::chrono::high_resolution_clock;

  const auto now = Clock::now();
  thread_local Clock::time_point end_time{now + phases_[_id].duration};
  for (; now > end_time; end_time += phases_[_id].duration) {
    if (++_id >= phases_.size()) return false;
  }
  return true;
}

/*############################################################################*
 * Public operators
 *############################################################################*/

template <class Key>
auto
ZipfWorkload<Key>::GetType(  //
    const size_t thread_id,
    std::mt19937_64 &rand_eng) const  //
    -> OPType
{
  return phases_[_id].op_selector.Select(thread_id, rand_eng);
}

template <class Key>
auto
ZipfWorkload<Key>::GetOps(            //
    std::mt19937_64 &rand_eng) const  //
    -> std::tuple<const Key &, size_t, size_t>
{
  const auto &phase = phases_[_id];
  const auto &[key, key_len] = keys_.GetKey(phase.zipf(rand_eng));
  return {key, key_len, phase.scan_size};
}

template <class Key>
auto
ZipfWorkload<Key>::CreateInitData() const  //
    -> std::tuple<size_t, bool, std::vector<std::tuple<const Key &, Payload, size_t>>>
{
  std::vector<std::tuple<const Key &, Payload, size_t>> entries{};
  entries.reserve(init_.key_num);
  for (size_t i = 0; i < init_.key_num; ++i) {
    const auto &[key, key_len] = keys_.GetSortedKey(i);
    entries.emplace_back(key, Payload{}, key_len);
  }

  const auto worker_num = init_.use_all_cores ? kMaxCoreNum : 1;
  return {worker_num, init_.use_bulkload, entries};
}

/*############################################################################*
 * Explicit instantiation definitions
 *############################################################################*/

template class ZipfWorkload<Payload>;
template class ZipfWorkload<VarLenData>;

}  // namespace dbgroup::index_bench
