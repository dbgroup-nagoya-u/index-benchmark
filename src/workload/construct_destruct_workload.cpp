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

// the corresponding header
#include "workload/construct_destruct_workload.hpp"

// C++ standard libraries
#include <cstddef>
#include <memory>
#include <random>
#include <tuple>
#include <utility>
#include <vector>

// external libraries
#include <yaml-cpp/yaml.h>

// local sources
#include "common.hpp"
#include "workload/key_space.hpp"

namespace dbgroup::index_bench
{
namespace
{
/*############################################################################*
 * Thread local variables
 *############################################################################*/
// NOLINTBEGIN

thread_local size_t tls_id{};

// NOLINTEND
}  // namespace

/*############################################################################*
 * Constructors
 *############################################################################*/

template <class Key>
ConstructDestructWorkload<Key>::ConstructDestructWorkload(  //
    const YAML::Node& workload,
    const bool is_construct,
    const size_t worker_num,
    std::unique_ptr<KeySpace> keys)
    : rec_num_{keys->Size()}
    , partitioned_{workload["partitioned"].as<bool>()}
    , reversed_{workload["reversed"].as<bool>()}
    , diff_{(partitioned_ ? 1 : static_cast<int>(worker_num)) * (reversed_ ? -1 : 1)}
    , op_type_{is_construct ? kInsertRelevant : kDelete}
    , keys_{std::move(keys)}
{
  exec_nums_.reserve(worker_num);
  for (size_t thread_id = 0; thread_id < worker_num; ++thread_id) {
    exec_nums_.emplace_back((rec_num_ + thread_id) / worker_num);
  }

  if (!is_construct) {
    const auto& init_param = workload["initialization"];
    init_.use_all_cores = init_param["use_all_cores"].as<bool>();
    init_.use_bulkload = init_param["use_bulkload"].as<bool>();
  }
}

/*############################################################################*
 * Public operators
 *############################################################################*/

template <class Key>
ConstructDestructWorkload<Key>::operator bool() const
{
  thread_local size_t cnt{};
  return cnt++ < exec_nums_[tls_id];
}

/*############################################################################*
 * Public APIs
 *############################################################################*/

template <class Key>
auto
ConstructDestructWorkload<Key>::GetType(  //
    const size_t thread_id,
    [[maybe_unused]] std::mt19937_64& rand_eng) const  //
    -> OPType
{
  tls_id = thread_id;
  return op_type_;
}

template <class Key>
auto
ConstructDestructWorkload<Key>::GetOps(                //
    [[maybe_unused]] std::mt19937_64& rand_eng) const  //
    -> std::tuple<Key, size_t, Payload, size_t>
{
  thread_local size_t id = GetBeginPosition();
  const auto key_id = keys_->GetMappedPos(id);
  const auto& [key, key_len] = keys_->GetKey(key_id);
  id += diff_;
  return {key, key_len, key_id, 0};
}

template <class Key>
auto
ConstructDestructWorkload<Key>::CreateInitData() const  //
    -> std::tuple<size_t, bool, std::vector<std::tuple<Key, Payload, size_t>>>
{
  std::vector<std::tuple<Key, Payload, size_t>> entries{};
  if (op_type_ == kDelete) {
    entries.reserve(rec_num_);
    for (size_t id = 0; id < rec_num_; ++id) {
      const auto& [key, key_len] = keys_->GetKey(id);
      entries.emplace_back(key, id, key_len);
    }
  }

  const auto worker_num = init_.use_all_cores ? kMaxCoreNum : 1;
  return {worker_num, init_.use_bulkload, entries};
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

template <class Key>
auto
ConstructDestructWorkload<Key>::GetBeginPosition() const noexcept  //
    -> size_t
{
  size_t pos{};
  if (partitioned_) {
    for (size_t i = 0; i < tls_id; ++i) {
      pos += exec_nums_[i];
    }
    if (reversed_) {
      pos += exec_nums_[tls_id] - 1;
    }
  } else {
    pos = tls_id;
    if (reversed_) {
      const auto worker_num = exec_nums_.size();
      const size_t n = (rec_num_ / worker_num) * worker_num;
      const size_t spill_num = rec_num_ - n;
      const size_t reversed_id = worker_num - (tls_id + 1);
      pos += n - worker_num + (reversed_id < spill_num ? spill_num : 0);
    }
  }
  return pos;
}

/*############################################################################*
 * Explicit instantiation definitions
 *############################################################################*/

template class ConstructDestructWorkload<UIntKey>;
template class ConstructDestructWorkload<StrKey>;

}  // namespace dbgroup::index_bench
