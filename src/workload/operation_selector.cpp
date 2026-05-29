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
#include "workload/operation_selector.hpp"

// C++ standard libraries
#include <cstddef>
#include <random>

// external libraries
#include "yaml-cpp/yaml.h"

// local sources
#include "common.hpp"

namespace dbgroup::index_bench
{

OPSelector::OPSelector(  //
    const YAML::Node& ratios,
    const size_t worker_num,
    const bool per_thread)
    : worker_num_{static_cast<double>(worker_num)}
    , per_thread_{per_thread}
{
  double ratio = 0;
  cum_dist_.reserve(ratios.size());
  if (const auto& op = ratios["read"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kRead, ratio);
  }
  if (const auto& op = ratios["scan"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kScan, ratio);
  }
  if (const auto& op = ratios["scan_latest"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kScanLatest, ratio);
  }
  if (const auto& op = ratios["write"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kWrite, ratio);
  }
  if (const auto& op = ratios["upsert"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kUpsert, ratio);
  }
  if (const auto& op = ratios["insert"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kInsert, ratio);
  }
  if (const auto& op = ratios["update"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kUpdate, ratio);
  }
  if (const auto& op = ratios["update_or_write"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kUpdateOrWrite, ratio);
  }
  if (const auto& op = ratios["delete"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kDelete, ratio);
  }
  if (const auto& op = ratios["delete_and_insert"]; op) {
    ratio += op.as<double>();
    cum_dist_.emplace_back(kDeleteAndInsert, ratio);
  }
}

auto
OPSelector::Select(  //
    const size_t thread_id,
    std::mt19937_64& rand) const  //
    -> OPType
{
  thread_local std::uniform_real_distribution<double> ratio_dist{0.0, 1.0};

  const auto v = per_thread_ ? static_cast<double>(thread_id) / worker_num_  //
                             : ratio_dist(rand);
  const auto n = cum_dist_.size() - 1;
  auto type = cum_dist_.back().first;
  for (size_t i = 0; i < n; ++i) {
    const auto& pair = cum_dist_[i];
    if (v < pair.second) {
      type = pair.first;
      break;
    }
  }
  return type;
}

}  // namespace dbgroup::index_bench
