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

#pragma once

#include <random>

#include "common.hpp"
#include "operation.hpp"
#include "random/zipf.hpp"
#include "workload.hpp"

using ::dbgroup::random::zipf::ZipfGenerator;

/**
 * @brief A class to represent index read/write operations.
 *
 */
class OperationGenerator
{
 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  const Workload workload_;

  std::mt19937_64 rand_engine_;

  ZipfGenerator &zipf_engine_;

  std::uniform_int_distribution<size_t> percent_generator_{0, 99};

  std::uniform_int_distribution<size_t> range_generator_{50, 150};

 public:
  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  OperationGenerator(  //
      ZipfGenerator &zipf_engine,
      const Workload workload,
      const size_t random_seed = std::random_device{}())
      : workload_{workload}, rand_engine_{random_seed}, zipf_engine_{zipf_engine}
  {
  }

  /*####################################################################################
   * Public utility operators
   *##################################################################################*/

  Operation
  operator()()
  {
    const auto key = zipf_engine_(rand_engine_);
    auto value = key;

    IndexOperation ops;
    const auto workload_rand = percent_generator_(rand_engine_);
    if (workload_rand < workload_.read_ratio) {
      ops = IndexOperation::kRead;
    } else if (workload_rand < workload_.scan_ratio) {
      value = range_generator_(rand_engine_);
      ops = IndexOperation::kScan;
    } else if (workload_rand < workload_.write_ratio) {
      ops = IndexOperation::kWrite;
    } else if (workload_rand < workload_.insert_ratio) {
      ops = IndexOperation::kInsert;
    } else if (workload_rand < workload_.update_ratio) {
      ops = IndexOperation::kUpdate;
    } else {  // workload_rand < workload_.delete_ratio
      ops = IndexOperation::kDelete;
    }

    return Operation{ops, key, value};
  }
};
