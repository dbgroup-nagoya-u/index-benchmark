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
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  const Workload workload_;

  std::mt19937_64 rand_engine_;

  ZipfGenerator &zipf_engine_;

  std::uniform_int_distribution<size_t> percent_generator_{0, 99};

  std::uniform_int_distribution<size_t> range_generator_{50, 150};

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  OperationGenerator(  //
      ZipfGenerator &zipf_engine,
      const Workload workload,
      const size_t random_seed = std::random_device{}())
      : workload_{workload}, rand_engine_{random_seed}, zipf_engine_{zipf_engine}
  {
  }

  /*################################################################################################
   * Public utility operators
   *##############################################################################################*/

  Operation
  operator()()
  {
    const auto key = zipf_engine_(rand_engine_);

    const auto rand_val = percent_generator_(rand_engine_);
    if (rand_val < workload_.read_ratio) {
      return Operation{IndexOperation::kRead, key};
    } else if (rand_val < workload_.scan_ratio) {
      const auto value = rand_engine_();
      const auto end_key = range_generator_(rand_engine_);
      return Operation{IndexOperation::kScan, key, value, end_key};
    } else if (rand_val < workload_.write_ratio) {
      const auto value = rand_engine_();
      return Operation{IndexOperation::kWrite, key, value};
    } else if (rand_val < workload_.insert_ratio) {
      const auto value = rand_engine_();
      return Operation{IndexOperation::kInsert, key, value};
    } else if (rand_val < workload_.update_ratio) {
      const auto value = rand_engine_();
      return Operation{IndexOperation::kUpdate, key, value};
    } else {  // rand_val < workload_.delete_ratio
      return Operation{IndexOperation::kDelete, key};
    }
  }
};
