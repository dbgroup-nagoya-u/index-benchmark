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

#ifndef INDEX_BENCHMARK_INDEX_HPP
#define INDEX_BENCHMARK_INDEX_HPP

#include <memory>
#include <thread>
#include <vector>

#include "common.hpp"
#include "operation.hpp"

/**
 * @brief A class for dealing with target indexes.
 *
 * @tparam Implementation A certain implementation of thread-safe indexes.
 */
template <class Implementation>
class Index
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  Index(  //
      const size_t init_thread_num,
      const size_t init_insert_num)
  {
    index_.ConstructIndex(init_thread_num, init_insert_num);
  }

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Index() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  void
  Execute(const Operation &ops)
  {
    switch (ops.type) {
      case kScan:
        index_.Scan(ops.key, ops.value);
        break;
      case kWrite:
        index_.Write(ops.key, ops.value);
        break;
      case kInsert:
        index_.Insert(ops.key, ops.value);
        break;
      case kUpdate:
        index_.Update(ops.key, ops.value);
        break;
      case kDelete:
        index_.Delete(ops.key);
        break;
      case kRead:
      default:
        index_.Read(ops.key);
        break;
    }
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an actual target implementation
  Implementation index_{};
};

#endif  // INDEX_BENCHMARK_INDEX_HPP
