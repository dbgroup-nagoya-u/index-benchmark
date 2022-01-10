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
      const size_t worker_num,
      const size_t init_thread_num,
      const size_t init_insert_num)
  {
    index_ = std::make_unique<Implementation>(worker_num + init_thread_num);

    // lambda function to insert key-value pairs in a certain thread
    auto f = [&](const size_t begin, const size_t end) {
      index_->SetUp();
      for (size_t i = begin; i < end; ++i) {
        index_->Write(i, i);
      }
      index_->TearDown();
    };

    // insert initial key-value pairs in multi-threads
    std::vector<std::thread> threads;
    size_t begin = 0;
    for (size_t i = 0; i < init_thread_num; ++i) {
      size_t n = (init_insert_num + i) / init_thread_num;
      size_t end = begin + n;
      threads.emplace_back(f, begin, end);
      begin = end;
    }
    for (auto &&t : threads) t.join();
  }

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Index() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  void
  SetUpForWorker()
  {
    index_->SetUp();
  }

  void
  TearDownForWorker()
  {
    index_->TearDown();
  }

  void
  Execute(const Operation &ops)
  {
    switch (ops.type) {
      case kScan:
        index_->Scan(ops.key, ops.value);
        break;
      case kWrite:
        index_->Write(ops.key, ops.value);
        break;
      case kInsert:
        index_->Insert(ops.key, ops.value);
        break;
      case kUpdate:
        index_->Update(ops.key, ops.value);
        break;
      case kDelete:
        index_->Delete(ops.key);
        break;
      case kRead:
      default:
        index_->Read(ops.key);
        break;
    }
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an actual target implementation
  std::unique_ptr<Implementation> index_{nullptr};
};

#endif  // INDEX_BENCHMARK_INDEX_HPP
