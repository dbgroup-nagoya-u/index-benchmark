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

#include <atomic>
#include <random>
#include <thread>
#include <utility>
#include <vector>

// suppress Masstree's assertion
#define precondition(x, ...) \
  {                          \
  }

#include "../external/masstree/masstree.hh"
#include "common.hpp"

template <class Key, class Value>
class MasstreeWrapper
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  void* masstree_{};

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  MasstreeWrapper() {}

  ~MasstreeWrapper() = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  ConstructIndex(  //
      const size_t thread_num,
      const size_t insert_num)
  {
    // const size_t insert_num_per_thread = insert_num / thread_num;

    // // lambda function to insert key-value pairs in a certain thread
    // auto f = [&](BzTree_t* index, const size_t begin, const size_t end) {
    //   for (size_t i = begin; i < end; ++i) {
    //     index->Write(i, i);
    //   }
    // };

    // // insert initial key-value pairs in multi-threads
    // std::vector<std::thread> threads;
    // auto begin = 0UL, end = insert_num_per_thread;
    // for (size_t i = 0; i < thread_num; ++i) {
    //   if (i == thread_num - 1) {
    //     end = insert_num;
    //   }
    //   threads.emplace_back(f, &masstree_, begin, end);
    //   begin = end;
    //   end += insert_num_per_thread;
    // }
    // for (auto&& t : threads) t.join();
  }

  /*################################################################################################
   * Public read/write APIs
   *##############################################################################################*/

  std::pair<int64_t, Value>
  Read([[maybe_unused]] const Key key)
  {
    // return masstree_.Read(key);
    return {0, Value{}};
  }

  void
  Scan(  //
      [[maybe_unused]] const Key begin_key,
      [[maybe_unused]] const Key scan_range)
  {
    // const auto end_key = begin_key + scan_range;
    // Value sum = 0;

    // RecordPage_t scan_results;
    // masstree_.Scan(scan_results, &begin_key, true, &end_key, true);
    // while (!scan_results.empty()) {
    //   for (auto&& [key, value] : scan_results) sum += value;

    //   const auto next_key = scan_results.GetLastKey();
    //   if (next_key == end_key) break;
    //   masstree_.Scan(scan_results, &next_key, false, &end_key, true);
    // }
  }

  int64_t
  Write(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
    // return masstree_.Write(key, value);
    return 0;
  }

  int64_t
  Insert(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
    // return masstree_.Insert(key, value);
    return 0;
  }

  int64_t
  Update(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
    // return masstree_.Update(key, value);
    return 0;
  }

  int64_t
  Delete(  //
      [[maybe_unused]] const Key key)
  {
    // return masstree_.Delete(key);
    return 0;
  }
};
