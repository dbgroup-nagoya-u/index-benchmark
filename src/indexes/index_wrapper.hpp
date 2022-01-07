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

#include "common.hpp"

template <class Key, class Value, template <class K, class V> class Index>
class IndexWrapper
{
  using Index_t = Index<Key, Value>;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  Index_t index_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  IndexWrapper() : index_{} {}

  ~IndexWrapper() = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  ConstructIndex(  //
      const size_t thread_num,
      const size_t insert_num)
  {
    const size_t insert_num_per_thread = insert_num / thread_num;

    // lambda function to insert key-value pairs in a certain thread
    auto f = [&](const size_t begin, const size_t end) {
      for (size_t i = begin; i < end; ++i) {
        this->Write(i, i);
      }
    };

    // insert initial key-value pairs in multi-threads
    std::vector<std::thread> threads;
    auto begin = 0UL, end = insert_num_per_thread;
    for (size_t i = 0; i < thread_num; ++i) {
      if (i == thread_num - 1) {
        end = insert_num;
      }
      threads.emplace_back(f, begin, end);
      begin = end;
      end += insert_num_per_thread;
    }
    for (auto&& t : threads) t.join();
  }

  /*################################################################################################
   * Public read/write APIs
   *##############################################################################################*/

  auto
  Read(const Key key)
  {
    return index_.Read(key);
  }

  void
  Scan(  //
      const Key begin_key,
      const Key scan_range)
  {
    const auto end_key = begin_key + scan_range;

    Value sum = 0;
    for (auto iter = index_.Scan(&begin_key, true, &end_key, true); iter.HasNext(); ++iter) {
      auto&& [key, value] = *iter;
      sum += value;
    }
  }

  auto
  Write(  //
      const Key key,
      const Value value)
  {
    return index_.Write(key, value);
  }

  auto
  Insert(  //
      const Key key,
      const Value value)
  {
    return index_.Insert(key, value);
  }

  auto
  Update(  //
      const Key key,
      const Value value)
  {
    return index_.Update(key, value);
  }

  auto
  Delete(const Key key)
  {
    return index_.Delete(key);
  }
};
