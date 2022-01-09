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

#include "../common.hpp"

template <class Key, class Value, template <class K, class V> class Index>
class IndexWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Index_t = Index<Key, Value>;

 public:
  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  IndexWrapper() = default;

  ~IndexWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  ConstructIndex(  //
      const size_t thread_num,
      const size_t insert_num)
  {
    // lambda function to insert key-value pairs in a certain thread
    auto f = [&](const size_t begin, const size_t end) {
      for (size_t i = begin; i < end; ++i) {
        this->Write(i, i);
      }
    };

    // insert initial key-value pairs in multi-threads
    std::vector<std::thread> threads;
    size_t begin = 0;
    for (size_t i = 0; i < thread_num; ++i) {
      size_t n = (insert_num + i) / thread_num;
      size_t end = begin + n;
      threads.emplace_back(f, begin, end);
      begin = end;
    }
    for (auto &&t : threads) t.join();
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const Key &key)
  {
    return index_.Read(key);
  }

  void
  Scan(  //
      const Key &begin_key,
      const Key &scan_range)
  {
    const auto &begin_k = std::make_pair(begin_key, kClosed);
    const auto &end_k = std::make_pair(begin_key + scan_range, kClosed);

    Value sum = 0;
    for (auto &&iter = index_.Scan(begin_k, end_k); iter.HasNext(); ++iter) {
      sum += iter.GetPayload();
    }
  }

  auto
  Write(  //
      const Key &key,
      const Value &value)
  {
    return index_.Write(key, value);
  }

  auto
  Insert(  //
      const Key &key,
      const Value &value)
  {
    return index_.Insert(key, value);
  }

  auto
  Update(  //
      const Key &key,
      const Value &value)
  {
    return index_.Update(key, value);
  }

  auto
  Delete(const Key &key)
  {
    return index_.Delete(key);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  Index_t index_{kGCInterval, kGCThreadNum};
};
