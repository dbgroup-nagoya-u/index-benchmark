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
#include <utility>
#include <vector>

#include "bztree/bztree.hpp"
#include "common.hpp"

template <class Key, class Value>
class BzTreeWrapper
{
  using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value>;
  using ReturnCode = ::dbgroup::index::bztree::ReturnCode;
  using RecordPage_t = ::dbgroup::index::bztree::RecordPage<Key, Value>;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  BzTree_t bztree_{100000};

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  constexpr BzTreeWrapper() {}

  ~BzTreeWrapper() = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  constexpr bool
  Read(const Key key)
  {
    return bztree_.Read(key).first == ReturnCode::kSuccess;
  }

  constexpr void
  Scan(  //
      const Key begin_key,
      const Key scan_range)
  {
    const auto end_key = begin_key + scan_range;
    Value sum = 0;

    RecordPage_t scan_results;
    bztree_.Scan(scan_results, &begin_key, true, &end_key, true);
    while (!scan_results.empty()) {
      for (auto &&[key, value] : scan_results) sum += value;

      const auto next_key = scan_results.GetLastKey();
      if (next_key == end_key) break;
      bztree_.Scan(scan_results, &next_key, false, &end_key, true);
    }
  }

  constexpr void
  Write(  //
      const Key key,
      const Value value)
  {
    bztree_.Write(key, value);
  }

  constexpr void
  Insert(  //
      const Key key,
      const Value value)
  {
    bztree_.Insert(key, value);
  }

  constexpr void
  Update(  //
      const Key key,
      const Value value)
  {
    bztree_.Update(key, value);
  }

  constexpr void
  Delete(  //
      const Key key)
  {
    bztree_.Delete(key);
  }
};
