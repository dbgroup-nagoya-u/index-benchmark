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

#ifndef INDEX_BENCHMARK_INDEXES_BTREE_OLC_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_BTREE_OLC_WRAPPER_HPP

#include <optional>
#include <utility>

#include "../common.hpp"
#include "open_bwtree/BTreeOLC/BTreeOLC.h"

template <class Key, class Value>
class BTreeOLCWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using BTreeOLC_t = btreeolc::BTree<Key, Value>;

 public:
  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  explicit BTreeOLCWrapper([[maybe_unused]] const size_t worker_num) {}

  ~BTreeOLCWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  SetUp()
  {
  }

  void
  TearDown()
  {
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const Key &key)  //
      -> std::optional<Value>
  {
    Value value{};
    if (index_.lookup(key, value)) return value;
    return std::nullopt;
  }

  void
  Scan(  //
      [[maybe_unused]] const Key &begin_key,
      [[maybe_unused]] const Key &scan_range)
  {
    // this operation is not implemented
    assert(false);
    return;
  }

  auto
  Write(  //
      const Key &key,
      const Value &value)
  {
    index_.insert(key, value);
    return 0;
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Value &value)
  {
    // this operation is not implemented
    assert(false);
    return 1;
  }

  auto
  Update(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Value &value)
  {
    // this operation is not implemented
    assert(false);
    return 1;
  }

  auto
  Delete([[maybe_unused]] const Key &key)
  {
    // this operation is not implemented
    assert(false);
    return 1;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  BTreeOLC_t index_{};
};

#endif  // INDEX_BENCHMARK_INDEXES_BTREE_OLC_WRAPPER_HPP
