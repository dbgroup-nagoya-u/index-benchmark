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
#include <limits>
#include <random>
#include <utility>
#include <vector>

#include "../external/PAM/c++/pam.h"
#include "../external/PAM/c++/pbbslib/utilities.h"
#include "common.hpp"

template <class Key, class Value>
struct ptree_entry {
  using key_t = Key;
  using val_t = Value;
  static_assert(!std::is_pointer<Key>::value);
  static_assert(std::is_scalar<Key>::value);

  static bool
  comp(const Key& a, const Key& b)
  {
    return a < b;
  }
};

template <class Key, class Value>
class PTreeWrapper
{
  using ptree_entry_t = ptree_entry<Key, Value>;
  using PTree_t = pam_map<ptree_entry<Key, Value>>;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/
  PTree_t ptree_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  PTreeWrapper() {}

  ~PTreeWrapper() = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  ConstructIndex(  //
      [[maybe_unused]] const size_t thread_num,
      const size_t insert_num)
  {
    for (size_t i = 0; i < insert_num; ++i) {
      ptree_.insert(std::make_pair(i, i));
    }
  }

  /*################################################################################################
   * Public read/write APIs
   *##############################################################################################*/

  std::pair<int64_t, Value>
  Read(const Key key)
  {
    constexpr Value kDefaultVal = std::numeric_limits<Value>::max();

    const auto read_val = ptree_.find(key, kDefaultVal);
    if (read_val == kDefaultVal) {
      return {1, kDefaultVal};
    }
    return {0, read_val};
  }

  void
  Scan(  //
      const Key begin_key,
      const Key scan_range)
  {
    const auto end_key = begin_key + scan_range;

    PTree_t::entries(PTree_t::range(ptree_, begin_key, end_key));
  }

  int64_t
  Write(  //
      const Key key,
      const Value value)
  {
    // ptree_->insert means "upsert"
    ptree_.insert(std::make_pair(key, value));

    return 0;
  }

  int64_t
  Insert(  //
      const Key key,
      const Value value)
  {
    // ptree_->insert means "upsert"
    assert(false);
    ptree_.insert(std::make_pair(key, value));

    return 0;
  }

  int64_t
  Update(  //
      const Key key,
      const Value value)
  {
    // define function for updating value
    auto f = [&](std::pair<Key, Value>) { return value; };
    // do nothing if key does not exist
    ptree_.update(key, f);

    return 0;
  }

  int64_t
  Delete(const Key key)
  {
    PTree_t::remove(ptree_, key);

    return 0;
  }
};
