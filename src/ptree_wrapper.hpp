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
  comp(const Key &a, const Key &b)
  {
    return a < b;
  }
};

template <class Key, class Value>
class PTreeWrapper
{
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

  constexpr void
  Read(const Key key)
  {
    ptree_.find(key);
  }

  constexpr void
  Scan(const Key begin_key, const Key end_key)
  {
    PTree_t::entries(PTree_t::range(ptree_, begin_key, end_key));
  }

  constexpr void
  Write(const Key key, const Value value)
  {
    // ptree_->insert means "upsert"
    ptree_.insert(std::make_pair(key, value));
  }

  constexpr void
  Insert(const Key key, const Value value)
  {
    // ptree_->insert means "upsert"
    ptree_.insert(std::make_pair(key, value));
  }

  constexpr void
  Update(const Key key, const Value value)
  {
    // define function for updating value
    auto f = [&](std::pair<Key, Value>) { return value; };
    // do nothing if key does not exist
    ptree_.update(key, f);
  }

  constexpr void
  Delete(const Key key)
  {
    PTree_t::remove(ptree_, key);
  }
};
