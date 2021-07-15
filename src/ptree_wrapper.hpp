// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

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
