// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <random>
#include <utility>
#include <vector>

#include "common.hpp"

template <class Key, class Value>
class OpenBwTreeWrapper
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  OpenBwTreeWrapper() {}

  ~OpenBwTreeWrapper() = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  constexpr void
  Read([[maybe_unused]] const Key key)
  {
  }

  constexpr void
  Scan(  //
      [[maybe_unused]] const Key begin_key,
      [[maybe_unused]] const Key end_key)
  {
  }

  constexpr void
  Write(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
  }

  constexpr void
  Insert(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
  }

  constexpr void
  Update(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
  }

  constexpr void
  Delete([[maybe_unused]] const Key key)
  {
  }
};
