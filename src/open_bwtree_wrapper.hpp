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
