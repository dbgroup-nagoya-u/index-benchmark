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

#ifndef INDEX_BENCHMARK_VALUE_HPP
#define INDEX_BENCHMARK_VALUE_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>

class InPlaceVal
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr InPlaceVal() : value_{}, control_bits_{0} {}

  constexpr explicit InPlaceVal(const size_t val) : value_{val}, control_bits_{0} {}

  constexpr InPlaceVal(const InPlaceVal &) = default;
  constexpr InPlaceVal(InPlaceVal &&) noexcept = default;
  constexpr auto operator=(const InPlaceVal &) -> InPlaceVal & = default;
  constexpr auto operator=(InPlaceVal &&) -> InPlaceVal & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~InPlaceVal() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return value_;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t value_ : 61;

  size_t control_bits_ : 3;
};

class AppendVal
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr AppendVal() : value_{}, control_bits_{0} {}

  constexpr explicit AppendVal(const size_t val) : value_{val}, control_bits_{0} {}

  constexpr AppendVal(const AppendVal &) = default;
  constexpr AppendVal(AppendVal &&) noexcept = default;
  constexpr auto operator=(const AppendVal &) -> AppendVal & = default;
  constexpr auto operator=(AppendVal &&) -> AppendVal & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~AppendVal() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return value_;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t value_ : 61;

  size_t control_bits_ : 3;
};

namespace std
{
template <>
struct hash<InPlaceVal> {
  auto
  operator()(const InPlaceVal &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<AppendVal> {
  auto
  operator()(const AppendVal &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};
}  // namespace std

#endif  // INDEX_BENCHMARK_VALUE_HPP
