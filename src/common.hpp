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

#ifndef INDEX_BENCHMARK_COMMON_HPP
#define INDEX_BENCHMARK_COMMON_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>

/*######################################################################################
 * Global enum and constants
 *####################################################################################*/

/**
 * @brief A list of index read/write operations.
 *
 */
enum class IndexOperation : uint32_t
{
  kRead,
  kScan,
  kWrite,
  kInsert,
  kUpdate,
  kDelete
};

constexpr size_t kGCInterval = 100000;

constexpr size_t kGCThreadNum = 8;

constexpr bool kClosed = true;

/*######################################################################################
 * Key/Value definitions
 *####################################################################################*/

class Key
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Key() : key_{} {}

  constexpr explicit Key(const size_t key) : key_{key} {}

  constexpr Key(const Key &) = default;
  constexpr Key(Key &&) noexcept = default;
  constexpr auto operator=(const Key &) -> Key & = default;
  constexpr auto operator=(Key &&) -> Key & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Key() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  operator<(const Key &obj) const  //
      -> bool
  {
    return key_ < obj.key_;
  }

  constexpr auto
  operator>(const Key &obj) const  //
      -> bool
  {
    return key_ > obj.key_;
  }

  constexpr auto
  operator==(const Key &obj) const  //
      -> bool
  {
    return key_ == obj.key_;
  }

  constexpr auto
  operator+(const size_t val) const  //
      -> Key
  {
    auto obj = *this;
    obj.key_ += val;
    return obj;
  }

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return key_;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t key_{};
};

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
struct hash<Key> {
  auto
  operator()(const Key &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

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

#endif  // INDEX_BENCHMARK_COMMON_HPP
