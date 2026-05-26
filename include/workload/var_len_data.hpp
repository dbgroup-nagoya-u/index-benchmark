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

#ifndef INDEX_BENCHMARK_WORKLOAD_VAR_LEN_DATA_HPP_
#define INDEX_BENCHMARK_WORKLOAD_VAR_LEN_DATA_HPP_

// C++ standard libraries
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

// local sources
#include "common.hpp"

namespace dbgroup::index_bench
{
/**
 * @brief A class for representing variable-length data.
 *
 */
struct VarLenData {
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr VarLenData() noexcept = default;

  explicit VarLenData(  //
      const std::string_view src) noexcept
      : len{static_cast<uint16_t>(src.length())}
  {
    std::memcpy(data, src.data(), len);
  }

  VarLenData(  //
      const void* const src,
      const size_t len) noexcept
      : len{static_cast<uint16_t>(len)}
  {
    std::memcpy(data, src, len);
  }

  constexpr VarLenData(const VarLenData&) noexcept = default;
  constexpr VarLenData(VarLenData&&) noexcept = default;

  constexpr auto operator=(const VarLenData&) noexcept -> VarLenData& = default;
  constexpr auto operator=(VarLenData&&) noexcept -> VarLenData& = default;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~VarLenData() = default;

  /*##########################################################################*
   * Public utilities
   *##########################################################################*/

  auto
  operator<(                                 //
      const VarLenData& rhs) const noexcept  //
      -> bool
  {
    const auto lt = (len < rhs.len);
    const auto cmp = std::memcmp(data, rhs.data, lt ? len : rhs.len);
    return cmp < 0 || (cmp == 0 && lt);
  }

  auto
  operator>(                                 //
      const VarLenData& rhs) const noexcept  //
      -> bool
  {
    const auto gt = (len > rhs.len);
    const auto cmp = std::memcmp(data, rhs.data, gt ? rhs.len : len);
    return cmp > 0 || (cmp == 0 && gt);
  }

  auto
  operator==(                                //
      const VarLenData& rhs) const noexcept  //
      -> bool
  {
    return len == rhs.len && std::memcmp(data, rhs.data, len) == 0;
  }

  auto
  operator!=(                                //
      const VarLenData& rhs) const noexcept  //
      -> bool
  {
    return len != rhs.len || std::memcmp(data, rhs.data, len) != 0;
  }

  /*##########################################################################*
   * Public member variables
   *##########################################################################*/

  /// @brief An actual data.
  std::byte data[kMaxVarLenSize] = {};

  /// @brief The length of a stored data.
  uint16_t len{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WORKLOAD_VAR_LEN_DATA_HPP_
