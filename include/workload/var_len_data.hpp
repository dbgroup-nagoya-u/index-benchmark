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

#ifndef INDEX_BENCHMARK_VAR_LEN_DATA_HPP
#define INDEX_BENCHMARK_VAR_LEN_DATA_HPP

// C++ standard libraries
#include <cstdint>
#include <string>

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

  constexpr VarLenData() = default;

  explicit VarLenData(  //
      const char *src);

  explicit VarLenData(  //
      const std::string &src);

  constexpr VarLenData(const VarLenData &) = default;
  constexpr VarLenData(VarLenData &&) noexcept = default;

  constexpr auto operator=(const VarLenData &) -> VarLenData & = default;
  constexpr auto operator=(VarLenData &&) noexcept -> VarLenData & = default;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~VarLenData() = default;

  /*##########################################################################*
   * Public utilities
   *##########################################################################*/

  auto operator<(                   //
      const VarLenData &rhs) const  //
      -> bool;

  auto operator>(                   //
      const VarLenData &rhs) const  //
      -> bool;

  auto operator==(                  //
      const VarLenData &rhs) const  //
      -> bool;

  auto operator!=(                  //
      const VarLenData &rhs) const  //
      -> bool;

  /*##########################################################################*
   * Public member variables
   *##########################################################################*/

  /// @brief An actual data.
  char data[kMaxVarLenSize]{};

  /// @brief The length of a stored data.
  uint8_t len{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_VAR_LEN_DATA_HPP
