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

#ifndef INDEX_BENCHMARK_OPERATION_HPP
#define INDEX_BENCHMARK_OPERATION_HPP

#include "common.hpp"

/**
 * @brief A class to represent index read/write operations.
 *
 */
template <class Key, class Payload>
struct Operation {
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  Operation(  //
      IndexOperation t,
      uint32_t k,
      uint32_t v)
      : type{t}, key{k}, value{v}
  {
  }

  /*####################################################################################
   * Public getters
   *##################################################################################*/

  [[nodiscard]] constexpr auto
  GetKey() const  //
      -> Key
  {
    return Key{key};
  }

  [[nodiscard]] constexpr auto
  GetPayload() const  //
      -> Payload
  {
    return Payload{value};
  }

  [[nodiscard]] constexpr auto
  GetKeyLength() const  //
      -> size_t
  {
    return sizeof(Key);
  }

  [[nodiscard]] constexpr auto
  GetPayloadLength() const  //
      -> size_t
  {
    return sizeof(Payload);
  }

  /*####################################################################################
   * Public member variables
   *##################################################################################*/

  /// a read/write operation to perform
  IndexOperation type{IndexOperation::kRead};

  /// a target key of this operation
  uint32_t key{};

  /// a target data of this operation
  uint32_t value{};
};

#endif  // INDEX_BENCHMARK_OPERATION_HPP
