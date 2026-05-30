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
#include <utility>

// external C++ libraries
#include <dbgroup/constants.hpp>

namespace dbgroup::index_bench
{
/**
 * @brief A class for representing variable-length data.
 *
 */
struct alignas(kWordSize) VarLenData {
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  constexpr VarLenData() noexcept = default;

  explicit VarLenData(  //
      const std::string_view src)
      : len_{static_cast<uint16_t>(src.length())}
  {
    auto* data = Alloc();
    std::memcpy(data, src.data(), src.length());
  }

  VarLenData(  //
      const void* const src,
      const size_t len) noexcept
      : len_{static_cast<uint16_t>(len)}
  {
    auto* data = Alloc();
    std::memcpy(data, src, len);
  }

  VarLenData(  //
      VarLenData&& obj) noexcept
      : len_{obj.len_}
      , has_ptr_{obj.has_ptr_}
  {
    std::memcpy(data_, &obj.data_, kInlinableLen);
    obj.has_ptr_ = 0;
  }

  auto
  operator=(                      //
      VarLenData&& obj) noexcept  //
      -> VarLenData&
  {
    len_ = obj.len_;
    has_ptr_ = obj.has_ptr_;
    std::memcpy(data_, &obj.data_, kInlinableLen);
    obj.has_ptr_ = 0;

    return *this;
  }

  // forbit copying
  VarLenData(const VarLenData&) = delete;
  auto operator=(const VarLenData&) -> VarLenData& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~VarLenData()
  {
    if (has_ptr_) {
      delete[] Get().first;
    }
  }

  /*##########################################################################*
   * Public operators
   *##########################################################################*/

  auto
  operator<(                                 //
      const VarLenData& rhs) const noexcept  //
      -> bool
  {
    const auto [l_data, l_len] = Get();
    const auto [r_data, r_len] = rhs.Get();
    const auto lt = (l_len < r_len);
    const auto cmp = std::memcmp(l_data, r_data, lt ? l_len : r_len);
    return cmp < 0 || (cmp == 0 && lt);
  }

  auto
  operator>(                                 //
      const VarLenData& rhs) const noexcept  //
      -> bool
  {
    const auto [l_data, l_len] = Get();
    const auto [r_data, r_len] = rhs.Get();
    const auto gt = (l_len > r_len);
    const auto cmp = std::memcmp(l_data, r_data, gt ? r_len : l_len);
    return cmp > 0 || (cmp == 0 && gt);
  }

  auto
  operator==(                                //
      const VarLenData& rhs) const noexcept  //
      -> bool
  {
    const auto [l_data, l_len] = Get();
    const auto [r_data, r_len] = rhs.Get();
    return l_len == r_len && std::memcmp(l_data, r_data, l_len) == 0;
  }

  auto
  operator!=(                                //
      const VarLenData& rhs) const noexcept  //
      -> bool
  {
    const auto [l_data, l_len] = Get();
    const auto [r_data, r_len] = rhs.Get();
    return l_len != r_len || std::memcmp(l_data, r_data, l_len) != 0;
  }

  /*##########################################################################*
   * Public APIs
   *##########################################################################*/

  constexpr auto
  Get() noexcept  //
      -> std::pair<char*, size_t>
  {
    char* ret;
    if (has_ptr_) {
      ret = *std::bit_cast<char**>(&(data_[kPtrPos]));
    } else {
      ret = std::bit_cast<char*>(&(data_[0]));
    }
    return {ret, static_cast<size_t>(len_)};
  }

  [[nodiscard]]
  constexpr auto
  Get() const noexcept  //
      -> std::pair<const char*, size_t>
  {
    char* ret;
    if (has_ptr_) {
      ret = *std::bit_cast<char**>(&(data_[kPtrPos]));
    } else {
      ret = std::bit_cast<char*>(&(data_[0]));
    }
    return {ret, static_cast<size_t>(len_)};
  }

 private:
  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  static constexpr auto kInlinableLen = 30;

  static constexpr auto kPtrPos = 6;

  /*##########################################################################*
   * Internal utilities
   *##########################################################################*/

  auto
  Alloc()  //
      -> char*
  {
    char* data;
    if (has_ptr_) {
      auto* dst = std::bit_cast<char**>(&(data_[kPtrPos]));
      *dst = new char[len_];
      data = *dst;
    } else {
      data = std::bit_cast<char*>(&(data_[0]));
    }
    return data;
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  uint16_t len_ : 15 {};

  uint16_t has_ptr_ : 1 {len_ > kInlinableLen};

  char data_[kInlinableLen] = {};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WORKLOAD_VAR_LEN_DATA_HPP_
