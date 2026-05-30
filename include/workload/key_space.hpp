/*
 * Copyright 2025 Database Group, Nagoya University
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

#ifndef INDEX_BENCHMARK_WORKLOAD_KEY_SPACE_HPP_
#define INDEX_BENCHMARK_WORKLOAD_KEY_SPACE_HPP_

// C++ standard libraries
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

// local sources
#include "common.hpp"
#include "workload/var_len_data.hpp"

namespace dbgroup::index_bench
{
template <class Key>
class KeySpace
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Data = std::conditional_t<std::is_same_v<Key, UIntKey>, UIntKey, VarLenData>;

 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  KeySpace() = default;

  KeySpace(  //
      size_t key_num,
      const std::optional<size_t>& rand_seed);

  KeySpace(  //
      size_t key_num,
      const std::optional<size_t>& rand_seed,
      const std::filesystem::path& dataset_path);

  KeySpace(KeySpace&&) noexcept = default;
  auto operator=(KeySpace&&) noexcept -> KeySpace& = default;

  // disable copying
  KeySpace(const KeySpace&) = delete;
  auto operator=(const KeySpace&) -> KeySpace& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~KeySpace() = default;

  /*##########################################################################*
   * Public getters
   *##########################################################################*/

  [[nodiscard]]
  constexpr auto
  Size() const noexcept  //
      -> size_t
  {
    return keys_.size();
  }

  [[nodiscard]]
  constexpr auto
  GetMappedPos(                   //
      size_t pos) const noexcept  //
      -> size_t
  {
    return mapping_[pos];
  }

  [[nodiscard]]
  constexpr auto
  GetKey(                         //
      size_t pos) const noexcept  //
      -> std::pair<Key, size_t>
  {
    const auto& key = keys_[pos];
    if constexpr (std::is_same_v<Key, UIntKey>) {
      return {key, sizeof(Key)};
    } else {
      auto [data, len] = key.Get();
      return {const_cast<char*>(data), len};
    }
  }

 private:
  /*##########################################################################*
   * Internal APIs
   *##########################################################################*/

  void PrepareMapping(  //
      size_t key_num,
      const std::optional<size_t>& rand_seed);

  /*##########################################################################*
   * Static assertions
   *##########################################################################*/

  static_assert(  //
      std::is_same_v<Key, UIntKey> || std::is_same_v<Key, StrKey>,
      "We assume unsigned long or binary data as keys.");

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief Sorted keys.
  std::vector<Data> keys_{};

  /// @brief Mapping IDs for referencing actual keys.
  std::vector<uint32_t> mapping_{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WORKLOAD_KEY_SPACE_HPP_
