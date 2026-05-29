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
#include <optional>
#include <utility>
#include <vector>

namespace dbgroup::index_bench
{
template <class Key>
class KeySpace
{
 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  KeySpace() = default;

  KeySpace(  //
      const size_t key_num,
      const std::optional<size_t> &rand_seed);

  KeySpace(KeySpace &&) noexcept = default;
  auto operator=(KeySpace &&) noexcept -> KeySpace & = default;

  // disable copying
  KeySpace(const KeySpace &) = delete;
  auto operator=(const KeySpace &) -> KeySpace & = delete;

  /*##########################################################################*
   * Public APIs
   *##########################################################################*/

  [[nodiscard]] auto Size() const  //
      -> size_t;

  [[nodiscard]] auto GetKey(   //
      const size_t pos) const  //
      -> std::pair<const Key &, size_t>;

  [[nodiscard]] auto GetSortedKey(  //
      const size_t pos) const       //
      -> std::pair<const Key &, size_t>;

 private:
  /*##########################################################################*
   * Internal APIs
   *##########################################################################*/

  void CreateIntegerKeys(  //
      const size_t key_num);

  void PrepareMapping(  //
  static_assert(  //
      std::is_same_v<Key, UIntKey> || std::is_same_v<Key, StrKey>,
      "We assume unsigned long or binary data as keys.");

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief Sorted keys.
  std::vector<Key> keys_{};

  /// @brief Mapping IDs for referencing actual keys.
  std::vector<size_t> mapping_{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WORKLOAD_KEY_SPACE_HPP_
