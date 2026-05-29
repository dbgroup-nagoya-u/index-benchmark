/*
 * Copyright 2023 Database Group, Nagoya University
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

#ifndef INDEX_BENCHMARK_INDEXES_ART_OLC_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_ART_OLC_WRAPPER_HPP

// C++ standard libraries
#include <cstddef>
#include <cstring>
#include <functional>
#include <optional>

// external libraries
#include <Key.h>
#include <Tree.h>

// external C++ libraries
#include <dbgroup/index/utility.hpp>

// local sources
#include "workload/key_space.hpp"

namespace dbgroup::index_bench
{
template <class KeyT, class Payload, class Comp = std::less<Key>>
class ARTOLCWrapper
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Index = ::ART_OLC::Tree;
  using ThreadInfo_t = ::ART::ThreadInfo;
  using ARTKey = ::Key;  // ART's key type

 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  ARTOLCWrapper() = default;

  ARTOLCWrapper(const ARTOLCWrapper&) = delete;
  ARTOLCWrapper(ARTOLCWrapper&&) = delete;

  auto operator=(const ARTOLCWrapper&) -> ARTOLCWrapper& = delete;
  auto operator=(ARTOLCWrapper&&) -> ARTOLCWrapper& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~ARTOLCWrapper() = default;

  /*##########################################################################*
   * Public read/write APIs
   *##########################################################################*/

  auto
  Read(  //
      const KeyT& key,
      const size_t key_len)  //
      -> std::optional<Payload>
  {
    auto&& ti = index_.getThreadInfo();
    std::optional<Payload> ret{};
    if (index_.lookup(ToARTKey(key, key_len), ti) > 0) {
      ret.emplace(1);
    }
    return ret;
  }

  void
  Write(  //
      const KeyT& key,
      const Payload& value,
      const size_t key_len)
  {
    auto&& ti = index_.getThreadInfo();
    index_.insert(ToARTKey(key, key_len), value, ti);
  }

  /*############################################################################*
   * Public static variables
   *############################################################################*/
  // NOLINTBEGIN

  /// @brief Declare global key space for accessing the original keys.
  static inline KeySpace<StrKey>* key_space{};

  // NOLINTEND
 private:
  /*##########################################################################*
   * Internal utility functions
   *##########################################################################*/

  static void
  LoadKey(  //
      TID tid,
      ARTKey& key)
  {
    if constexpr (std::is_same_v<KeyT, StrKey>) {
      const auto& [src_key, key_len] = key_space->GetKey(tid);
      key.set(src_key, key_len);
    } else {
      key.setInt(tid);
    }
  }

  static auto
  ToARTKey(  //
      const KeyT& key,
      const size_t key_len)  //
      -> ARTKey
  {
    ARTKey ret{};
    ret.set(index::ConvertToBinaryData<KeyT, char>(key), key_len);
    return ret;
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  Index index_{LoadKey};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_INDEXES_ART_OLC_WRAPPER_HPP
