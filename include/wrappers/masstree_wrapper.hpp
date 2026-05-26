/*
 * Copyright 2026 Database Group, Nagoya University
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

#ifndef INDEX_BENCHMARK_WRAPPERS_MASSTREE_WRAPPER_HPP_
#define INDEX_BENCHMARK_WRAPPERS_MASSTREE_WRAPPER_HPP_

// C++ standard libraries
#include <cstddef>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <tuple>

// external C++ libraries
#include "dbgroup/masstree/masstree.hpp"

// local sources
#include "common.hpp"  // IWYU pragma: keep

namespace dbgroup::index_bench
{
/*############################################################################*
 * Class definition
 *############################################################################*/

template <class Key, class Payload>
class MasstreeWrapper
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Index = dbgroup::index::masstree::Masstree<Key, Payload>;
  using ScanKey = std::optional<std::tuple<Key, size_t, bool>>;

 public:
  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  MasstreeWrapper() = default;

  MasstreeWrapper(const MasstreeWrapper&) = delete;
  MasstreeWrapper(MasstreeWrapper&&) = delete;

  auto operator=(const MasstreeWrapper&) -> MasstreeWrapper& = delete;
  auto operator=(MasstreeWrapper&&) -> MasstreeWrapper& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~MasstreeWrapper() = default;

  /*##########################################################################*
   * Public read/write APIs
   *##########################################################################*/

  auto
  Read(  //
      const Key& key,
      const size_t key_len)  //
      -> std::optional<Payload>
  {
    return index_.Read(key, key_len);
  }

  auto
  Scan(                                                          //
      [[maybe_unused]] const ScanKey& begin_key = std::nullopt)  //
      -> int
  {
    throw std::runtime_error{"ERROR: the scan operation is not implemented."};
  }

  void
  Write(  //
      const Key& key,
      const Payload& value,
      const size_t key_len)
  {
    return index_.Write(key, value, key_len);
  }

  auto
  Upsert(  //
      const Key& key,
      const Payload& value,
      const size_t key_len)
  {
    return index_.Upsert(key, value, key_len);
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key& key,
      [[maybe_unused]] const Payload& value,
      [[maybe_unused]] const size_t key_len)
  {
    throw std::runtime_error{"ERROR: the insert operation is not implemented."};
  }

  auto
  Update(  //
      [[maybe_unused]] const Key& key,
      [[maybe_unused]] const Payload& value,
      [[maybe_unused]] const size_t key_len)
  {
    throw std::runtime_error{"ERROR: the update operation is not implemented."};
  }

  auto
  Delete(  //
      [[maybe_unused]] const Key& key,
      [[maybe_unused]] const size_t key_len)
  {
    throw std::runtime_error{"ERROR: the delete operation is not implemented."};
  }

 private:
  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  Index index_{};
};

/*############################################################################*
 * Specialization for wrappers
 *############################################################################*/

template <>
constexpr auto
HasBulkload<MasstreeWrapper>()  //
    -> bool
{
  return false;
}

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WRAPPERS_MASSTREE_WRAPPER_HPP_
