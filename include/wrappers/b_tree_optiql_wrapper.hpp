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

#ifndef INDEX_BENCHMARK_INDEXES_B_TREE_OPTIQL_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_B_TREE_OPTIQL_WRAPPER_HPP

// C++ standard libraries
#include <cstddef>
#include <cstring>
#include <functional>
#include <optional>
#include <tuple>

// external libraries
#include <indexes/BTreeOLC/BTreeOMCS.h>

// local sources
#include "common.hpp"  // IWYU pragma: keep

namespace dbgroup::index_bench
{
template <class Key, class Payload, class Comp = std::less<Key>>
class BTreeOptiQLWrapper
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Index = btreeolc::BTreeOMCS<Key, Payload>;
  using ScanKey = std::optional<std::tuple<Key, size_t, bool>>;

 public:
  /*##########################################################################*
   * Public class declarations
   *##########################################################################*/

  class Iterator;

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  BTreeOptiQLWrapper() = default;

  BTreeOptiQLWrapper(const BTreeOptiQLWrapper&) = delete;
  BTreeOptiQLWrapper(BTreeOptiQLWrapper&&) = delete;

  auto operator=(const BTreeOptiQLWrapper&) -> BTreeOptiQLWrapper& = delete;
  auto operator=(BTreeOptiQLWrapper&&) -> BTreeOptiQLWrapper& = delete;

  ~BTreeOptiQLWrapper() = default;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  auto
  Read(  //
      const Key& key,
      [[maybe_unused]] const size_t key_len)  //
      -> std::optional<Payload>
  {
    Payload value{};
    if (index_.lookup(key, value)) return value;
    return std::nullopt;
  }

  auto
  Scan(  //
      const ScanKey& begin_key = std::nullopt)
  {
    thread_local Payload payloads[kScanSize];

    auto&& key = (begin_key) ? std::get<0>(*begin_key) : Key{0};
    const auto size = index_.scan(key, kScanSize, payloads);
    return Iterator{&index_, key, payloads, size};
  }

  auto
  Insert(  //
      const Key& key,
      const Payload& value,
      [[maybe_unused]] const size_t key_len)  //
      -> std::optional<Payload>
  {
    std::optional<Payload> ret{};
    if (!index_.insert(key, value)) {
      ret.emplace(1);
    }
    return ret;
  }

  auto
  Update(  //
      const Key& key,
      const Payload& value,
      [[maybe_unused]] const size_t key_len)  //
      -> std::optional<Payload>
  {
    std::optional<Payload> ret{};
    if (index_.update(key, value)) {
      ret.emplace(1);
    }
    return ret;
  }

  auto
  Delete(  //
      const Key& key,
      [[maybe_unused]] const size_t key_len)  //
      -> std::optional<Payload>
  {
    std::optional<Payload> ret{};
    if (index_.remove(key)) {
      ret.emplace(1);
    }
    return ret;
  }

  /*##########################################################################*
   * Public class definitions
   *##########################################################################*/

  class Iterator
  {
   public:
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    Iterator(  //
        Index* index,
        Key begin_key,
        Payload* payloads,
        size_t size)
        : index_{index}
        , key_{std::move(begin_key)}
        , payloads_{payloads}
        , size_{size}
    {
    }

    Iterator(const Iterator&) = delete;
    Iterator(Iterator&&) = delete;

    auto operator=(const Iterator&) -> Iterator& = delete;
    auto operator=(Iterator&&) -> Iterator& = delete;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~Iterator() = default;

    /*########################################################################*
     * Public operators for iterators
     *########################################################################*/

    explicit
    operator bool()
    {
      while (true) {
        if (pos_ < size_) return true;        // records remain in this node
        if (size_ < kScanSize) return false;  // this node is the end of range-scan

        key_ = key_ + kScanSize;
        size_ = index_->scan(key_, kScanSize, payloads_);
        pos_ = 0;
      }
    }

    constexpr void
    operator++()
    {
      ++pos_;
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief A pointer to a BwTree for sibling scanning.
    Index* index_{nullptr};

    /// @brief The current begin key.
    Key key_{};

    /// @brief The scanned payloads.
    Payload* payloads_{nullptr};

    /// @brief The number of payloads.
    size_t size_{0};

    /// @brief The position of a current record.
    size_t pos_{0};
  };

 private:
  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kScanSize = 1000;

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  Index index_{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_INDEXES_B_TREE_OPTIQL_WRAPPER_HPP
