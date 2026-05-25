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
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <stdexcept>
#include <tuple>

// external libraries
#include <Key.h>
#include <Tree.h>

// external C++ libraries
#include <dbgroup/index/utility.hpp>

// local sources
#include "common.hpp"  // IWYU pragma: keep

namespace dbgroup::index_bench
{
template <class KeyT, class Payload>
class ARTOLCWrapper
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Index = ::ART_OLC::Tree;
  using ThreadInfo_t = ::ART::ThreadInfo;
  using ARTKey = ::Key;  // ART's key type
  using ScanKey = std::optional<std::tuple<KeyT, size_t, bool>>;

 public:
  /*##########################################################################*
   * Public class declarations
   *##########################################################################*/

  class Iterator;

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
    const auto tid = index_.lookup(ToARTKey(key, key_len), ti);
    if (tid != 0) return Payload{tid};
    return std::nullopt;
  }

  auto
  Scan(  //
      const ScanKey& begin_key = std::nullopt)
  {
    thread_local TID tuple_ids[kScanSize];

    auto&& ti = index_.getThreadInfo();
    KeyT key;
    size_t key_len;
    if (begin_key) {
      std::tie(key, key_len, std::ignore) = *begin_key;
    } else {
      key = {};
      key_len = 0;
    }
    auto&& bin_key = ToARTKey(key, key_len);

    ARTKey cont_key{};
    size_t rec_num = 0;
    index_.lookupRange(bin_key, kEndKey, cont_key, tuple_ids, kScanSize, rec_num, ti);

    return Iterator{&index_, tuple_ids, rec_num};
  }

  void
  Write(  //
      const KeyT& key,
      [[maybe_unused]] const Payload& value,
      const size_t key_len)
  {
    auto&& ti = index_.getThreadInfo();
    index_.insert(ToARTKey(key, key_len), key, ti);
  }

  auto
  Upsert(  //
      const KeyT& key,
      [[maybe_unused]] const Payload& value,
      const size_t key_len)
  {
    auto&& ti = index_.getThreadInfo();
    index_.insert(ToARTKey(key, key_len), key, ti);
  }

  auto
  Insert(  //
      [[maybe_unused]] const KeyT& key,
      [[maybe_unused]] const Payload& value,
      [[maybe_unused]] const size_t key_len)
  {
    throw std::runtime_error{"ERROR: the insert operation is not implemented."};
  }

  auto
  Update(  //
      [[maybe_unused]] const KeyT& key,
      [[maybe_unused]] const Payload& value,
      [[maybe_unused]] const size_t key_len)
  {
    throw std::runtime_error{"ERROR: the update operation is not implemented."};
  }

  auto
  Delete(  //
      const KeyT& key,
      const size_t key_len)
  {
    auto&& ti = index_.getThreadInfo();
    index_.remove(ToARTKey(key, key_len), key, ti);
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
        TID* tuple_ids,
        size_t size)
        : index_{index}
        , tuple_ids_{tuple_ids}
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

        auto&& ti = index_->getThreadInfo();
        const uint32_t next_tid = tuple_ids_[kScanSize - 1U] + 1U;
        KeyT key{next_tid};
        auto&& bin_key = ToARTKey(key, sizeof(KeyT));
        ARTKey cont_key{};
        size_ = 0;
        index_->lookupRange(bin_key, kEndKey, cont_key, tuple_ids_, kScanSize, size_, ti);
        pos_ = 0;
      }
    }

    constexpr void
    operator++() noexcept
    {
      ++pos_;
    }

    /*########################################################################*
     * Public getters/setters
     *########################################################################*/

    [[nodiscard]] auto
    GetPayload() const  //
        -> Payload
    {
      return Payload{tuple_ids_[pos_]};
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// @brief A pointer to a BwTree for sibling scanning.
    Index* index_{};

    /// @brief The scanned payloads.
    TID* tuple_ids_{};

    /// @brief The number of payloads.
    size_t size_{};

    /// @brief The position of a current record.
    size_t pos_{};
  };

 private:
  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kScanSize = 1000;

  static const inline ARTKey kEndKey{std::numeric_limits<uint64_t>::max()};  // NOLINT

  /*##########################################################################*
   * Internal utility functions
   *##########################################################################*/

  static void
  LoadKey(  //
      TID tid,
      ARTKey& key)
  {
    key.setInt(tid);
  }

  static auto
  ToARTKey(  //
      const KeyT& key,
      const size_t key_len)  //
      -> ARTKey
  {
    ARTKey ret{};
    ret.data = const_cast<uint8_t*>(index::ConvertToBinaryData(key));
    ret.len = key_len;
    return ret;
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  Index index_{LoadKey};
};

/*############################################################################*
 * Specialization for wrappers
 *############################################################################*/

template <>
constexpr auto
HasBulkload<ARTOLCWrapper>()  //
    -> bool
{
  return false;
}

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_INDEXES_ART_OLC_WRAPPER_HPP
