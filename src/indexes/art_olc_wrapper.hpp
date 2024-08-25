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
#include <optional>
#include <utility>

// external sources
#include "Key.h"
#include "Tree.h"

// local sources
#include "common.hpp"

namespace dbgroup
{

template <class K, class Payload>
class ArtOLCWrapper
{
  /*############################################################################
   * Type aliases
   *##########################################################################*/

  using Index_t = ::ART_OLC::Tree;
  using ThreadInfo_t = ::ART::ThreadInfo;
  using ArtKey = ::Key;  // ART's key type
  using ScanKey = std::optional<std::tuple<const K &, size_t, bool>>;

 public:
  /*############################################################################
   * Public inner classes
   *##########################################################################*/

  /**
   * @brief A class for representing an iterator of scan results.
   *
   */
  class RecordIterator
  {
   public:
    /*##########################################################################
     * Public constructors and assignment operators
     *########################################################################*/

    /**
     * @brief Construct a new object as an initial iterator.
     *
     * @param index a pointer to an index.
     */
    RecordIterator(  //
        Index_t *index,
        TID *tuple_ids,
        size_t size)
        : index_{index}, tuple_ids_{tuple_ids}, size_{size}
    {
    }

    RecordIterator(const RecordIterator &) = delete;
    RecordIterator(RecordIterator &&) = delete;

    auto operator=(const RecordIterator &) -> RecordIterator & = delete;
    auto operator=(RecordIterator &&obj) -> RecordIterator & = delete;

    /*##########################################################################
     * Public destructors
     *########################################################################*/

    /**
     * @brief Destroy the iterator and a retained node if exist.
     *
     */
    ~RecordIterator() = default;

    /*##########################################################################
     * Public operators for iterators
     *########################################################################*/

    /**
     * @retval true if this iterator indicates a live record.
     * @retval false otherwise.
     */
    explicit
    operator bool()
    {
      while (true) {
        if (pos_ < size_) return true;        // records remain in this node
        if (size_ < kScanSize) return false;  // this node is the end of range-scan

        auto &&ti = index_->getThreadInfo();
        const uint32_t next_tid = tuple_ids_[kScanSize - 1U] + 1U;
        K k{next_tid};
        ArtKey cont_key{};
        size_ = 0;
        index_->lookupRange(ToArtKey(k), kEndKey, cont_key, tuple_ids_, kScanSize, size_, ti);
        pos_ = 0;
      }
    }

    /**
     * @brief Forward this iterator.
     *
     */
    constexpr void
    operator++()
    {
      ++pos_;
    }

    /*##########################################################################
     * Public getters/setters
     *########################################################################*/

    /**
     * @return a payload of a current record
     */
    [[nodiscard]] auto
    GetPayload() const  //
        -> Payload
    {
      return Payload{tuple_ids_[pos_]};
    }

   private:
    /*##########################################################################
     * Internal member variables
     *########################################################################*/

    /// a pointer to a BwTree for sibling scanning.
    Index_t *index_{nullptr};

    /// the scanned payloads.
    TID *tuple_ids_{nullptr};

    /// the number of payloads.
    size_t size_{0};

    /// the position of a current record.
    size_t pos_{0};
  };

  /*############################################################################
   * Public constructors/destructors
   *##########################################################################*/

  ArtOLCWrapper() = default;

  ~ArtOLCWrapper() = default;

  /*############################################################################
   * Public utility functions
   *##########################################################################*/

  constexpr auto
  Bulkload(  //
      [[maybe_unused]] const std::vector<std::pair<K, Payload>> &entries,
      [[maybe_unused]] const size_t thread_num)  //
      -> int
  {
    return kFailed;
  }

  /*############################################################################
   * Public read/write APIs
   *##########################################################################*/

  auto
  Read(const K &key)  //
      -> std::optional<Payload>
  {
    auto &&ti = index_.getThreadInfo();
    const auto tid = index_.lookup(ToArtKey(key), ti);
    if (tid != 0) return Payload{tid};
    return std::nullopt;
  }

  auto
  Scan(const ScanKey &begin_key = std::nullopt)  //
      -> RecordIterator
  {
    thread_local TID tuple_ids_[kScanSize];

    auto &&ti = index_.getThreadInfo();
    const auto &key = (begin_key) ? std::get<0>(*begin_key) : K{0};
    ArtKey cont_key{};
    size_t rec_num = 0;
    index_.lookupRange(ToArtKey(key), kEndKey, cont_key, tuple_ids_, kScanSize, rec_num, ti);

    return RecordIterator{&index_, tuple_ids_, rec_num};
  }

  auto
  Write(  //
      const K &key,
      [[maybe_unused]] const Payload &value)
  {
    auto &&ti = index_.getThreadInfo();
    index_.insert(ToArtKey(key), key, ti);
    return kSuccess;
  }

  auto
  Insert(  //
      [[maybe_unused]] const K &key,
      [[maybe_unused]] const Payload &value)
  {
    throw std::runtime_error{"ERROR: the insert operation is not implemented."};
    return kFailed;
  }

  auto
  Update(  //
      [[maybe_unused]] const K &key,
      [[maybe_unused]] const Payload &value)
  {
    throw std::runtime_error{"ERROR: the update operation is not implemented."};
    return kFailed;
  }

  auto
  Delete(const K &key)
  {
    auto &&ti = index_.getThreadInfo();
    index_.remove(ToArtKey(key), key, ti);
    return kSuccess;
  }

 private:
  /*############################################################################
   * Internal constants
   *##########################################################################*/

  static const inline ArtKey kEndKey{std::numeric_limits<uint64_t>::max()};

  /*############################################################################
   * Internal utility functions
   *##########################################################################*/

  static void
  LoadKey(TID tid, ArtKey &key)
  {
    key.setInt(tid);
  }

  static constexpr auto
  ToArtKey(const K &k)  //
      -> ArtKey
  {
    return ArtKey{k};
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  Index_t index_{LoadKey};
};

}  // namespace dbgroup

#endif  // INDEX_BENCHMARK_INDEXES_ART_OLC_WRAPPER_HPP
