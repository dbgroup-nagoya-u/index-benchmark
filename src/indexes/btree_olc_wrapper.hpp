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

#ifndef INDEX_BENCHMARK_INDEXES_BTREE_OLC_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_BTREE_OLC_WRAPPER_HPP

// C++ standard libraries
#include <optional>
#include <utility>

// external sources
#include "open_bwtree/BTreeOLC/BTreeOLC.h"

// local sources
#include "common.hpp"

template <class Key, class Payload>
class BTreeOLCWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Index_t = btreeolc::BTree<Key, Payload>;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;

 public:
  /*####################################################################################
   * Public inner classes
   *##################################################################################*/

  /**
   * @brief A class for representing an iterator of scan results.
   *
   */
  class RecordIterator
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new object as an initial iterator.
     *
     * @param index a pointer to an index.
     */
    RecordIterator(  //
        Index_t *index,
        Key begin_key,
        Payload *payloads,
        size_t size)
        : index_{index}, key_{std::move(begin_key)}, payloads_{payloads}, size_{size}, pos_{0}
    {
    }

    RecordIterator(const RecordIterator &) = delete;
    RecordIterator(RecordIterator &&) = delete;

    auto operator=(const RecordIterator &) -> RecordIterator & = delete;
    auto operator=(RecordIterator &&obj) -> RecordIterator & = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    /**
     * @brief Destroy the iterator and a retained node if exist.
     *
     */
    ~RecordIterator() = default;

    /*##################################################################################
     * Public operators for iterators
     *################################################################################*/

    /**
     * @retval true if this iterator indicates a live record.
     * @retval false otherwise.
     */
    explicit operator bool()
    {
      while (true) {
        if (pos_ < size_) return true;        // records remain in this node
        if (size_ < kScanSize) return false;  // this node is the end of range-scan

        key_ = key_ + kScanSize;
        size_ = index_->scan(key_, kScanSize, payloads_);
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

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @return a payload of a current record
     */
    [[nodiscard]] auto
    GetPayload() const  //
        -> Payload
    {
      return payloads_[pos_];
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// a pointer to a BwTree for sibling scanning.
    Index_t *index_{nullptr};

    /// the current begin key.
    Key key_{};

    /// the scanned payloads.
    Payload *payloads_{nullptr};

    /// the number of payloads.
    size_t size_{0};

    /// the position of a current record.
    size_t pos_{0};
  };

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  BTreeOLCWrapper(  //
      [[maybe_unused]] const size_t gc_interval,
      [[maybe_unused]] const size_t gc_thread_num)
  {
  }

  ~BTreeOLCWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  constexpr auto
  Bulkload(  //
      [[maybe_unused]] const std::vector<std::pair<Key, Payload>> &entries,
      [[maybe_unused]] const size_t thread_num)  //
      -> int
  {
    return kFailed;
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    Payload value{};
    if (index_.lookup(key, value)) return value;
    return std::nullopt;
  }

  auto
  Scan(const ScanKey &begin_key = std::nullopt)  //
      -> RecordIterator
  {
    thread_local Payload payloads[kScanSize];

    if (begin_key) {
      const auto &[key, key_len, closed] = *begin_key;
      const auto size = index_.scan(key, kScanSize, payloads);
      return RecordIterator{&index_, key, payloads, size};
    }

    Key key{0};
    const auto size = index_.scan(key, kScanSize, payloads);
    return RecordIterator{&index_, key, payloads, size};
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)
  {
    index_.insert(key, value);
    return kSuccess;
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)
  {
    throw std::runtime_error{"ERROR: the insert operation is not implemented."};
    return kFailed;
  }

  auto
  Update(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)
  {
    throw std::runtime_error{"ERROR: the update operation is not implemented."};
    return kFailed;
  }

  auto
  Delete([[maybe_unused]] const Key &key)
  {
    throw std::runtime_error{"ERROR: the delete operation is not implemented."};
    return kFailed;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  Index_t index_{};
};

#endif  // INDEX_BENCHMARK_INDEXES_BTREE_OLC_WRAPPER_HPP
