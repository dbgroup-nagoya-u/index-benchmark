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

#ifndef INDEX_BENCHMARK_INDEXES_ALEX_OLC_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_ALEX_OLC_WRAPPER_HPP

// C++ standard libraries
#include <algorithm>
#include <atomic>
#include <iostream>
#include <iterator>
#include <optional>
#include <string>
#include <utility>

// external sources
#include "GRE/src/competitor/alexol/src/alex.h"

// local sources
#include "common.hpp"

/*######################################################################################
 * Class definition
 *####################################################################################*/

template <class Key, class Payload>
class AlexOLCWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Index_t = alexol::
      Alex<Key, Payload, alexol::AlexCompare, std::allocator<std::pair<Key, Payload>>, false>;
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
        std::pair<Key, Payload> *records,
        size_t size)
        : index_{index}, records_{records}, size_{size}, pos_{0}
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

        const auto &next_key = records_[kScanSize - 1].first + 1;
        size_ = index_->range_scan_by_size(next_key, kScanSize, records_);
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
      return records_[pos_].second;
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// a pointer to a BwTree for sibling scanning.
    Index_t *index_{nullptr};

    /// the scanned records.
    std::pair<Key, Payload> *records_{nullptr};

    /// the number of records.
    size_t size_{0};

    /// the position of a current record.
    size_t pos_{0};
  };

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  AlexOLCWrapper(  //
      [[maybe_unused]] const size_t gc_interval,
      [[maybe_unused]] const size_t gc_thread_num)
  {
  }

  ~AlexOLCWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  constexpr auto
  Bulkload(  //
      const std::vector<std::pair<Key, Payload>> &entries,
      [[maybe_unused]] const size_t thread_num)
  {
    // switch buffer to ignore messages
    auto *tmp_cout_buf = std::cout.rdbuf();
    std::ofstream out_null{"/dev/null"};
    std::cout.rdbuf(out_null.rdbuf());

    // call bulkload API of ALEX
    auto &&non_const_entries = const_cast<std::vector<std::pair<Key, Payload>> &>(entries);
    index_.bulk_load(non_const_entries.data(), static_cast<int>(non_const_entries.size()));

    // reset output buffer
    std::cout.rdbuf(tmp_cout_buf);
    return kSuccess;
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    Payload value{};
    if (index_.get_payload(key, &value)) return value;
    return std::nullopt;
  }

  auto
  Scan(const ScanKey &begin_key = std::nullopt)  //
      -> RecordIterator
  {
    thread_local std::pair<Key, Payload> records[kScanSize];
    auto *ptr = static_cast<std::pair<Key, Payload> *>(records);

    if (begin_key) {
      const auto &[key, key_len, closed] = *begin_key;
      const size_t size = index_.range_scan_by_size(key, kScanSize, ptr);
      return RecordIterator{&index_, ptr, size};
    }

    Key key{0};
    const size_t size = index_.range_scan_by_size(key, kScanSize, ptr);
    return RecordIterator{&index_, ptr, size};
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)
  {
    if (index_.insert(key, value)) return kSuccess;
    return (index_.update(key, value)) ? kSuccess : kFailed;
  }

  auto
  Insert(  //
      const Key &key,
      const Payload &value)
  {
    return (index_.insert(key, value)) ? kSuccess : kFailed;
  }

  auto
  Update(  //
      const Key &key,
      const Payload &value)
  {
    return (index_.update(key, value)) ? kSuccess : kFailed;
  }

  auto
  Delete(const Key &key)
  {
    return (index_.erase(key) > 0) ? kSuccess : kFailed;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  Index_t index_{};
};

#endif  // INDEX_BENCHMARK_INDEXES_ALEX_OLC_WRAPPER_HPP
