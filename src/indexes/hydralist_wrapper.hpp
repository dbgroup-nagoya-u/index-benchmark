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

#ifndef INDEX_BENCHMARK_INDEXES_HYDRALIST_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_HYDRALIST_WRAPPER_HPP

// C++ standard libraries
#include <optional>
#include <utility>
#include <vector>

// external sources
#include "HydraList.h"
#include "numa-config.h"

// local sources
#include "common.hpp"

namespace dbgroup
{

template <class Key, class Payload>
class HydraListWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Index_t = ::HydraList;
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
        Key &&key,
        std::vector<Payload> &payloads)
        : index_{index}, key_{key}, payloads_{payloads}
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
        const auto size = payloads_.size();
        if (pos_ < size) return true;        // records remain in this node
        if (size < kScanSize) return false;  // this node is the end of range-scan

        key_ += kScanSize;
        payloads_.clear();
        index_->scan(key_, kScanSize, payloads_);
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
      return payloads_.at(pos_);
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
    std::vector<Payload> &payloads_;

    /// the position of a current record.
    size_t pos_{0};
  };

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  HydraListWrapper(  //
      [[maybe_unused]] const size_t gc_interval,
      [[maybe_unused]] const size_t gc_thread_num)
  {
  }

  ~HydraListWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  SetUp()
  {
    index_.registerThread();
  }

  void
  TearDown()
  {
    index_.unregisterThread();
  }

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
    return index_.lookup(key);
  }

  auto
  Scan(const ScanKey &begin_key = std::nullopt)  //
      -> RecordIterator
  {
    thread_local std::vector<Payload> payloads{};
    payloads.clear();

    auto key = (begin_key) ? std::get<0>(*begin_key) : Key{0};
    index_.scan(key, kScanSize, payloads);

    return RecordIterator{&index_, std::move(key), payloads};
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
    return (index_.remove(key)) ? kSuccess : kFailed;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  Index_t index_{NUM_SOCKET};
};

template <>
constexpr auto
HasSetUpTearDown<HydraListWrapper>()  //
    -> bool
{
  return true;
}

}  // namespace dbgroup

#endif  // INDEX_BENCHMARK_INDEXES_HYDRALIST_WRAPPER_HPP
