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

#ifndef INDEX_BENCHMARK_INDEXES_OPEN_BW_TREE_HPP
#define INDEX_BENCHMARK_INDEXES_OPEN_BW_TREE_HPP

// C++ standard libraries
#include <atomic>
#include <optional>
#include <utility>
#include <vector>

// external sources
#include "open_bwtree/BwTree/bwtree.h"

// local sources
#include "common.hpp"
#include "key.hpp"

/*######################################################################################
 * Specification for OpenBw-Tree
 *####################################################################################*/

namespace wangziqi2013::bwtree
{
/// disable OpenBw-Tree's debug logs
bool print_flag = false;

/// initialize GC ID for each thread
thread_local int BwTreeBase::gc_id = 0;

/// initialize the counter of the total number of entering threads
std::atomic<size_t> BwTreeBase::total_thread_num = 1;

}  // namespace wangziqi2013::bwtree

/*######################################################################################
 * Class definition
 *####################################################################################*/

template <class Key, class Payload>
class OpenBwTreeWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Index_t = wangziqi2013::bwtree::BwTree<Key, Payload>;
  using ForwardIterator = typename Index_t::ForwardIterator;
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
        std::optional<Key> begin_key = std::nullopt)
        : index_{index}
    {
      if (begin_key) {
        iter_ = ForwardIterator{index_, *begin_key};
      } else {
        iter_ = index_->Begin();
      }
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
    explicit operator bool() { return !iter_.IsEnd(); }

    /**
     * @brief Forward this iterator.
     *
     */
    constexpr void
    operator++()
    {
      ++iter_;
    }

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @return a payload of a current record
     */
    [[nodiscard]] auto
    GetPayload()  //
        -> Payload
    {
      return iter_->second;
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// a pointer to a BwTree for sibling scanning.
    Index_t *index_{nullptr};

    /// the current begin key.
    ForwardIterator iter_{};
  };

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  OpenBwTreeWrapper(  //
      [[maybe_unused]] const size_t gc_interval,
      [[maybe_unused]] const size_t gc_thread_num)
  {
    index_.UpdateThreadLocal(2 * kMaxCoreNum + 1);
  }

  ~OpenBwTreeWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  SetUp()
  {
    open_bw_thread_id_ = open_bw_thread_counter_.fetch_add(1);
    index_.AssignGCID(open_bw_thread_id_);
  }

  void
  TearDown()
  {
    index_.UnregisterThread(open_bw_thread_id_);
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
    std::vector<Payload> read_results{};
    index_.GetValue(key, read_results);

    if (read_results.empty()) return std::nullopt;
    return read_results[0];
  }

  auto
  Scan(const ScanKey &begin_key = std::nullopt)  //
      -> RecordIterator
  {
    if (begin_key) {
      const auto &[key, key_len, closed] = *begin_key;
      return RecordIterator{&index_, key};
    }
    return RecordIterator{&index_};
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)
  {
    index_.Upsert(key, value);
    return kSuccess;
  }

  auto
  Insert(  //
      const Key &key,
      const Payload &value)
  {
    return (index_.Insert(key, value)) ? kSuccess : kFailed;
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
  Delete(const Key &key)
  {
    // a delete operation in Open-Bw-tree requrires a key-value pair
    return (index_.Delete(key, Payload{key.GetValue()})) ? kSuccess : kFailed;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an atomic counter to count the number of worker threads
  static inline std::atomic_size_t open_bw_thread_counter_ = 1;

  /// a thread id for each worker thread
  static thread_local inline size_t open_bw_thread_id_ = 0;

  Index_t index_{};
};

template <>
constexpr auto
HasSetUpTearDown<OpenBwTreeWrapper>()  //
    -> bool
{
  return true;
}

namespace std
{
template <>
struct hash<Key<8>> {
  auto
  operator()(const Key<8> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key<16>> {
  auto
  operator()(const Key<16> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key<32>> {
  auto
  operator()(const Key<32> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key<64>> {
  auto
  operator()(const Key<64> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key<128>> {
  auto
  operator()(const Key<128> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

}  // namespace std

#endif  // INDEX_BENCHMARK_INDEXES_OPEN_BW_TREE_HPP
