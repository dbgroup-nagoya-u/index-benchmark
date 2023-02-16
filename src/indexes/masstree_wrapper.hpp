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

#ifndef INDEX_BENCHMARK_INDEXES_MASSTREE_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_MASSTREE_WRAPPER_HPP

// C++ standard libraries
#include <atomic>
#include <optional>
#include <string>
#include <utility>

// external sources
#include "masstree/clp.h"
#include "masstree/config.h"
#include "masstree/json.hh"
#include "masstree/kvio.hh"
#include "masstree/kvrandom.hh"
#include "masstree/kvrow.hh"
#include "masstree/kvstats.hh"
#include "masstree/kvtest.hh"
#include "masstree/masstree_insert.hh"
#include "masstree/masstree_remove.hh"
#include "masstree/masstree_scan.hh"
#include "masstree/masstree_tcursor.hh"
#include "masstree/nodeversion.hh"
#include "masstree/query_masstree.hh"
#include "masstree/timestamp.hh"

// local sources
#include "common.hpp"

/*######################################################################################
 * Global variables for Masstree
 *####################################################################################*/

/// global epoch, updated by main thread regularly
volatile mrcu_epoch_type globalepoch = timestamp() >> 16;

///
volatile mrcu_epoch_type active_epoch = globalepoch;

///
kvepoch_t global_log_epoch = 0;

/// don't add log entries, and free old value immediately
volatile bool recovering = false;

///
kvtimestamp_t initial_timestamp;

/*######################################################################################
 * Class definition
 *####################################################################################*/

template <class Key, class Payload>
class MasstreeWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Query_t = query<row_type>;
  using Table_t = Masstree::default_table;
  using Str_t = lcdf::Str;
  using Json_t = lcdf::Json;
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
        Table_t *table,
        Key &&key,
        std::vector<Payload> &payloads)
        : table_{table}, payloads_{payloads}, key_{key}
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
        const size_t size = payloads_.size();
        if (pos_ < size) return true;        // records remain in this node
        if (size < kScanSize) return false;  // this node is the end of range-scan

        key_ = key_ + kScanSize;
        Scanner scanner{kScanSize, payloads_};
        table_->table().scan(ToStr(key_), true, scanner, *thread_info_);
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

    Table_t *table_{nullptr};

    /// the scanned payloads.
    std::vector<Payload> &payloads_;

    /// the position of a current record.
    size_t pos_{0};

    /// the current scan key.
    Key key_{};
  };

  class Scanner
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    Scanner(  //
        const size_t scan_size,
        std::vector<Payload> &payloads)
        : num_remain_(scan_size), payloads_(payloads)
    {
      payloads.clear();
    }

    /*##################################################################################
     * Public utilities
     *################################################################################*/

    template <typename SS, typename K>
    void
    visit_leaf(const SS &, const K &, const threadinfo &)
    {
      // do nothing for single-version scanning
    }

    auto
    visit_value(  //
        Str_t,
        row_type *value,
        threadinfo &)  //
        -> bool
    {
      if (row_is_marker(value)) return true;

      Payload payload{};
      memcpy(&payload, value->col(0).data(), sizeof(Payload));
      payloads_.emplace_back(std::move(payload));

      return (--num_remain_) > 0;
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    int64_t num_remain_{0};

    std::vector<Payload> &payloads_;
  };

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  MasstreeWrapper(  //
      [[maybe_unused]] const size_t gc_interval,
      [[maybe_unused]] const size_t gc_thread_num)
  {
    // assume that a main thread construct this instance
    thread_info_ = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*thread_info_);
  }

  ~MasstreeWrapper() = default;

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  SetUp()
  {
    thread_id_ = thread_counter_.fetch_add(1);
    thread_info_ = threadinfo::make(threadinfo::TI_PROCESS, thread_id_);
  }

  void
  TearDown()
  {
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
    Payload value{};
    auto &&str_val = ToStr(value);
    auto found = index_.run_get1(table_.table(), ToStr(key), 0, str_val, *thread_info_);

    if (found) return value;
    return std::nullopt;
  }

  auto
  Scan(const ScanKey &begin_key = std::nullopt)  //
      -> RecordIterator
  {
    thread_local std::vector<Payload> payloads{kScanSize};

    auto key = (begin_key) ? std::get<0>(*begin_key) : Key{0};
    Scanner scanner{kScanSize, payloads};
    table_.table().scan(ToStr(key), true, scanner, *thread_info_);

    return RecordIterator{&table_, std::move(key), payloads};
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)
  {
    index_.run_replace(table_.table(), ToStr(key), ToStr(value), *thread_info_);
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
  Delete(const Key &key)
  {
    return (index_.run_remove(table_.table(), ToStr(key), *thread_info_)) ? kSuccess : kFailed;
  }

 private:
  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  template <class T>
  static constexpr auto
  ToStr(const T &data)  //
      -> Str_t
  {
    return Str_t{reinterpret_cast<const char *>(&data), sizeof(T)};
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an atomic counter to count the number of worker threads
  static inline std::atomic_size_t thread_counter_ = 0;

  /// information of each worker thread
  static thread_local inline threadinfo *thread_info_ = nullptr;

  /// a thread id for each worker thread
  static thread_local inline size_t thread_id_ = 0;

  Query_t index_{};

  Table_t table_{};
};

template <>
constexpr auto
HasSetUpTearDown<MasstreeWrapper>()  //
    -> bool
{
  return true;
}

#endif  // INDEX_BENCHMARK_INDEXES_MASSTREE_WRAPPER_HPP
