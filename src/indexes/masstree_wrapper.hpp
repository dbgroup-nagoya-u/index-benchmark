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

#include <atomic>
#include <optional>
#include <string>
#include <utility>

#include "common.hpp"
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

/*######################################################################################
 * Global variables for Masstree
 *####################################################################################*/

static nodeversion32 global_epoch_lock(false);

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

  using Table_t = Masstree::default_table;
  using Str_t = lcdf::Str;

 public:
  /*####################################################################################
   * Public type aliases
   *##################################################################################*/

  using K = Key;
  using V = Payload;

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  MasstreeWrapper([[maybe_unused]] const size_t worker_num)
  {
    // assume that a main thread construct this instance
    thread_info_ = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*thread_info_);
    thread_info_->rcu_start();
  }

  ~MasstreeWrapper() { thread_info_->rcu_stop(); }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  SetUp()
  {
    thread_id_ = thread_counter_.fetch_add(1);
    thread_info_ = threadinfo::make(threadinfo::TI_PROCESS, thread_id_);
    thread_info_->rcu_start();

    TryRCUQuiesce();
  }

  void
  TearDown()
  {
    thread_info_->rcu_stop();
  }

  constexpr auto
  Bulkload(  //
      [[maybe_unused]] const std::vector<Entry<Key, Payload>> &entries,
      [[maybe_unused]] const size_t thread_num)  //
      -> bool
  {
    return false;
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
    TryRCUQuiesce();

    if (found) return value;
    return std::nullopt;
  }

  void
  Scan(  //
      [[maybe_unused]] const Key &begin_key,
      [[maybe_unused]] const size_t scan_range)
  {
    throw std::runtime_error{"ERROR: the scan operation is not implemented."};
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)  //
      -> int64_t
  {
    // run replace procedure as write (upsert)
    index_.run_replace(table_.table(), ToStr(key), ToStr(value), *thread_info_);
    TryRCUQuiesce();

    return 0;
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)  //
      -> int64_t
  {
    throw std::runtime_error{"ERROR: the insert operation is not implemented."};
    return 1;
  }

  auto
  Update(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)  //
      -> int64_t
  {
    throw std::runtime_error{"ERROR: the update operation is not implemented."};
    return 1;
  }

  auto
  Delete(const Key &key)  //
      -> int64_t
  {
    auto deleted = index_.run_remove(table_.table(), ToStr(key), *thread_info_);
    TryRCUQuiesce();

    return !deleted;
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kGCThresholdMask = (1 << 6) - 1;

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

  void
  SetGlobalEpoch(const mrcu_epoch_type e)
  {
    if (global_epoch_lock.try_lock()) {
      if (mrcu_signed_epoch_type(e - globalepoch) > 0) {
        globalepoch = e;
        active_epoch = threadinfo::min_active_epoch();
      }
      global_epoch_lock.unlock();
    }
  }

  void
  TryRCUQuiesce()
  {
    if ((++ops_counter_ & kGCThresholdMask) == 0) {
      const auto e = timestamp() >> 16;
      if (e != globalepoch) {
        SetGlobalEpoch(e);
      }
      thread_info_->rcu_quiesce();
    }
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

  /// a counter for controling GC
  static thread_local inline size_t ops_counter_ = 0;

  query<row_type> index_{};

  Table_t table_{};
};

#endif  // INDEX_BENCHMARK_INDEXES_MASSTREE_WRAPPER_HPP
