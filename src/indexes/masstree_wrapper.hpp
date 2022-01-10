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
#include <string>
#include <utility>

#include "../common.hpp"
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

template <class Key, class Value>
class MasstreeWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Table_t = Masstree::default_table;
  using Str_t = lcdf::Str;

 public:
  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  MasstreeWrapper([[maybe_unused]] const size_t worker_num)
  {
    // assume that a main thread construct this instance
    mass_thread_info_ = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*mass_thread_info_);
    mass_thread_info_->rcu_start();
  }

  ~MasstreeWrapper() { mass_thread_info_->rcu_stop(); }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  void
  SetUp()
  {
    mass_thread_id_ = mass_thread_counter_.fetch_add(1);
    mass_thread_info_ = threadinfo::make(threadinfo::TI_PROCESS, mass_thread_id_);
    mass_thread_info_->rcu_start();

    RunGC();
  }

  void
  TearDown()
  {
    mass_thread_info_->rcu_stop();
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const Key key)  //
      -> std::pair<int64_t, Value>
  {
    auto &&str_key = quick_istr{key}.string();

    Str_t payload{};
    auto found_key = masstree_.run_get1(table_.table(), str_key, 0, payload, *mass_thread_info_);

    RunGC();
    return {!found_key, payload.to_i()};
  }

  void
  Scan(  //
      [[maybe_unused]] const Key begin_key,
      [[maybe_unused]] const Key scan_range)
  {
    // this operation is not implemented
    assert(false);
  }

  auto
  Write(  //
      const Key key,
      const Value value)  //
      -> int64_t
  {
    // unique quick_istr converters are required for each key/value
    quick_istr key_conv{key}, val_conv{value};

    // run replace procedure as write (upsert)
    masstree_.run_replace(table_.table(), key_conv.string(), val_conv.string(), *mass_thread_info_);

    RunGC();
    return 0;
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)  //
      -> int64_t
  {
    // this operation is not implemented
    assert(false);
    return 1;
  }

  auto
  Update(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)  //
      -> int64_t
  {
    // this operation is not implemented
    assert(false);
    return 1;
  }

  auto
  Delete(const Key key)  //
      -> int64_t
  {
    const auto str_key = quick_istr{key}.string();
    auto deleted = masstree_.run_remove(table_.table(), str_key, *mass_thread_info_);

    RunGC();
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

  void
  SetGlobalEpoch(const mrcu_epoch_type e)
  {
    global_epoch_lock.lock();
    if (mrcu_signed_epoch_type(e - globalepoch) > 0) {
      globalepoch = e;
      active_epoch = threadinfo::min_active_epoch();
    }
    global_epoch_lock.unlock();
  }

  void
  RunGC()
  {
    if ((ops_counter_++ & kGCThresholdMask) == 0) {
      const auto e = timestamp() >> 16;
      if (e != globalepoch) SetGlobalEpoch(e);
      mass_thread_info_->rcu_quiesce();
    }
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// an atomic counter to count the number of worker threads
  static inline std::atomic_size_t mass_thread_counter_ = 0;

  /// information of each worker thread
  static thread_local inline threadinfo *mass_thread_info_ = nullptr;

  /// a thread id for each worker thread
  static thread_local inline size_t mass_thread_id_ = 0;

  /// a counter for controling GC
  static thread_local inline size_t ops_counter_ = 0;

  query<row_type> masstree_{};

  Table_t table_{};
};

#endif  // INDEX_BENCHMARK_INDEXES_MASSTREE_WRAPPER_HPP
