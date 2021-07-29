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

#pragma once

#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../external/masstree/clp.h"
#include "../external/masstree/config.h"
#include "../external/masstree/json.hh"
#include "../external/masstree/kvio.hh"
#include "../external/masstree/kvrandom.hh"
#include "../external/masstree/kvrow.hh"
#include "../external/masstree/kvstats.hh"
#include "../external/masstree/kvtest.hh"
#include "../external/masstree/masstree_insert.hh"
#include "../external/masstree/masstree_remove.hh"
#include "../external/masstree/masstree_scan.hh"
#include "../external/masstree/masstree_tcursor.hh"
#include "../external/masstree/nodeversion.hh"
#include "../external/masstree/query_masstree.hh"
#include "../external/masstree/timestamp.hh"
#include "common.hpp"

/*##################################################################################################
 * Global variables
 *################################################################################################*/

// global epoch, updated by main thread regularly
volatile mrcu_epoch_type globalepoch = 1;

volatile mrcu_epoch_type active_epoch = 1;

kvepoch_t global_log_epoch = 0;

// don't add log entries, and free old value immediately
volatile bool recovering = false;

kvtimestamp_t initial_timestamp;

std::atomic_size_t mass_thread_counter = 0;

/*##################################################################################################
 * Global thread-local storages
 *################################################################################################*/

thread_local threadinfo* mass_thread_info;

thread_local size_t mass_thread_id;

template <class Key, class Value>
class MasstreeWrapper
{
  using Table_t = Masstree::default_table;
  using Str_t = lcdf::Str;

 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  query<row_type> masstree_;

  Table_t table_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  MasstreeWrapper() : table_{}
  {
    // assume that a main thread construct this instance
    mass_thread_info = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*mass_thread_info);
  }

  ~MasstreeWrapper() = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  ConstructIndex(  //
      const size_t thread_num,
      const size_t insert_num)
  {
    const size_t insert_num_per_thread = insert_num / thread_num;

    // lambda function to insert key-value pairs in a certain thread
    auto f = [&](const size_t begin, const size_t end) {
      const auto thread_id = mass_thread_counter.fetch_add(1);
      mass_thread_info = threadinfo::make(threadinfo::TI_PROCESS, thread_id);

      for (size_t i = begin; i < end; ++i) {
        this->Write(i, i);
      }
    };

    // insert initial key-value pairs in multi-threads
    std::vector<std::thread> threads;
    auto begin = 0UL, end = insert_num_per_thread;
    for (size_t i = 0; i < thread_num; ++i) {
      if (i == thread_num - 1) {
        end = insert_num;
      }
      threads.emplace_back(f, begin, end);
      begin = end;
      end += insert_num_per_thread;
    }
    for (auto&& t : threads) t.join();
  }

  void
  RegisterThread()
  {
    mass_thread_id = mass_thread_counter.fetch_add(1);
    mass_thread_info = threadinfo::make(threadinfo::TI_PROCESS, mass_thread_id);
  }

  /*################################################################################################
   * Public read/write APIs
   *##############################################################################################*/

  std::pair<int64_t, Value>
  Read(const Key key)
  {
    const auto str_key = quick_istr{key}.string();

    Str_t payload;
    auto found_key = masstree_.run_get1(table_.table(), str_key, 0, payload, *mass_thread_info);

    return {!found_key, payload.to_i()};
  }

  void
  Scan(  //
      [[maybe_unused]] const Key begin_key,
      [[maybe_unused]] const Key scan_range)
  {
    // begin-end scan is not implemented in Masstree
  }

  int64_t
  Write(  //
      const Key key,
      const Value value)
  {
    // unique quick_istr converters are required for each key/value
    quick_istr key_conv{key}, val_conv{value};

    // run replace procedure as write (upsert)
    masstree_.run_replace(table_.table(), key_conv.string(), val_conv.string(), *mass_thread_info);

    return 0;
  }

  int64_t
  Insert(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
    // an insert operation is not implemented in Masstree
    assert(false);
    return 0;
  }

  int64_t
  Update(  //
      [[maybe_unused]] const Key key,
      [[maybe_unused]] const Value value)
  {
    // an update operation is not implemented in Masstree
    assert(false);
    return 0;
  }

  int64_t
  Delete(const Key key)
  {
    const auto str_key = quick_istr{key}.string();
    auto deleted = masstree_.run_remove(table_.table(), str_key, *mass_thread_info);
    return !deleted;
  }
};
