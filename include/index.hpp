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

#ifndef INDEX_BENCHMARK_INDEX_HPP_
#define INDEX_BENCHMARK_INDEX_HPP_

// C++ standard libraries
#include <cstddef>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

// external C++ libraries
#include <dbgroup/index/utility.hpp>

// local sources
#include "common.hpp"

namespace dbgroup::index_bench
{
/**
 * @brief A class for dealing with target indexes.
 *
 * @tparam Comp A comparator class for keys.
 * @tparam Implementation A thread-safe index implementation.
 */
template <template <class K, class V, class... Others> class Implementation, class OPEngine>
class Index
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Key = OPEngine::Key;
  using Target = Implementation<Key, Payload>;

 public:
  /*##########################################################################*
   * Public types
   *##########################################################################*/

  using OPType = OPEngine::OPType;
  using Operation = OPEngine::Operation;

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  Index() = default;

  Index(Index&&) noexcept = default;
  auto operator=(Index&&) noexcept -> Index& = default;

  // delete copies
  Index(const Index&) = delete;
  auto operator=(const Index&) -> Index& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~Index() = default;

  /*##########################################################################*
   * APIs for benchmark
   *##########################################################################*/

  void
  SetUpForWorker()
  {
    if constexpr (HasSetUp<Implementation>()) {
      index_->SetUp();
    }
  }

  void
  PreProcess()
  {
    if constexpr (HasPreProcess<Implementation>()) {
      index_->PreProcess();
    }
  }

  auto
  Execute(  //
      const OPType type,
      const Operation& ops)  //
      -> size_t
  {
    constexpr auto kClosed = dbgroup::index::kClosed;
    const auto& [key, key_len, scan_size] = ops;
    size_t count = 1;
    switch (type) {
      case kRead:
        index_->Read(key, key_len);
        break;
      case kScan:
        count = 0;
        for (auto&& iter = index_->Scan(std::make_tuple(key, key_len, kClosed));  //
             iter && count < scan_size;                                           //
             ++iter, ++count) {
          // do nothing
        }
        break;
      case kScanLatest:
        count = 0;
        for (auto&& iter = index_->Scan(); iter && count < scan_size; ++iter, ++count) {
          // do nothing
        }
        break;
      case kWrite:
        index_->Write(key, Payload{}, key_len);
        break;
      case kUpsert:
        index_->Upsert(key, Payload{}, key_len);
        break;
      case kInsert:
        index_->Insert(key, Payload{}, key_len);
        break;
      case kUpdate:
        index_->Update(key, Payload{}, key_len);
        break;
      case kDelete:
        index_->Delete(key, key_len);
        break;
      case kDeleteAndInsert:
        index_->Delete(key, key_len);
        index_->Insert(key, Payload{}, key_len);
        break;
      default:
        throw std::runtime_error{"ERROR: an undefined operation is about to be executed."};
    }

    return count;
  }

  void
  PostProcess()
  {
    if constexpr (HasPostProcess<Implementation>()) {
      index_->PostProcess();
    }
  }

  void
  TearDownForWorker()
  {
    if constexpr (HasTearDown<Implementation>()) {
      index_->TearDown();
    }
  }

  /*##########################################################################*
   * Public APIs
   *##########################################################################*/

  void
  Construct(  //
      const OPEngine& op_engine)
  {
    const auto& [worker_num, use_bulkload, entries] = op_engine.CreateInitData();
    // if constexpr (HasBulkload<Implementation>()) {
    //   if (use_bulkload) {
    //     index_->Bulkload(entries, worker_num);
    //     return;
    //   }
    // }

    std::vector<std::thread> threads{};
    threads.reserve(worker_num);
    for (size_t id = 0, begin_pos = 0; id < worker_num; ++id) {
      const auto n = (entries.size() + id) / worker_num;
      threads.emplace_back(
          [&](const size_t pos, const size_t num) {
            // lambda function to insert key-value pairs in a certain thread
            SetUpForWorker();
            for (size_t i = 0; i < num; ++i) {
              const auto& [key, payload, key_len] = entries[pos + i];
              index_->Write(key, payload, key_len);
            }
            TearDownForWorker();
          },
          begin_pos, n);
      begin_pos += n;
    }
    for (auto&& t : threads) {
      t.join();
    }
  }

  auto
  CheckMemoryUsage()  //
      -> std::pair<size_t, size_t>
  {
    size_t actual_size = 0;
    size_t virtual_size = 0;

    const auto& stat_data = index_->CollectStatisticalData();
    for (size_t level = 0; level < stat_data.size(); ++level) {
      const auto& [node_num, act, vir] = stat_data.at(level);
      actual_size += act;
      virtual_size += vir;

      std::cout << level << "," << node_num << "," << act << "," << vir << std::endl;
    }

    return {actual_size, virtual_size};
  }

 private:
  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief A target implementation.
  std::unique_ptr<Target> index_{std::make_unique<Target>()};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_INDEX_HPP_
