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
#include <functional>
#include <memory>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

// external C++ libraries
#include <dbgroup/index/concepts.hpp>
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
  using Comp = std::conditional_t<  //
      std::is_same_v<Key, UIntKey>,
      std::less<Key>,
      index::CompareAsCString>;
  using Target = Implementation<Key, Payload, Comp>;

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
    if constexpr (index::HasSetUp<Target>()) {
      index_->SetUp();
    }
  }

  void
  PreProcess()
  {
  }

  auto
  Execute(  //
      const OPType type,
      const Operation& ops)  //
      -> size_t
  {
    constexpr auto kClosed = dbgroup::index::kClosed;
    const auto& [key, key_len, payload, scan_size] = ops;
    size_t count = 1;
    switch (type) {
      case kRead:
        if constexpr (index::HasRead<Target, Key, Payload>()) {
          index_->Read(key, key_len);
        } else {
          throw std::runtime_error{"The read operation is not implemented."};
        }
        break;
      case kScan:
        if constexpr (index::HasScan<Target, Key, Payload>()) {
          count = 0;
          for (auto&& iter = index_->Scan(std::make_tuple(key, key_len, kClosed));  //
               iter && count < scan_size;                                           //
               ++iter, ++count) {
            // do nothing
          }
        } else {
          throw std::runtime_error{"The scan operation is not implemented."};
        }
        break;
      case kScanLatest:
        if constexpr (index::HasScan<Target, Key, Payload>()) {
          count = 0;
          for (auto&& iter = index_->Scan(); iter && count < scan_size; ++iter, ++count) {
            // do nothing
          }
        } else {
          throw std::runtime_error{"The scan (w/o keys) operation is not implemented."};
        }
        break;
      case kWrite:
        if constexpr (index::HasWrite<Target, Key, Payload>()) {
          index_->Write(key, payload, key_len);
        } else {
          throw std::runtime_error{"The write operation is not implemented."};
        }
        break;
      case kUpsert:
        if constexpr (index::HasUpsert<Target, Key, Payload>()) {
          index_->Upsert(key, payload, key_len);
        } else {
          throw std::runtime_error{"The upsert operation is not implemented."};
        }
        break;
      case kInsert:
        if constexpr (index::HasInsert<Target, Key, Payload>()) {
          index_->Insert(key, payload, key_len);
        } else {
          throw std::runtime_error{"The insert operation is not implemented."};
        }
        break;
      case kUpdate:
        if constexpr (index::HasUpdate<Target, Key, Payload>()) {
          index_->Update(key, payload, key_len);
        } else {
          throw std::runtime_error{"The update operation is not implemented."};
        }
        break;
      case kUpdateOrWrite:
        if constexpr (index::HasUpdate<Target, Key, Payload>()) {
          index_->Update(key, payload, key_len);
        } else if constexpr (index::HasWrite<Target, Key, Payload>()) {
          index_->Write(key, payload, key_len);
        } else {
          throw std::runtime_error{"There are no update relevant operations."};
        }
        break;
      case kDelete:
        if constexpr (index::HasDelete<Target, Key, Payload>()) {
          index_->Delete(key, key_len);
        } else {
          throw std::runtime_error{"The delete operation is not implemented."};
        }
        break;
      case kDeleteAndInsert:
        if constexpr (index::HasInsert<Target, Key, Payload>()
                      && index::HasDelete<Target, Key, Payload>()) {
          index_->Delete(key, key_len);
          index_->Insert(key, payload, key_len);
        } else {
          throw std::runtime_error{"The insert/delete operations are not implemented."};
        }
        break;
      default:
        throw std::runtime_error{"ERROR: an undefined operation is about to be executed."};
    }

    return count;
  }

  void
  PostProcess()
  {
  }

  void
  TearDownForWorker()
  {
    if constexpr (index::HasTearDown<Target>()) {
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
    if constexpr (index::HasBulkload<Target, Key, Payload>()) {
      if (use_bulkload) {
        index_->Bulkload(entries, worker_num);
        return;
      }
    }

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
              if constexpr (index::HasWrite<Target, Key, Payload>()) {
                index_->Write(key, payload, key_len);
              } else if constexpr (index::HasInsert<Target, Key, Payload>()) {
                index_->Insert(key, payload, key_len);
              } else if constexpr (index::HasUpsert<Target, Key, Payload>()) {
                index_->Upsert(key, payload, key_len);
              } else {
                throw std::runtime_error{"There are no write relevant operations."};
              }
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
  MemoryUsage()  //
      -> std::pair<size_t, size_t>
  {
    if constexpr (index::HasMemoryUsage<Target>()) {
      return index_->MemoryUsage();
    } else {
      throw std::runtime_error{"The memory usage operation is not implemented."};
    }
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
