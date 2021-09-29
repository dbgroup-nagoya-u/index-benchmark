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

#include <algorithm>
#include <array>
#include <chrono>
#include <random>
#include <utility>
#include <vector>

#include "common.hpp"
#include "index_wrapper.hpp"
#include "operation_generator.hpp"

/*##################################################################################################
 * Target index implementations
 *################################################################################################*/

#include "bztree/bztree.hpp"
using BzTree_t = IndexWrapper<Key, Value, ::dbgroup::index::bztree::BzTree>;

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
#include "open_bwtree_wrapper.hpp"
using OpenBwTree_t = OpenBwTreeWrapper<Key, Value>;
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE
#include "masstree_wrapper.hpp"
using Masstree_t = MasstreeWrapper<Key, Value>;
#endif

#ifdef INDEX_BENCH_BUILD_PTREE
#include "ptree_wrapper.hpp"
using PTree_t = PTreeWrapper<Key, Value>;
#endif

/*##################################################################################################
 * Class definition
 *################################################################################################*/

/**
 * @brief A class of a worker thread for benchmarking.
 *
 * @tparam Index target index structure
 */
template <class Index>
class Worker
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// the number of operations executed in this worker
  const size_t operation_counts_;

  /// a reference to a target index
  Index *index_;

  /// an operation generator according to a given workload
  OperationGenerator operation_engine_;

  /// a queue that holds index read/write operations
  std::vector<Operation> operation_queue_;

  /// total execution time [ns]
  size_t exec_time_nano_;

  /// execution time for each operation [ns]
  std::vector<size_t> exec_times_nano_;

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  /**
   * @brief Construct a new Worker object for benchmarking.
   *
   * @param workload a workload for benchmarking
   * @param total_key_num the total number of keys
   * @param skew_parameter a Zipf skew parameter
   * @param operation_counts the number of operations executed in each thread
   * @param random_seed a random seed for reproducibility
   */
  Worker(  //
      Index *index,
      ZipfGenerator &zipf_engine,
      const Workload workload,
      const size_t operation_counts,
      const size_t random_seed = 0)
      : operation_counts_{operation_counts},
        index_{index},
        operation_engine_{zipf_engine, workload, random_seed},
        exec_time_nano_{0}
  {
    exec_times_nano_.reserve(operation_counts_);

    // generate an operation-queue for benchmark
    operation_queue_.reserve(operation_counts_);
    for (size_t i = 0; i < operation_counts_; ++i) {
      operation_queue_.emplace_back(operation_engine_());
    }

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
    if constexpr (std::is_same_v<Index, OpenBwTree_t>) {
      index_->RegisterThread();
    }
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
    if constexpr (std::is_same_v<Index, Masstree_t>) {
      index_->RegisterThread();
    }
#endif
  }

  ~Worker()
  {
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
    if constexpr (std::is_same_v<Index, OpenBwTree_t>) {
      index_->UnregisterThread();
    }
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
    if constexpr (std::is_same_v<Index, Masstree_t>) {
      index_->UnregisterThread();
    }
#endif
  }

  Worker(const Worker &) = delete;
  Worker &operator=(const Worker &) = delete;
  Worker(Worker &&) = default;
  Worker &operator=(Worker &&) = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Measure and store execution time for each operation.
   *
   */
  void
  MeasureLatency()
  {
    assert(operation_queue_.size() == operation_counts_);
    assert(exec_times_nano_.empty());

    for (size_t i = 0; i < operation_counts_; ++i) {
      const auto ops = operation_queue_[i];
      const auto start_time = std::chrono::high_resolution_clock::now();
      switch (ops.type) {
        case kRead:
          index_->Read(ops.key);
          break;
        case kScan:
          index_->Scan(ops.key, ops.value);
          break;
        case kWrite:
          index_->Write(ops.key, ops.value);
          break;
        case kInsert:
          index_->Insert(ops.key, ops.value);
          break;
        case kUpdate:
          index_->Update(ops.key, ops.value);
          break;
        case kDelete:
          index_->Delete(ops.key);
          break;
        default:
          break;
      }
#ifdef INDEX_BENCH_BUILD_MASSTREE
      if constexpr (std::is_same_v<Index, Masstree_t>) {
        if ((i & Masstree_t::kGCThresholdMask) == 0) {
          index_->RunGC();
        }
      }
#endif
      const auto end_time = std::chrono::high_resolution_clock::now();
      const auto exec_time =
          std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

      exec_times_nano_.emplace_back(exec_time);
    }
  }

  /**
   * @brief Measure and store total execution time.
   *
   */
  void
  MeasureThroughput()
  {
    assert(operation_queue_.size() == operation_counts_);

    const auto start_time = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < operation_counts_; ++i) {
      const auto ops = operation_queue_[i];
      switch (ops.type) {
        case kRead:
          index_->Read(ops.key);
          break;
        case kScan:
          index_->Scan(ops.key, ops.value);
          break;
        case kWrite:
          index_->Write(ops.key, ops.value);
          break;
        case kInsert:
          index_->Insert(ops.key, ops.value);
          break;
        case kUpdate:
          index_->Update(ops.key, ops.value);
          break;
        case kDelete:
          index_->Delete(ops.key);
          break;
        default:
          break;
      }
#ifdef INDEX_BENCH_BUILD_MASSTREE
      if constexpr (std::is_same_v<Index, Masstree_t>) {
        if ((i & Masstree_t::kGCThresholdMask) == 0) {
          index_->RunGC();
        }
      }
#endif
    }
    const auto end_time = std::chrono::high_resolution_clock::now();

    exec_time_nano_ =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  }

  /**
   * @brief Sort execution time to compute percentiled latency.
   *
   * To reduce computation time, this function performs ramdom samping on the execution
   * time array.
   *
   * @param sample_num the number of samples.
   */
  void
  SortExecutionTimes(const size_t sample_num)
  {
    std::vector<size_t> exec_time_samples;
    exec_time_samples.reserve(sample_num);
    std::uniform_int_distribution<size_t> percent_generator_{0, operation_counts_ - 1};
    std::mt19937_64 rand_engine_{std::random_device{}()};

    // perform random sampling to reduce sorting targets
    for (size_t i = 0; i < sample_num; ++i) {
      const auto sample_idx = percent_generator_(rand_engine_);
      exec_time_samples.emplace_back(exec_times_nano_[sample_idx]);
    }

    // sort sampled execution times
    std::sort(exec_time_samples.begin(), exec_time_samples.end());
    exec_times_nano_ = std::move(exec_time_samples);
  }

  /**
   * @return size_t total execution time
   */
  constexpr size_t
  GetTotalExecTime() const
  {
    return exec_time_nano_;
  }

  std::vector<size_t> &
  GetExecTimeVec()
  {
    return exec_times_nano_;
  }
};
