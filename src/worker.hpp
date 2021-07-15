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
#include "operation_generator.hpp"

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
  Index &index_;

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
      Index &index,
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
  }

  ~Worker() = default;

  Worker(const Worker &) = delete;
  Worker &operator=(const Worker &) = delete;
  Worker(Worker &&) = default;
  Worker &operator=(Worker &&) = default;

  /*################################################################################################
   * Public read/write APIs
   *##############################################################################################*/

  constexpr void
  Read(const Key key)
  {
    index_.Read(key);
  }

  constexpr void
  Scan([[maybe_unused]] const Key begin_key, [[maybe_unused]] const Key end_key)
  {
  }

  constexpr void
  Write(const Key key, const Value value)
  {
    index_.Write(key, value);
  }

  constexpr void
  Insert(const Key key, const Value value)
  {
    index_.Insert(key, value);
  }

  constexpr void
  Update(const Key key, const Value value)
  {
    index_.Update(key, value);
  }

  constexpr void
  Delete(const Key key)
  {
    index_.Delete(key);
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Measure and store execution time for each operation.
   *
   */
  constexpr void
  MeasureLatency()
  {
    assert(operation_queue_.size() == operation_counts_);
    assert(exec_times_nano_.empty());

    for (size_t i = 0; i < operation_counts_; ++i) {
      const auto ops = operation_queue_[i];
      const auto start_time = std::chrono::high_resolution_clock::now();
      switch (ops.type) {
        case kRead:
          Read(ops.key);
          break;
        case kScan:
          Scan(ops.key, ops.end_key);
          break;
        case kWrite:
          Write(ops.key, ops.value);
          break;
        case kInsert:
          Insert(ops.key, ops.value);
          break;
        case kUpdate:
          Update(ops.key, ops.value);
          break;
        case kDelete:
          Delete(ops.key);
          break;
        default:
          break;
      }
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
  constexpr void
  MeasureThroughput()
  {
    assert(operation_queue_.size() == operation_counts_);

    const auto start_time = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < operation_counts_; ++i) {
      const auto ops = operation_queue_[i];
      switch (ops.type) {
        case kRead:
          Read(ops.key);
          break;
        case kScan:
          Scan(ops.key, ops.end_key);
          break;
        case kWrite:
          Write(ops.key, ops.value);
          break;
        case kInsert:
          Insert(ops.key, ops.value);
          break;
        case kUpdate:
          Update(ops.key, ops.value);
          break;
        case kDelete:
          Delete(ops.key);
          break;
        default:
          break;
      }
    }
    const auto end_time = std::chrono::high_resolution_clock::now();

    exec_time_nano_ =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
  }

  /**
   * @brief Sort execution time to compute percentiled latency.
   *
   */
  constexpr void
  SortExecutionTimes()
  {
    std::sort(exec_times_nano_.begin(), exec_times_nano_.end());
  }

  /**
   * @param index a target index to get latency
   * @return size_t `index`-th execution time
   */
  constexpr size_t
  GetLatency(const size_t index) const
  {
    return exec_times_nano_[index];
  }

  /**
   * @return size_t total execution time
   */
  constexpr size_t
  GetTotalExecTime() const
  {
    return exec_time_nano_;
  }
};
