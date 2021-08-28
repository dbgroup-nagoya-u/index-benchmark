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
#include <atomic>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "random/zipf.hpp"
#include "worker.hpp"

/*##################################################################################################
 * Global variables
 *################################################################################################*/

/// a mutex to control workers
std::shared_mutex mutex_1st;

/// a mutex to control workers
std::shared_mutex mutex_2nd;

/// a flag to control output format
bool output_format_is_text = true;

/*##################################################################################################
 * Global utility functions
 *################################################################################################*/

/**
 * @brief Log a message to stdout if the output mode is `text`.
 *
 * @param message an output message
 */
void
Log(const char *message)
{
  if (output_format_is_text) {
    std::cout << message << std::endl;
  }
}

/*##################################################################################################
 * Class definition
 *################################################################################################*/

/**
 * @brief A class to run benchmark.
 *
 */
template <class Index>
class IndexBench
{
 private:
  using Worker_t = Worker<Index>;

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  /// the maximum number of elements to compute percentiled latency
  static constexpr size_t kMaxLatencyTargetNum = 1e6;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a benchmarking workload
  const Workload workload_;

  /// the total number of executions
  const size_t total_exec_num_;

  /// the number of worker threads
  const size_t thread_num_;

  /// the total number of keys
  const size_t total_key_num_;

  /// The number of threads for initialization
  const size_t init_thread_num_;

  /// The number of insert operations for initialization
  const size_t init_insert_num_;

  /// a random generator according to Zipf's law
  ZipfGenerator zipf_engine_;

  /// a base random seed
  const size_t random_seed_;

  /// true: throughput, false: latency
  const bool measure_throughput_;

  /// a target index instance
  std::unique_ptr<Index> target_index_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  /**
   * @brief Compute a throughput score and output it to stdout.
   *
   * @param workers worker pointers that hold benchmark results
   */
  void
  LogThroughput(const std::vector<Worker_t *> &workers) const
  {
    size_t avg_nano_time = 0;
    for (auto &&worker : workers) {
      avg_nano_time += worker->GetTotalExecTime();
    }
    avg_nano_time /= thread_num_;

    const auto throughput = total_exec_num_ / (avg_nano_time / 1E9);

    if (output_format_is_text) {
      std::cout << "Throughput [Ops/s]: " << throughput << std::endl;
    } else {
      std::cout << throughput;
    }
  }

  /**
   * @brief Compute percentiled latency and output it to stdout.
   *
   * @param workers worker pointers that hold benchmark results
   */
  void
  LogLatency(const std::vector<Worker_t *> &workers) const
  {
    std::vector<size_t> latencies;
    latencies.reserve(kMaxLatencyTargetNum);

    // sort all execution time
    for (auto &&worker : workers) {
      auto worker_latencies = worker->GetExecTimeVec();
      latencies.insert(latencies.end(), worker_latencies.begin(), worker_latencies.end());
    }
    std::sort(latencies.begin(), latencies.end());

    Log("Percentiled Latencies [ns]:");
    for (double percentile = 0; percentile < 1.01; percentile += 0.05) {
      if (percentile > 0.99) percentile = 0.99;

      const size_t percentiled_idx = latencies.size() * percentile;
      if (output_format_is_text) {
        std::cout << "  " << std::fixed << std::setprecision(2) << percentile << ": ";
      }
      std::cout << latencies[percentiled_idx];
      if (output_format_is_text) {
        std::cout << std::endl;
      } else if (percentile < 0.99) {
        std::cout << ",";
      }
    }
  }

  /**
   * @brief Run a worker thread to measure throuput/latency.
   *
   * @param p a promise of a worker pointer that holds benchmark results
   * @param exec_num the number of operations executed by this worker
   * @param random_seed a random seed
   */
  void
  RunWorker(  //
      std::promise<Worker_t *> p,
      const size_t exec_num,
      const size_t sample_num,
      const size_t random_seed)
  {
    // prepare a worker
    Worker_t *worker;

    {  // create a lock to stop a main thread
      const auto lock = std::shared_lock<std::shared_mutex>(mutex_2nd);
      worker = new Worker_t{target_index_.get(), zipf_engine_, workload_, exec_num, random_seed};
    }  // unlock to notice that this worker has been created

    {  // wait for benchmark to be ready
      const auto guard = std::shared_lock<std::shared_mutex>(mutex_1st);
      if (measure_throughput_) {
        worker->MeasureThroughput();
      } else {
        worker->MeasureLatency();
      }
    }  // unlock to notice that this worker has finished

    if (!measure_throughput_) {
      {  // wait for benchmark to finish
        const auto guard = std::shared_lock<std::shared_mutex>(mutex_2nd);
        worker->SortExecutionTimes(sample_num);
      }  // unlock to notice that this worker has finished
    }

    p.set_value(worker);
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  IndexBench(  //
      const Workload workload,
      const size_t num_exec,
      const size_t num_thread,
      const size_t num_key,
      const size_t num_init_thread,
      const size_t num_init_insert,
      const double skew_parameter,
      const size_t random_seed,
      const bool measure_throughput)
      : workload_{workload},
        total_exec_num_{num_exec},
        thread_num_{num_thread},
        total_key_num_{num_key},
        init_thread_num_{num_init_thread},
        init_insert_num_{num_init_insert},
        zipf_engine_{total_key_num_, skew_parameter},
        random_seed_{random_seed},
        measure_throughput_{measure_throughput}
  {
    target_index_ = std::make_unique<Index>();
  }

  ~IndexBench() = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Run benchmark and output results to stdout.
   *
   */
  void
  Run()
  {
    /*----------------------------------------------------------------------------------------------
     * Preparation of a target index
     *--------------------------------------------------------------------------------------------*/
    std::mt19937_64 rand_engine{random_seed_};
    target_index_->ConstructIndex(init_thread_num_, init_insert_num_);
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
    if constexpr (std::is_same_v<Index, OpenBwTree_t>) {
      // reserve threads for workers and the main
      target_index_->ReserveThreads(thread_num_);
    }
#endif

    /*----------------------------------------------------------------------------------------------
     * Preparation of benchmark workers
     *--------------------------------------------------------------------------------------------*/
    Log("Prepare workers for benchmarking...");

    std::vector<std::future<Worker_t *>> futures;

    {  // create a lock to stop workers from running
      const auto lock = std::unique_lock<std::shared_mutex>(mutex_1st);

      // create workers in each thread
      const size_t lat_arr_size =
          (total_exec_num_ < kMaxLatencyTargetNum) ? total_exec_num_ : kMaxLatencyTargetNum;
      for (size_t i = 0; i < thread_num_; ++i) {
        // distribute operations to each thread so that the number of executions are almost equal
        size_t exec_num = total_exec_num_ / thread_num_;
        size_t sample_num = lat_arr_size / thread_num_;
        if (i == thread_num_ - 1) {
          exec_num = total_exec_num_ - exec_num * (thread_num_ - 1);
          sample_num = lat_arr_size - sample_num * (thread_num_ - 1);
        }

        // create a worker instance in a certain thread
        std::promise<Worker_t *> p;
        futures.emplace_back(p.get_future());
        std::thread{&IndexBench::RunWorker, this, std::move(p), exec_num, sample_num, rand_engine()}
            .detach();
      }

      // wait for all workers to be created
      // std::this_thread::sleep_for(std::chrono::milliseconds(100));
      const auto guard = std::unique_lock<std::shared_mutex>(mutex_2nd);
    }  // unlock to run workers

    /*----------------------------------------------------------------------------------------------
     * Measuring throughput/latency
     *--------------------------------------------------------------------------------------------*/
    if (measure_throughput_) {
      Log("Run workers to measure throughput...");
    } else {
      Log("Run workers to measure latency...");
    }

    {  // create a lock to stop workers from running
      const auto lock = std::unique_lock<std::shared_mutex>(mutex_2nd);

      // wait for all workers to finish measuring throughput
      const auto guard = std::unique_lock<std::shared_mutex>(mutex_1st);
    }  // unlock to run workers

    if (!measure_throughput_) {
      {  // wait for all workers to finish sorting own latency
        const auto guard = std::unique_lock<std::shared_mutex>(mutex_2nd);
      }
    }

    /*----------------------------------------------------------------------------------------------
     * Finalization of benchmark
     *--------------------------------------------------------------------------------------------*/
    Log("Gather benchmark results...\n");

    std::vector<Worker_t *> results;
    results.reserve(thread_num_);
    for (auto &&future : futures) {
      results.emplace_back(future.get());
    }

    if (measure_throughput_) {
      LogThroughput(results);
    } else {
      LogLatency(results);
    }

    for (auto &&worker : results) {
      delete worker;
    }
  }
};
