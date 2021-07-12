// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <future>
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
#include "worker_bztree.hpp"
#include "worker_open_bwtree.hpp"

#ifdef INDEX_BENCH_BUILD_PTREE
#include "worker_ptree.hpp"
using PTree = pam_map<ptree_entry<Key, Value>>;
#endif

using NUBzTree = ::dbgroup::index::bztree::BzTree<Key, Value>;

/// temporal
constexpr size_t kInitialTreeSize = 1000000;

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
 * Utility Classes
 *################################################################################################*/

/**
 * @brief A class to run benchmark.
 *
 */
class IndexBench
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a ratio of read operations
  const Workload workload_;

  /// the number of operations executed in each thread
  const size_t exec_num_;

  /// the number of execution threads
  const size_t thread_num_;

  /// the number of total keys
  const size_t total_key_num_;

  /// a skew parameter
  const double skew_parameter_;

  /// a base random seed
  const size_t random_seed_;

  /// true: throughput, false: latency
  const bool measure_throughput_;

  /*################################################################################################
   * Internal utility functions
   *##############################################################################################*/

  /**
   * @brief Compute a throughput score and output it to stdout.
   *
   * @param workers worker pointers that hold benchmark results
   */
  void
  LogThroughput(const std::vector<Worker *> &workers) const
  {
    size_t avg_nano_time = 0;
    for (auto &&worker : workers) {
      avg_nano_time += worker->GetTotalExecTime();
    }
    avg_nano_time /= thread_num_;

    const size_t total_exec_num = exec_num_ * thread_num_;
    const auto throughput = total_exec_num / (avg_nano_time / 1E9);

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
  LogLatency(const std::vector<Worker *> &workers) const
  {
    size_t lat_0 = std::numeric_limits<size_t>::max(), lat_90, lat_95, lat_99, lat_100;

    // compute the minimum latency and initialize an index list
    std::vector<size_t> indexes;
    indexes.reserve(thread_num_);
    for (size_t thread = 0; thread < thread_num_; ++thread) {
      indexes.emplace_back(exec_num_ - 1);
      const auto exec_time = workers[thread]->GetLatency(0);
      if (exec_time < lat_0) {
        lat_0 = exec_time;
      }
    }

    // check latency with descending order
    const size_t total_exec_num = exec_num_ * thread_num_;
    for (size_t count = total_exec_num; count >= total_exec_num * 0.90; --count) {
      size_t target_thread = 0;
      auto max_exec_time = std::numeric_limits<size_t>::min();
      for (size_t thread = 0; thread < thread_num_; ++thread) {
        const auto exec_time = workers[thread]->GetLatency(indexes[thread]);
        if (exec_time > max_exec_time) {
          max_exec_time = exec_time;
          target_thread = thread;
        }
      }

      // if `count` reaches target percentiles, store its latency
      if (count == total_exec_num) {
        lat_100 = max_exec_time;
      } else if (count == static_cast<size_t>(total_exec_num * 0.99)) {
        lat_99 = max_exec_time;
      } else if (count == static_cast<size_t>(total_exec_num * 0.95)) {
        lat_95 = max_exec_time;
      } else if (count == static_cast<size_t>(total_exec_num * 0.90)) {
        lat_90 = max_exec_time;
      }

      --indexes[target_thread];
    }

    Log("Percentiled Latencies [ns]:");
    if (output_format_is_text) {
      std::cout << "  MIN: " << lat_0 << std::endl;
      std::cout << "  90%: " << lat_90 << std::endl;
      std::cout << "  95%: " << lat_95 << std::endl;
      std::cout << "  99%: " << lat_99 << std::endl;
      std::cout << "  MAX: " << lat_100 << std::endl;
    } else {
      std::cout << lat_0 << "," << lat_90 << "," << lat_95 << "," << lat_99 << "," << lat_100;
    }
  }

  /**
   * @brief Create and initialize an index for benchmarking.
   *
   * @param target a constant to represent a target implementation
   * @param random_seed a random seed
   * @return void* a pointer to a created index
   */
  void *
  CreateTargetIndex(  //
      const BenchTarget target,
      const size_t random_seed)
  {
    ZipfGenerator zipf_engine{total_key_num_, skew_parameter_, random_seed};

    switch (target) {
      case kOpenBwTree:
        return nullptr;
      case kBzTree: {
        auto index = new NUBzTree{};
        for (size_t i = 0; i < kInitialTreeSize; ++i) {
          index->Insert(zipf_engine(), i);
        }
        return index;
      }
      #ifdef INDEX_BENCH_BUILD_PTREE
      case kPTree: {
        auto index = new PTree;
        for (size_t i = 0; i < kInitialTreeSize; ++i) {
          index->insert(std::make_pair(zipf_engine(), i));
        }
        return index;
      }
      #endif
      default:
        return nullptr;
    }
  }

  /**
   * @brief Delete a target index according to its implementation.
   *
   * @param target a constant to represent a target implementation
   * @param target_index a target index to delete
   */
  void
  DeleteTargetIndex(  //
      const BenchTarget target,
      void *target_index)
  {
    switch (target) {
      case kOpenBwTree:
        break;
      case kBzTree:
        delete reinterpret_cast<NUBzTree *>(target_index);
        break;
      #ifdef INDEX_BENCH_BUILD_PTREE
      case kPTree:
        delete reinterpret_cast<PTree *>(target_index);
        break;
      #endif
      default:
        break;
    }
  }

  /**
   * @brief Create a worker to run benchmark for a given target implementation.
   *
   * @param target a target implementation
   * @param random_seed a random seed
   * @return Worker* a created worker
   */
  Worker *
  CreateWorker(  //
      const BenchTarget target,
      void *target_index,
      const size_t random_seed)
  {
    switch (target) {
      case kOpenBwTree:
        return new WorkerOpenBwTree{workload_, total_key_num_, skew_parameter_, exec_num_,
                                    random_seed};
      case kBzTree:
        return new WorkerBzTree{target_index,    workload_, total_key_num_,
                                skew_parameter_, exec_num_, random_seed};
      #ifdef INDEX_BENCH_BUILD_PTREE
      case kPTree:
        return new WorkerPTree{target_index,    workload_, total_key_num_,
                               skew_parameter_, exec_num_, random_seed};
      #endif
      default:
        return nullptr;
    }
  }

  /**
   * @brief Run a worker thread to measure throuput/latency.
   *
   * @param p a promise of a worker pointer that holds benchmark results
   * @param target a target implementation
   * @param random_seed a random seed
   */
  void
  RunWorker(  //
      std::promise<Worker *> p,
      const BenchTarget target,
      void *target_index,
      const size_t random_seed)
  {
    // prepare a worker
    Worker *worker;

    {  // create a lock to stop a main thread
      const auto lock = std::shared_lock<std::shared_mutex>(mutex_2nd);
      worker = CreateWorker(target, target_index, random_seed);
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
        worker->SortExecutionTimes();
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
      const double skew_parameter,
      const size_t random_seed,
      const bool measure_throughput)
      : workload_{workload},
        exec_num_{num_exec},
        thread_num_{num_thread},
        total_key_num_{num_key},
        skew_parameter_{skew_parameter},
        random_seed_{random_seed},
        measure_throughput_{measure_throughput}
  {
  }

  ~IndexBench() = default;

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  /**
   * @brief Run benchmark and output results to stdout.
   *
   * @param target a target implementation
   */
  void
  Run(const BenchTarget target)
  {
    /*----------------------------------------------------------------------------------------------
     * Preparation of a target index
     *--------------------------------------------------------------------------------------------*/
    std::mt19937_64 rand_engine{random_seed_};
    auto target_index = CreateTargetIndex(target, rand_engine());

    /*----------------------------------------------------------------------------------------------
     * Preparation of benchmark workers
     *--------------------------------------------------------------------------------------------*/
    std::vector<std::future<Worker *>> futures;

    {  // create a lock to stop workers from running
      const auto lock = std::unique_lock<std::shared_mutex>(mutex_1st);

      // create workers in each thread
      for (size_t index = 0; index < thread_num_; ++index) {
        std::promise<Worker *> p;
        futures.emplace_back(p.get_future());
        std::thread{&IndexBench::RunWorker, this, std::move(p), target, target_index, rand_engine()}
            .detach();
      }

      // wait for all workers to be created
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
    Log("Gather benchmark results...");

    std::vector<Worker *> results;
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
    DeleteTargetIndex(target, target_index);
  }
};
