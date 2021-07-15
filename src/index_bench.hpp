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

/*##################################################################################################
 * Target index implementations
 *################################################################################################*/

#include "bztree/bztree.hpp"
using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value>;

#ifdef INDEX_BENCH_BUILD_PTREE
#include "ptree_wrapper.hpp"
using PTree_t = PTreeWrapper<Key, Value>;
#endif

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
template <class Index>
class IndexBench
{
 private:
  using Worker_t = Worker<Index>;

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

  /// The number of insert operations for initialization
  const size_t init_insert_num_;

  /// a random generator according to Zipf's law
  ZipfGenerator zipf_engine_;

  /// a base random seed
  const size_t random_seed_;

  /// true: throughput, false: latency
  const bool measure_throughput_;

  /// a target index instance
  Index target_index_;

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
    size_t lat_0 = std::numeric_limits<size_t>::max(), lat_90, lat_95, lat_99, lat_100;

    // compute the minimum latency and initialize an index list
    std::vector<size_t> indexes;
    indexes.reserve(thread_num_);
    for (size_t thread = 0; thread < thread_num_; ++thread) {
      indexes.emplace_back(total_exec_num_ - 1);
      const auto exec_time = workers[thread]->GetLatency(0);
      if (exec_time < lat_0) {
        lat_0 = exec_time;
      }
    }

    // check latency with descending order
    const size_t total_exec_num = total_exec_num_ * thread_num_;
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
   * @brief Initialize an index for benchmarking.
   *
   * @param random_seed a random seed
   */
  void
  InitializeTargetIndex(const size_t random_seed)
  {
    std::uniform_int_distribution<> uniform_dist{0, static_cast<int>(total_key_num_)};
    std::mt19937_64 rand_engine{random_seed};

    for (size_t i = 0; i < init_insert_num_; ++i) {
      target_index_.Insert(uniform_dist(rand_engine), uniform_dist(rand_engine));
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
      const size_t random_seed)
  {
    // prepare a worker
    Worker_t *worker;

    {  // create a lock to stop a main thread
      const auto lock = std::shared_lock<std::shared_mutex>(mutex_2nd);
      worker = new Worker_t{target_index_, zipf_engine_, workload_, exec_num, random_seed};
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
      const size_t num_init_insert,
      const double skew_parameter,
      const size_t random_seed,
      const bool measure_throughput)
      : workload_{workload},
        total_exec_num_{num_exec},
        thread_num_{num_thread},
        total_key_num_{num_key},
        init_insert_num_{num_init_insert},
        zipf_engine_{total_key_num_, skew_parameter},
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
   */
  void
  Run()
  {
    /*----------------------------------------------------------------------------------------------
     * Preparation of a target index
     *--------------------------------------------------------------------------------------------*/
    std::mt19937_64 rand_engine{random_seed_};
    InitializeTargetIndex(rand_engine());

    /*----------------------------------------------------------------------------------------------
     * Preparation of benchmark workers
     *--------------------------------------------------------------------------------------------*/
    Log("Prepare workers for benchmarking...");

    std::vector<std::future<Worker_t *>> futures;

    {  // create a lock to stop workers from running
      const auto lock = std::unique_lock<std::shared_mutex>(mutex_1st);

      // create workers in each thread
      for (size_t i = 0; i < thread_num_; ++i) {
        // distribute operations to each thread so that the number of executions are almost equal
        size_t exec_num = total_exec_num_ / thread_num_;
        if (i == thread_num_ - 1) {
          exec_num = total_exec_num_ - exec_num * (thread_num_ - 1);
        }

        // create a worker instance in a certain thread
        std::promise<Worker_t *> p;
        futures.emplace_back(p.get_future());
        std::thread{&IndexBench::RunWorker, this, std::move(p), exec_num, rand_engine()}.detach();
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
