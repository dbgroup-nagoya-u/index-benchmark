// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include <gflags/gflags.h>
#include <pmwcas.h>

#include <future>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <thread>

#include "worker.hpp"
#include "worker_cas.hpp"
#include "worker_mwcas.hpp"
#include "worker_pmwcas.hpp"

static bool
ValidatePositiveVal(const char *flagname, uint64_t value)
{
  if (value > 0) {
    return true;
  }
  std::cout << "A value must be positive for " << flagname << std::endl;
  return false;
}

static bool
ValidateTargetNum([[maybe_unused]] const char *flagname, uint64_t value)
{
  if (value > 0 && value <= kMaxTargetNum) {
    return true;
  }
  std::cout << "The number of MwCAS targets must be between [1, " << kMaxTargetNum << "]"
            << std::endl;
  return false;
}

// set command line options
DEFINE_uint64(read_ratio, 0, "The ratio of MwCAS read operations [%]");
DEFINE_uint64(num_exec, 10000, "The number of MwCAS operations executed in each thread");
DEFINE_validator(num_exec, &ValidatePositiveVal);
DEFINE_uint64(num_loop, 1, "The number of loops to measure performance");
DEFINE_validator(num_loop, &ValidatePositiveVal);
DEFINE_uint64(num_thread, 1, "The number of execution threads");
DEFINE_validator(num_thread, &ValidatePositiveVal);
DEFINE_uint64(num_shared, 10000, "The number of total target fields");
DEFINE_validator(num_shared, &ValidatePositiveVal);
DEFINE_uint64(num_target, 2, "The number of target fields for each MwCAS");
DEFINE_validator(num_target, &ValidateTargetNum);
DEFINE_bool(ours, true, "Use MwCAS library (DB Group @ Nagoya Univ.) as a benchmark target");
DEFINE_bool(microsoft, false, "Use PMwCAS library (Microsoft) as a benchmark target");
DEFINE_bool(single, false, "Use Single CAS as a benchmark target");
DEFINE_bool(csv, false, "Output benchmark results as CSV format");
DEFINE_bool(throughput, true, "Measure throughput");
DEFINE_bool(latency, true, "Measure latency");

/*##################################################################################################
 * Global variables
 *################################################################################################*/

/// a mutex to trigger workers simultaneously
std::shared_mutex mutex_1st;

/// a mutex to wait until all workers have been created
std::shared_mutex mutex_2nd;

/// a flag to control output format
bool output_format_is_text = true;

/*##################################################################################################
 * Global utility functions
 *################################################################################################*/

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
 * @brief A class to run MwCAS benchmark.
 *
 */
class MwCASBench
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  /// a ratio of read operations
  const size_t read_ratio_;

  /// the number of MwCAS operations executed in each thread
  const size_t num_exec_;

  /// the number of loops to measure performance
  const size_t num_loop_;

  /// the number of execution threads
  const size_t num_thread_;

  /// the number of total target fields
  const size_t num_shared_;

  /// the number of target fields for each MwCAS
  const size_t num_target_;

  /// a flag to measure throughput
  const bool measure_throughput_;

  /// a flag to measure latency
  const bool measure_latency_;

  /// target fields of MwCAS
  size_t *shared_fields_;

  /// PMwCAS descriptor pool
  pmwcas::DescriptorPool *desc_pool_;

  /*################################################################################################
   * Private utility functions
   *##############################################################################################*/

  void
  LogThroughput(const double throughput) const
  {
    if (output_format_is_text) {
      std::cout << "Throughput [Ops/s]: " << throughput << std::endl;
    } else {
      std::cout << throughput;
    }
  }

  void
  LogLatency(  //
      const size_t lat_0,
      const size_t lat_90,
      const size_t lat_95,
      const size_t lat_99,
      const size_t lat_100) const
  {
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

  void
  InitializeSharedFields()
  {
    assert(shared_fields_);

    for (size_t index = 0; index < num_shared_; ++index) {
      shared_fields_[index] = 0;
    }
  }

  Worker *
  CreateWorker(  //
      const BenchTarget target,
      const size_t random_seed)
  {
    switch (target) {
      case kOurs:
        return new WorkerMwCAS{shared_fields_, num_shared_, num_target_, read_ratio_,
                               num_exec_,      num_loop_,   random_seed};
      case kMicrosoft:
        return new WorkerPMwCAS{*desc_pool_, shared_fields_, num_shared_, num_target_,
                                read_ratio_, num_exec_,      num_loop_,   random_seed};
      case kSingleCAS:
        return new WorkerSingleCAS{shared_fields_, num_shared_, num_target_, read_ratio_,
                                   num_exec_,      num_loop_,   random_seed};
      default:
        return nullptr;
    }
  }

  void
  RunWorker(  //
      std::promise<Worker *> p,
      const BenchTarget target,
      const size_t random_seed)
  {
    // prepare a worker
    Worker *worker;
    {
      // create a lock to stop a main thread
      const auto lock = std::shared_lock<std::shared_mutex>(mutex_2nd);
      worker = CreateWorker(target, random_seed);

      // unlock to notice that worker has been created
    }
    {
      // wait for benchmark to be ready
      const auto guard = std::shared_lock<std::shared_mutex>(mutex_1st);
      if (measure_throughput_) worker->MeasureThroughput();

      // unlock to notice that worker has measured thuroughput
    }
    {
      // wait for benchmark to be ready
      const auto guard = std::shared_lock<std::shared_mutex>(mutex_2nd);
      if (measure_latency_) worker->MeasureLatency();

      // unlock to notice that worker has measured latency
    }
    {
      // wait for benchmark to be ready
      const auto guard = std::shared_lock<std::shared_mutex>(mutex_1st);
      worker->SortExecutionTimes();
    }

    p.set_value(worker);
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  MwCASBench()
      : read_ratio_{FLAGS_read_ratio},
        num_exec_{FLAGS_num_exec},
        num_loop_{FLAGS_num_loop},
        num_thread_{FLAGS_num_thread},
        num_shared_{FLAGS_num_shared},
        num_target_{FLAGS_num_target},
        measure_throughput_{FLAGS_throughput},
        measure_latency_{FLAGS_latency}
  {
    // prepare PMwCAS descriptor pool
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create, pmwcas::DefaultAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create, pmwcas::LinuxEnvironment::Destroy);
    desc_pool_ = new pmwcas::DescriptorPool{static_cast<uint32_t>(8192 * num_thread_),
                                            static_cast<uint32_t>(num_thread_)};

    // prepare shared target fields
    shared_fields_ = new size_t[num_shared_];
    InitializeSharedFields();
  }

  ~MwCASBench()
  {
    delete shared_fields_;
    delete desc_pool_;
  }

  /*################################################################################################
   * Public utility functions
   *##############################################################################################*/

  void
  RunMwCASBench(const BenchTarget target)
  {
    // prepare workers
    std::vector<std::future<Worker *>> futures;
    {
      // create a lock to stop workers from running
      const auto lock = std::unique_lock<std::shared_mutex>(mutex_1st);

      // create workers in each thread
      std::mt19937_64 rand_engine{0};
      for (size_t index = 0; index < num_thread_; ++index) {
        std::promise<Worker *> p;
        futures.emplace_back(p.get_future());
        std::thread{&MwCASBench::RunWorker, this, std::move(p), target, rand_engine()}.detach();
      }

      // wait for all workers to be created
      const auto guard = std::unique_lock<std::shared_mutex>(mutex_2nd);

      InitializeSharedFields();

      // unlock to run workers
    }

    if (measure_throughput_) Log("Run workers to measure throughput...");

    {
      // create a lock to stop workers from running
      const auto lock = std::unique_lock<std::shared_mutex>(mutex_2nd);

      // wait for all workers to finish measuring throughput
      const auto guard = std::unique_lock<std::shared_mutex>(mutex_1st);

      InitializeSharedFields();

      // unlock to run workers
    }

    if (measure_latency_) Log("Run workers to measure latency...");

    {
      // create a lock to stop workers from running
      const auto lock = std::unique_lock<std::shared_mutex>(mutex_1st);

      // wait for all workers to finish measuring latency
      const auto guard = std::unique_lock<std::shared_mutex>(mutex_2nd);
    }

    Log("Finish running...");

    // gather results
    std::vector<Worker *> results;
    results.reserve(num_thread_);
    for (auto &&future : futures) {
      results.emplace_back(future.get());
    }

    const size_t total_exec_num = num_exec_ * num_loop_ * num_thread_;
    if (measure_throughput_) {
      // compute throughput
      size_t avg_nano_time = 0;
      for (auto &&worker : results) {
        avg_nano_time += worker->GetTotalExecTime();
      }
      avg_nano_time /= num_thread_;
      const auto throughput = total_exec_num / (avg_nano_time / 1E9);

      LogThroughput(throughput);
    }

    if (measure_latency_) {
      // compute latency
      size_t lat_0 = std::numeric_limits<size_t>::max(), lat_90, lat_95, lat_99, lat_100;
      std::vector<size_t> indexes;
      indexes.reserve(num_thread_);
      for (size_t thread = 0; thread < num_thread_; ++thread) {
        indexes.emplace_back(num_exec_ * num_loop_ - 1);
        const auto exec_time = results[thread]->GetLatency(0);
        if (exec_time < lat_0) {
          lat_0 = exec_time;
        }
      }
      for (size_t count = total_exec_num; count >= total_exec_num * 0.90; --count) {
        int64_t target_thread = -1;
        auto max_exec_time = std::numeric_limits<size_t>::min();
        for (size_t thread = 0; thread < num_thread_; ++thread) {
          const auto exec_time = results[thread]->GetLatency(indexes[thread]);
          if (exec_time > max_exec_time) {
            max_exec_time = exec_time;
            target_thread = thread;
          }
        }
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
      LogLatency(lat_0, lat_90, lat_95, lat_99, lat_100);
    }

    for (auto &&worker : results) {
      delete worker;
    }
  }
};

/*##################################################################################################
 * Main function
 *################################################################################################*/

int
main(int argc, char *argv[])
{
  // parse command line options
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  output_format_is_text = !FLAGS_csv;

  Log("=== Start MwCAS Benchmark ===");
  auto bench = MwCASBench{};
  if (FLAGS_ours) {
    Log("** Run our MwCAS...");
    bench.RunMwCASBench(BenchTarget::kOurs);
    Log("** Finish.");
  }
  if (FLAGS_microsoft) {
    Log("** Run Microsoft's PMwCAS...");
    bench.RunMwCASBench(BenchTarget::kMicrosoft);
    Log("** Finish.");
  }
  if (FLAGS_single) {
    Log("** Run Single CAS...");
    bench.RunMwCASBench(BenchTarget::kSingleCAS);
    Log("** Finish.");
  }
  Log("==== End MwCAS Benchmark ====");

  return 0;
}
