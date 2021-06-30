// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "worker_open_bwtree.hpp"

#include "gtest/gtest.h"

class WorkerOpenBwTreeFixture : public ::testing::Test
{
 public:
  static constexpr size_t kTotalKeyNum = 1000;
  static constexpr size_t kOperationNum = 100000;
  static constexpr double kSkewParameter = 0;
  static constexpr size_t kRandomSeed = 0;

  ZipfGenerator zipf_engine{kTotalKeyNum, kSkewParameter};

  std::unique_ptr<WorkerOpenBwTree> worker;

 protected:
  void
  SetUp() override
  {
<<<<<<< HEAD
    Workload workload{30, 40, 50, 60, 90, 100};
    worker = std::make_unique<WorkerOpenBwTree>(workload, kTotalKeyNum, kSkewParameter,
                                                kOperationNum, kRandomSeed);
=======
    Workload workload{100, 0, 0, 0, 0, 0};
    worker = std::make_unique<WorkerOpenBwTree>(zipf_engine, workload, kOperationNum, kRandomSeed);
>>>>>>> main
  }

  void
  TearDown() override
  {
  }
};

TEST_F(WorkerOpenBwTreeFixture, MeasureThroughput_Condition_MeasureReasonableExecutionTime)
{
  const auto start_time = std::chrono::high_resolution_clock::now();
  worker->MeasureThroughput();
  const auto end_time = std::chrono::high_resolution_clock::now();
  const auto total_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

  EXPECT_GE(worker->GetTotalExecTime(), 0);
  EXPECT_LE(worker->GetTotalExecTime(), total_time);
}
