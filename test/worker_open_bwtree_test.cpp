// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "worker_open_bwtree.hpp"

#include "gtest/gtest.h"

class WorkerOpenBwTreeFixture : public ::testing::Test
{
  using BwTree_t =
      wangziqi2013::bwtree::BwTree<Key, Value, WorkerKeyComparator, WorkerKeyEqualityChecker>;

 public:
  static constexpr size_t kTotalKeyNum = 1000;
  static constexpr size_t kOperationNum = 100000;
  static constexpr double kSkewParameter = 0;
  static constexpr size_t kRandomSeed = 0;
  static constexpr size_t kOperationTestKeyNum = 100;
  ZipfGenerator zipf_engine{kTotalKeyNum, kSkewParameter};

  std::unique_ptr<WorkerOpenBwTree> worker;

 protected:
  void
  SetUp() override
  {
    Workload workload{100, 0, 0, 0, 0, 0};
    worker = std::make_unique<WorkerOpenBwTree>(zipf_engine, workload, kOperationNum, kRandomSeed);
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

TEST_F(WorkerOpenBwTreeFixture, OperationTest)
{
  // Insert and Read Test
  for (Key i = 0; i < kOperationTestKeyNum; ++i) {
    EXPECT_EQ(0, worker->bwtree_->GetValue(i).size());
    worker->Insert(i, i);
    EXPECT_EQ(1, worker->bwtree_->GetValue(i).size());
  }

  // Scan Test
  BwTree_t::ForwardIterator* tree_iterator = new BwTree_t::ForwardIterator(worker->bwtree_, 0);
  size_t exist_key_num = 0;
  while (tree_iterator->IsEnd() == false) {
    exist_key_num++;
    (*tree_iterator)++;
  }
  EXPECT_EQ(kOperationTestKeyNum, exist_key_num);

  // Delete Test
  for (Key i = 0; i < kOperationTestKeyNum; ++i) {
    worker->Delete(i);
    EXPECT_EQ(0, worker->bwtree_->GetValue(i).size());
  }
}
