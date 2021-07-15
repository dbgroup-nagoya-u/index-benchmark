// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "worker.hpp"

#include "gtest/gtest.h"

/*##################################################################################################
 * Target index implementations
 *################################################################################################*/

#include "bztree/bztree.hpp"
using BzTree_t = ::dbgroup::index::bztree::BzTree<Key, Value>;

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
#include "open_bwtree_wrapper.hpp"
using OpenBwTree_t = OpenBwTreeWrapper<Key, Value>;
#endif

#ifdef INDEX_BENCH_BUILD_PTREE
#include "ptree_wrapper.hpp"
using PTree_t = PTreeWrapper<Key, Value>;
#endif

template <class Index>
class WorkerFixture : public ::testing::Test
{
 protected:
  static constexpr size_t kTotalKeyNum = 2;
  static constexpr size_t kOperationNum = 1000;
  static constexpr double kSkewParameter = 0;
  static constexpr size_t kRandomSeed = 0;

  // a target index instance
  Index index;

  // a random value generator according to Zipf's law
  ZipfGenerator zipf_engine{kTotalKeyNum, kSkewParameter};

  // a worker instance for benchmarking
  std::unique_ptr<Worker<Index>> worker;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    Workload workload{100, 0, 0, 0, 0, 0};
    worker =
        std::make_unique<Worker<Index>>(index, zipf_engine, workload, kOperationNum, kRandomSeed);
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyMeasureThroughput()
  {
    const auto start_time = std::chrono::high_resolution_clock::now();
    worker->MeasureThroughput();
    const auto end_time = std::chrono::high_resolution_clock::now();
    const auto total_time =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

    EXPECT_GT(worker->GetTotalExecTime(), 0);
    EXPECT_LE(worker->GetTotalExecTime(), total_time);
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/
using Indexes = ::testing::Types<BzTree_t
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
                                 ,
                                 OpenBwTree_t
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
                                 ,
                                 PTree_t
#endif
                                 >;
TYPED_TEST_CASE(WorkerFixture, Indexes);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(WorkerFixture, MeasureThroughput_Condition_MeasureReasonableExecutionTime)
{
  TestFixture::VerifyMeasureThroughput();
}
