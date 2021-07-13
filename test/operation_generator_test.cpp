// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "operation_generator.hpp"

#include <array>

#include "gtest/gtest.h"
#include "worker_open_bwtree.hpp"

class OperationGeneratorFixture : public ::testing::Test
{
 public:
  // Constants for testing
  static constexpr size_t kRepeatNum = 100000;
  static constexpr size_t kOperationTypeNum = 6;
  static constexpr double kAllowableError = 0.01;
  static constexpr size_t kTotalKeyNum = 10000;
  static constexpr double kSkewParameter = 1.0;

  // Initial workload settings
  static constexpr size_t kReadRatio = 16;
  static constexpr size_t kScanRatio = 32;
  static constexpr size_t kWriteRatio = 48;
  static constexpr size_t kInsertRatio = 64;
  static constexpr size_t kUpdateRatio = 80;
  static constexpr size_t kDeleteRatio = 100;

  std::array<size_t, kOperationTypeNum + 1> op_ratio = {
      0, kReadRatio, kScanRatio, kWriteRatio, kInsertRatio, kUpdateRatio, kDeleteRatio};

  Workload workload{kReadRatio, kScanRatio, kWriteRatio, kInsertRatio, kUpdateRatio, kDeleteRatio};

  ZipfGenerator zipf_engine{kTotalKeyNum, kSkewParameter};

  OperationGenerator op_generator = OperationGenerator{zipf_engine, workload};

 protected:
  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }
};

TEST_F(OperationGeneratorFixture, BracketsOperator_StaticWorkload_ErrRateLessThanAllowableErr)
{
  // generate operations and count occurrences of each operation type
  std::array<size_t, kOperationTypeNum> op_freq = {0, 0, 0, 0, 0, 0};
  for (size_t i = 0; i < kRepeatNum; ++i) {
    ++op_freq[op_generator().type];
  }

  // a workload has percentages like a CDF, so convert it to discrete probability distribution
  std::array<double, kOperationTypeNum> expected_freq;
  for (size_t i = 0; i < kOperationTypeNum; ++i) {
    expected_freq[i] = (op_ratio[i + 1] - op_ratio[i]) * (kRepeatNum / 100.0);
  }

  for (size_t i = 0; i < kOperationTypeNum; ++i) {
    double error_ratio = abs(expected_freq[i] - op_freq[i]) / kRepeatNum;
    EXPECT_LE(error_ratio, kAllowableError);
  }
}
