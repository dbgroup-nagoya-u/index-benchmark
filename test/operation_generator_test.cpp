// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "operation_generator.hpp"

#include <array>

#include "gtest/gtest.h"
#include "worker_open_bwtree.hpp"

class OperationGeneratorFixture : public ::testing::Test
{
 public:
  static constexpr int kRepeatNum = 10000;
  static constexpr int kOperationTypeNum = 6;
  static constexpr double kAllowableError = 0.01;
  static constexpr size_t read_ratio = 16;
  static constexpr size_t scan_ratio = 32;
  static constexpr size_t write_ratio = 48;
  static constexpr size_t insert_ratio = 64;
  static constexpr size_t update_ratio = 80;
  static constexpr size_t delete_ratio = 100;
  std::array<size_t, kOperationTypeNum + 1> op_ratio = {
      0, read_ratio, scan_ratio, write_ratio, insert_ratio, update_ratio, delete_ratio};

  static constexpr size_t total_key_num = 10000;
  static constexpr double skew_parameter = 1.0;
  Workload workload{read_ratio, scan_ratio, write_ratio, insert_ratio, update_ratio, delete_ratio};
  OperationGenerator op_generator = OperationGenerator{workload, total_key_num, skew_parameter};

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
  // Generate operation and count operation type

  std::array<size_t, kOperationTypeNum> op_freq = {0, 0, 0, 0, 0, 0};
  for (int i = 0; i < kRepeatNum; ++i) {
    ++op_freq[op_generator().type];
  }

  // a workload has percentages like a CDF, so convert it to discrete probability distribution
  std::array<double, kOperationTypeNum> expected_freq;
  for (int i = 0; i < kOperationTypeNum; ++i) {
    expected_freq[i] = (op_ratio[i + 1] - op_ratio[i]) * (kRepeatNum / 100.0);
  }

  for (int i = 0; i < kOperationTypeNum; ++i) {
    double error_percentage = abs(expected_freq[i] - op_freq[i]) / kRepeatNum;
    EXPECT_LE(error_percentage, kAllowableError);
  }
}
