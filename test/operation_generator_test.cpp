// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "operation_generator.hpp"

#include "gtest/gtest.h"
#include "worker_open_bwtree.hpp"

class OperationGeneratorFixture : public ::testing::Test
{
 public:
  static constexpr size_t kOperationNum = 10000;
  static constexpr double kAllowableError = 0.01;
  const size_t read_ratio = 16;
  const size_t scan_ratio = 32;
  const size_t write_ratio = 48;
  const size_t insert_ratio = 64;
  const size_t update_ratio = 80;
  const size_t delete_ratio = 100;
  size_t op_ratio[6] = {read_ratio,   scan_ratio,   write_ratio,
                        insert_ratio, update_ratio, delete_ratio};
  const size_t total_key_num = 10000;
  double skew_parameter = 1.0;
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
  for (size_t i = 0; i < kOperationNum; ++i) {
    ++op_freq[op_generator().type];
  }

  // a workload has percentages like a CDF, so convert it to discrete probability distribution
  std::array<double, kOperationTypeNum> expected_freq;
  for (size_t i = 0; i < kOperationTypeNum; ++i) {
    if (i == 0) {
      expected_freq[i] = (op_ratio[i] - 0) * (kOperationTypeNum / 100.0);
    } else {
      expected_freq[i] = (op_ratio[i] - op_ratio[i - 1]) * (kOperationTypeNum / 100.0);
    }
  }

  for (int i = 0; i < 6; i++) {
    double error_percentage = abs(expected_freq[i] - op_freq[i]) / OPERATION_NUM;
    EXPECT_LE(error_percentage, kAllowableError);
  }
}
