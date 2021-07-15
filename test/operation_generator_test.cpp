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

#include "operation_generator.hpp"

#include <array>

#include "gtest/gtest.h"

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
