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

#include "workload/workload.hpp"

#include <vector>

#include "gtest/gtest.h"
#include "workload/operation.hpp"

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr size_t kThreadNum = 8;
constexpr size_t kDefaultKeyNum = 1E6;
constexpr size_t kRepeatNum = 1E7;
constexpr double kAllowableError = 0.05;
constexpr size_t kRandomSeed = 20;

/*######################################################################################
 * Fixture class definition
 *####################################################################################*/

class WorkloadFixture : public ::testing::Test
{
 public:
  using Key_t = Key<k8>;
  using Payload_t = uint64_t;
  using Operation_t = Operation<Key_t, Payload_t>;
  using Json_t = ::nlohmann::json;

 protected:
  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  /*####################################################################################
   * Utility functions
   *##################################################################################*/

  auto
  PrepareOperationVector()  //
      -> std::vector<Operation_t>
  {
    std::vector<Operation_t> operations{};
    operations.reserve(kRepeatNum);
    return operations;
  }
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TEST_F(WorkloadFixture, ConstructorWOArgsGenerateReadOnlyWorkload)
{  //
  Workload workload{};

  EXPECT_EQ(workload.GetKeyNum(), kDefaultKeyNum);
  EXPECT_EQ(workload.GetExecutionRatio(), 1.0);

  auto &&operations = PrepareOperationVector();
  workload.AddOperations(operations, kRepeatNum, 0, 1, kRandomSeed);

  std::vector<size_t> frequency(kDefaultKeyNum, 0);
  for (const auto &ops : operations) {
    EXPECT_EQ(ops.type, kRead);
    ++(frequency.at(ops.key));
  }
  std::sort(frequency.begin(), frequency.end());
  const auto err = (frequency.back() - frequency.front()) / static_cast<double>(kRepeatNum);
  EXPECT_LT(err, kAllowableError);
}

TEST_F(WorkloadFixture, WorkloadHavingAllOperationsGenerateOperationsUniformly)
{  //
  constexpr size_t kOpsTypeNum = 10;
  constexpr auto kOpsRatio = 1.0 / kOpsTypeNum;
  Json_t w_json = {{"operation ratios",
                    {{"read", kOpsRatio},
                     {"scan", kOpsRatio},
                     {"write", kOpsRatio},
                     {"insert", kOpsRatio},
                     {"update", kOpsRatio},
                     {"delete", kOpsRatio},
                     {"insert or update", kOpsRatio},
                     {"delete and insert", kOpsRatio},
                     {"delete or insert", kOpsRatio},
                     {"insert and delete", kOpsRatio}}},
                   {"# of keys", kDefaultKeyNum},
                   {"partitioning policy", "none"},
                   {"access pattern", "random"},
                   {"scan length", 100}};

  Workload workload{w_json};

  EXPECT_EQ(workload.GetKeyNum(), kDefaultKeyNum);
  EXPECT_EQ(workload.GetExecutionRatio(), 1.0);

  auto &&operations = PrepareOperationVector();
  workload.AddOperations(operations, kRepeatNum, 0, 1, kRandomSeed);

  std::vector<size_t> frequency(kOpsTypeNum, 0);
  for (const auto &ops : operations) {
    ++(frequency.at(static_cast<size_t>(ops.type)));
  }
  std::sort(frequency.begin(), frequency.end());
  const auto err = (frequency.back() - frequency.front()) / static_cast<double>(kRepeatNum);
  EXPECT_LT(err, kAllowableError);
}

TEST_F(WorkloadFixture, WorkloadWithSkewParameterGenerateSkewedKeys)
{  //
  Json_t w_json = {{
                       "operation ratios",
                       {{"read", 1.0}},
                   },
                   {"# of keys", kDefaultKeyNum},
                   {"partitioning policy", "none"},
                   {"access pattern", "random"},
                   {"skew parameter", 1.0}};

  Workload workload{w_json};

  EXPECT_EQ(workload.GetKeyNum(), kDefaultKeyNum);
  EXPECT_EQ(workload.GetExecutionRatio(), 1.0);

  auto &&operations = PrepareOperationVector();
  workload.AddOperations(operations, kRepeatNum, 0, 1, kRandomSeed);

  std::vector<size_t> frequency(kDefaultKeyNum, 0);
  for (const auto &ops : operations) {
    ++(frequency.at(ops.key));
  }
  std::sort(frequency.begin(), frequency.end(), std::greater<size_t>{});
  const auto base_prob = frequency.front() / static_cast<double>(kRepeatNum);
  for (size_t i = 1; i < frequency.size(); ++i) {
    const auto err = fabs(base_prob / i - frequency.at(i) / static_cast<double>(kRepeatNum));
    EXPECT_LT(err, kAllowableError);
  }
}

TEST_F(WorkloadFixture, WorkloadWithRangePartitionGenerateSeparatedKeys)
{  //
  constexpr size_t kOpsNumPerThread = kDefaultKeyNum / kThreadNum;

  Json_t w_json = {{
                       "operation ratios",
                       {{"read", 1.0}},
                   },
                   {"# of keys", kDefaultKeyNum},
                   {"partitioning policy", "range"},
                   {"access pattern", "sequential"}};

  Workload workload{w_json};

  EXPECT_EQ(workload.GetKeyNum(), kDefaultKeyNum);
  EXPECT_EQ(workload.GetExecutionRatio(), 1.0);

  auto &&operations = PrepareOperationVector();
  size_t counter = 0;
  for (size_t i = 0; i < kThreadNum; ++i) {
    workload.AddOperations(operations, kOpsNumPerThread, i, kThreadNum, kRandomSeed);
    for (const auto &ops : operations) {
      EXPECT_EQ(ops.key, counter++);
    }
    operations.clear();
  }
}

TEST_F(WorkloadFixture, WorkloadWithStripePartitionGenerateStripedKeys)
{  //
  constexpr size_t kOpsNumPerThread = kDefaultKeyNum / kThreadNum;

  Json_t w_json = {{
                       "operation ratios",
                       {{"read", 1.0}},
                   },
                   {"# of keys", kDefaultKeyNum},
                   {"partitioning policy", "stripe"},
                   {"access pattern", "sequential"}};

  Workload workload{w_json};

  EXPECT_EQ(workload.GetKeyNum(), kDefaultKeyNum);
  EXPECT_EQ(workload.GetExecutionRatio(), 1.0);

  auto &&operations = PrepareOperationVector();
  for (size_t i = 0; i < kThreadNum; ++i) {
    workload.AddOperations(operations, kOpsNumPerThread, i, kThreadNum, kRandomSeed);
    auto counter = i;
    for (const auto &ops : operations) {
      EXPECT_EQ(ops.key, counter);
      counter += kThreadNum;
    }
    operations.clear();
  }
}
