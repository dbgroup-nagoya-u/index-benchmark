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

#include "workload/operation_engine.hpp"

#include "gtest/gtest.h"

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr size_t kThreadNum = 2;
constexpr size_t kRepeatNum = 1E6;
constexpr size_t kOpsNumPerThread = kRepeatNum / kThreadNum;
constexpr size_t kRandomSeed = 20;

/*######################################################################################
 * Fixture class definition
 *####################################################################################*/

class OperationEngineFixture : public ::testing::Test
{
 public:
  using Key_t = Key<k8>;
  using Payload_t = uint64_t;
  using OperationEngine_t = OperationEngine<Key_t, Payload_t>;
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
   * Member variables
   *##################################################################################*/

  OperationEngine_t ops_engine{kThreadNum};
};

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TEST_F(OperationEngineFixture, SinglePhaseWorkloadGenerateValidOperations)
{
  Json_t w_json = R"({
    "initialization": {
      "# of keys": 1000000,
      "use all cores": true
    },
    "workloads": [
      {
        "operation ratios": {"read": 1.0},
        "# of keys": 1000000,
        "partitioning policy": "stripe",
        "access pattern": "random"
      }
    ]
  })"_json;

  ops_engine.ParseJson(w_json);

  const auto [init_num, use_all] = ops_engine.GetInitParameters();
  EXPECT_EQ(init_num, 1000000);
  EXPECT_TRUE(use_all);

  size_t counter = 0;
  for (const auto &ops : ops_engine.Generate(kOpsNumPerThread, kRandomSeed)) {
    EXPECT_EQ(ops.type, kRead);
    EXPECT_EQ(ops.key % 2, 0);
    ++counter;
  }
  for (const auto &ops : ops_engine.Generate(kOpsNumPerThread, kRandomSeed)) {
    EXPECT_EQ(ops.type, kRead);
    EXPECT_EQ(ops.key % 2, 1);
    ++counter;
  }
  EXPECT_EQ(counter, kRepeatNum);
}

TEST_F(OperationEngineFixture, MultiplePhasesWorkloadGenerateValidOperations)
{
  Json_t w_json = R"({
    "initialization": {
      "# of keys": 1000000,
      "use all cores": false
    },
    "workloads": [
      {
        "operation ratios": {"read": 1.0},
        "# of keys": 1000000,
        "partitioning policy": "stripe",
        "access pattern": "random",
        "execution ratio": 0.5
      },
      {
        "operation ratios": {"write": 1.0},
        "# of keys": 1000000,
        "partitioning policy": "stripe",
        "access pattern": "random",
        "execution ratio": 0.5
      }
    ]
  })"_json;

  ops_engine.ParseJson(w_json);

  const auto [init_num, use_all] = ops_engine.GetInitParameters();
  EXPECT_EQ(init_num, 1000000);
  EXPECT_FALSE(use_all);

  size_t counter = 0;
  for (const auto &ops : ops_engine.Generate(kOpsNumPerThread, kRandomSeed)) {
    EXPECT_EQ(ops.type, (counter < kOpsNumPerThread / 2) ? kRead : kWrite);
    EXPECT_EQ(ops.key % 2, 0);
    ++counter;
  }
  EXPECT_EQ(counter, kOpsNumPerThread);

  counter = 0;
  for (const auto &ops : ops_engine.Generate(kOpsNumPerThread, kRandomSeed)) {
    EXPECT_EQ(ops.type, (counter < kOpsNumPerThread / 2) ? kRead : kWrite);
    EXPECT_EQ(ops.key % 2, 1);
    ++counter;
  }
  EXPECT_EQ(counter, kOpsNumPerThread);
}
