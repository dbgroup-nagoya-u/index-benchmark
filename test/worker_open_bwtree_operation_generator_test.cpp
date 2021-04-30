// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "worker_open_bwtree.hpp"
#include "operation_generator.hpp"
#include "gtest/gtest.h"

class WorkerOpenBwTreeFixture : public ::testing::Test
{
 public:
  const int OPERATION_NUM = 10000;
  static constexpr double kAllowableError = 0.01;
  const size_t read_ratio = 16;
  const size_t scan_ratio=  32;
  const size_t write_ratio= 48;
  const size_t insert_ratio= 64;
  const size_t update_ratio= 80;
  const size_t delete_ratio=100;
  size_t op_ratio[6] = {read_ratio,scan_ratio,write_ratio,insert_ratio,update_ratio,delete_ratio};
  const size_t total_key_num = 10000;
  double skew_parameter = 1.0;
  Workload workload{read_ratio,scan_ratio,write_ratio,insert_ratio,update_ratio,delete_ratio};
  OperationGenerator op_generator =   OperationGenerator{workload,
                                        total_key_num,
                                        skew_parameter};
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

TEST_F(WorkerOpenBwTreeFixture, MeasureThroughput_Condition_MeasureReasonableExecutionTime)
{
  // Check  read <= scan <= write <= insert <= update <= delete
  for(int i=0;i<5;i++)
    EXPECT_LE(op_ratio[i], op_ratio[i+1]);

  // Generate operation and count operation type

  int op_freq[6] = {0,0,0,0,0,0};
  for(int i=0;i<OPERATION_NUM;i++){
    op_freq[op_generator().type]++;
  }

  // Test
  double expected_freq[6];
  for(int i=0;i<6;i++){
    if(i == 0)
      expected_freq[i] = (op_ratio[i] - 0            )*(OPERATION_NUM/100);
    else
      expected_freq[i] = (op_ratio[i] - op_ratio[i-1])*(OPERATION_NUM/100);
  }
  
  for(int i=0;i<6;i++){
    double error_percentage = abs(expected_freq[i] - op_freq[i])/OPERATION_NUM;
    EXPECT_LE(error_percentage , kAllowableError);
  }
}
