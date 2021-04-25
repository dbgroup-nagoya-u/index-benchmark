// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#include "gtest/gtest.h"

class HelloWorldFixture : public ::testing::Test
{
 public:
  int64_t common_variable;

 protected:
  void
  SetUp() override
  {
    common_variable = 0;
  }

  void
  TearDown() override
  {
  }
};

TEST_F(HelloWorldFixture, TestTarget_Situation_DesiredResults)
{
  EXPECT_EQ(0, common_variable);
  EXPECT_NE(1, common_variable);
  EXPECT_LT(common_variable, 1);
  EXPECT_LE(common_variable, 0);
  EXPECT_GT(1, common_variable);
  EXPECT_GE(0, common_variable);
  auto is_equal = common_variable == 0;
  EXPECT_TRUE(is_equal);
  EXPECT_FALSE(!is_equal);
}
