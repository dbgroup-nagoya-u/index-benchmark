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

// the corresponding header
#include "var_len_data.hpp"

// C++ standard libraries
#include <algorithm>
#include <random>
#include <vector>

// external sources
#include "gtest/gtest.h"

namespace dbgroup
{

/*######################################################################################
 * Classes for templated testing
 *####################################################################################*/

template <size_t kDataLen>
struct Target {
  inline static constexpr size_t kSize = kDataLen;
};

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr size_t kRepeatNum = 1e6;
constexpr size_t kRandomSeed = 20;

/*######################################################################################
 * Fixture class definition
 *####################################################################################*/

template <class Target>
class VarLenDataFixture : public ::testing::Test
{
  using VarLenData_t = VarLenData<Target::kSize>;

 protected:
  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  auto
  CreateSortedRandomUInt()  //
      -> std::vector<uint32_t>
  {
    std::vector<uint32_t> vec;
    vec.reserve(kRepeatNum);

    for (size_t i = 0; i < kRepeatNum; ++i) {
      vec.emplace_back(uint_dist_(randome_engine_));
    }
    std::sort(vec.begin(), vec.end());

    return vec;
  }

  void
  VerifyGetValue()
  {
    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto expected_val = uint_dist_(randome_engine_);
      VarLenData_t data{expected_val};
      EXPECT_EQ(data.GetValue(), expected_val);
    }
  }

  void
  VerifyCompareOperators()
  {
    const auto &expected_values = CreateSortedRandomUInt();

    auto prev_val = expected_values[0];
    VarLenData_t prev_data{prev_val};
    for (size_t i = 1; i < expected_values.size(); ++i) {
      auto next_val = expected_values[i];
      VarLenData_t next_data{next_val};

      if (prev_val == next_val) {
        EXPECT_EQ(prev_data, next_data);
        EXPECT_FALSE(prev_data < next_data);
        EXPECT_FALSE(prev_data > next_data);
      } else {
        EXPECT_FALSE(prev_data == next_data);
        EXPECT_LT(prev_data, next_data);
        EXPECT_GT(next_data, prev_data);
      }

      prev_val = next_val;
      prev_data = next_data;
    }
  }

  void
  VerifyPlusOperator()
  {
    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto base_val = uint_dist_(randome_engine_) / 2;
      const auto diff_val = uint_dist_(randome_engine_) / 2;

      VarLenData_t base_data{base_val};
      const auto &added_data = base_data + diff_val;

      EXPECT_EQ(added_data.GetValue(), base_val + diff_val);
    }
  }

  std::mt19937_64 randome_engine_{kRandomSeed};
  std::uniform_int_distribution<uint32_t> uint_dist_{};
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using TestTargets = ::testing::Types<  //
    Target<8>,
    Target<16>,
    Target<32>,
    Target<64>,
    Target<128>>;
TYPED_TEST_SUITE(VarLenDataFixture, TestTargets);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TYPED_TEST(VarLenDataFixture, GetValueReturnOriginalUInt)
{  //
  TestFixture::VerifyGetValue();
}

TYPED_TEST(VarLenDataFixture, ComparaOperatorsReturnSameResultsWithUInt)
{  //
  TestFixture::VerifyCompareOperators();
}

TYPED_TEST(VarLenDataFixture, PlusOperatorsReturnIncrementedData)
{  //
  TestFixture::VerifyPlusOperator();
}

}  // namespace dbgroup
