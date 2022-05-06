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

#include "key.hpp"

#include <algorithm>
#include <random>
#include <vector>

#include "gtest/gtest.h"

/*######################################################################################
 * Classes for templated testing
 *####################################################################################*/

template <size_t kKeyLen>
struct Target {
  inline static constexpr size_t kSize = kKeyLen;
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
class KeyFixture : public ::testing::Test
{
  using Key_t = Key<Target::kSize>;

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
      Key_t key{expected_val};
      EXPECT_EQ(key.GetValue(), expected_val);
    }
  }

  void
  VerifyCompareOperators()
  {
    const auto &expected_values = CreateSortedRandomUInt();

    auto prev_val = expected_values[0];
    Key_t prev_key{prev_val};
    for (size_t i = 1; i < expected_values.size(); ++i) {
      auto next_val = expected_values[i];
      Key_t next_key{next_val};

      if (prev_val == next_val) {
        EXPECT_EQ(prev_key, next_key);
        EXPECT_FALSE(prev_key < next_key);
        EXPECT_FALSE(prev_key > next_key);
      } else {
        EXPECT_FALSE(prev_key == next_key);
        EXPECT_LT(prev_key, next_key);
        EXPECT_GT(next_key, prev_key);
      }

      prev_val = next_val;
      prev_key = next_key;
    }
  }

  void
  VerifyPlusOperator()
  {
    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto base_val = uint_dist_(randome_engine_) / 2;
      const auto diff_val = uint_dist_(randome_engine_) / 2;

      Key_t base_key{base_val};
      const auto &added_key = base_key + diff_val;

      EXPECT_EQ(added_key.GetValue(), base_val + diff_val);
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
TYPED_TEST_SUITE(KeyFixture, TestTargets);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TYPED_TEST(KeyFixture, GetValueReturnOriginalUInt)
{  //
  TestFixture::VerifyGetValue();
}

TYPED_TEST(KeyFixture, ComparaOperatorsReturnSameResultsWithUInt)
{  //
  TestFixture::VerifyCompareOperators();
}

TYPED_TEST(KeyFixture, PlusOperatorsReturnIncrementedKeys)
{  //
  TestFixture::VerifyPlusOperator();
}
