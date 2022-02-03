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

constexpr size_t kRepeatNum = 1e6;
constexpr size_t kRandomSeed = 20;
constexpr size_t kWordBitNum = 64;
constexpr size_t kKey16ArrSize = 2;
constexpr size_t kKey16ShiftSize = 16;
constexpr size_t kKey16PaddingSize = 4;
constexpr size_t kKey32ArrSize = 4;
constexpr size_t kKey32ShiftSize = 8;
constexpr size_t kKey32PaddingSize = 8;
constexpr size_t kKey64ArrSize = 8;
constexpr size_t kKey64ShiftSize = 4;
constexpr size_t kKey64PaddingSize = 16;
constexpr size_t kKey128ArrSize = 16;
constexpr size_t kKey128ShiftSize = 2;
constexpr size_t kKey128PaddingSize = 32;

class KeyFixture : public ::testing::Test
{
 protected:
  void
  SetUp() override
  {
  }

  void
  TearDown() override
  {
  }

  void
  ClearArray(  //
      uint64_t *arr,
      size_t size)
  {
    for (size_t i = 0; i < size; ++i) {
      arr[i] = 0UL;
    }
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

  template <class Key>
  void
  VerifyGetValue()
  {
    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto expected_val = uint_dist_(randome_engine_);
      Key key{expected_val};
      EXPECT_EQ(key.GetValue(), expected_val);
    }
  }

  template <class Key>
  void
  VerifyCompareOperators()
  {
    const auto &expected_values = CreateSortedRandomUInt();

    auto prev_val = expected_values[0];
    Key prev_key{prev_val};
    for (size_t i = 1; i < expected_values.size(); ++i) {
      auto next_val = expected_values[i];
      Key next_key{next_val};

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

  template <class Key>
  void
  VerifyPlusOperator()
  {
    for (size_t i = 0; i < kRepeatNum; ++i) {
      const auto base_val = uint_dist_(randome_engine_) / 2;
      const auto diff_val = uint_dist_(randome_engine_) / 2;

      Key base_key{base_val};
      const auto &added_key = base_key + diff_val;

      EXPECT_EQ(added_key.GetValue(), base_val + diff_val);
    }
  }

  std::mt19937_64 randome_engine_{kRandomSeed};
  std::uniform_int_distribution<uint32_t> uint_dist_{};
};

TEST_F(KeyFixture, ExtendToKey16CreateExpectedKeys)
{
  uint64_t arr[kKey16ArrSize];
  ClearArray(arr, kKey16ArrSize);

  ExtendToKey16(~0U, arr);

  uint64_t expected_val = 0;
  for (size_t shift = 0; shift < kWordBitNum; shift += kKey16PaddingSize) {
    expected_val |= 1UL << shift;
  }

  for (size_t i = 0; i < kKey16ArrSize; ++i) {
    EXPECT_EQ(arr[i], expected_val);
  }
}

TEST_F(KeyFixture, ExtendToKey32CreateExpectedKeys)
{
  uint64_t arr[kKey32ArrSize];
  ClearArray(arr, kKey32ArrSize);

  ExtendToKey32(~0U, arr);

  uint64_t expected_val = 0;
  for (size_t shift = 0; shift < kWordBitNum; shift += kKey32PaddingSize) {
    expected_val |= 1UL << shift;
  }

  for (size_t i = 0; i < kKey32ArrSize; ++i) {
    EXPECT_EQ(arr[i], expected_val);
  }
}

TEST_F(KeyFixture, ExtendToKey64CreateExpectedKeys)
{
  uint64_t arr[kKey64ArrSize];
  ClearArray(arr, kKey64ArrSize);

  ExtendToKey64(~0U, arr);

  uint64_t expected_val = 0;
  for (size_t shift = 0; shift < kWordBitNum; shift += kKey64PaddingSize) {
    expected_val |= 1UL << shift;
  }

  for (size_t i = 0; i < kKey64ArrSize; ++i) {
    EXPECT_EQ(arr[i], expected_val);
  }
}

TEST_F(KeyFixture, ExtendToKey128CreateExpectedKeys)
{
  uint64_t arr[kKey128ArrSize];
  ClearArray(arr, kKey128ArrSize);

  ExtendToKey128(~0U, arr);

  uint64_t expected_val = 0;
  for (size_t shift = 0; shift < kWordBitNum; shift += kKey128PaddingSize) {
    expected_val |= 1UL << shift;
  }

  for (size_t i = 0; i < kKey128ArrSize; ++i) {
    EXPECT_EQ(arr[i], expected_val);
  }
}

TEST_F(KeyFixture, GetValueReturnOriginalUInt)
{
  VerifyGetValue<Key8>();
  VerifyGetValue<Key16>();
  VerifyGetValue<Key32>();
  VerifyGetValue<Key64>();
  VerifyGetValue<Key128>();
}

TEST_F(KeyFixture, ComparaOperatorsReturnSameResultsWithUInt)
{
  VerifyCompareOperators<Key8>();
  VerifyCompareOperators<Key16>();
  VerifyCompareOperators<Key32>();
  VerifyCompareOperators<Key64>();
  VerifyCompareOperators<Key128>();
}

TEST_F(KeyFixture, PlusOperatorsReturnIncrementedKeys)
{
  VerifyPlusOperator<Key8>();
  VerifyPlusOperator<Key16>();
  VerifyPlusOperator<Key32>();
  VerifyPlusOperator<Key64>();
  VerifyPlusOperator<Key128>();
}
