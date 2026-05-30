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
#include "workload/var_len_data.hpp"

// C++ standard libraries
#include <algorithm>
#include <array>
#include <random>

// external libraries
#include <gtest/gtest.h>

// local sources
#include "common.hpp"

namespace dbgroup::index_bench::test
{
/*############################################################################*
 * Global constants
 *############################################################################*/

constexpr size_t kRandomSeed = (DBGROUP_TEST_RANDOM_SEED);
constexpr size_t kStringNum = 1000;

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

TEST(  //
    VarLenDataFixture,
    ComparisonOperatorsReturnExpectedResults)
{
  std::mt19937_64 rng{kRandomSeed};
  std::uniform_int_distribution<size_t> len_dist{0, kMaxVarLenSize - 1};
  std::uniform_int_distribution<char> char_dist{'A', 'z'};

  std::array<std::string, kStringNum> strings = {};
  for (auto& str : strings) {
    const auto len = len_dist(rng);
    str.resize(len);
    std::generate(str.begin(), str.end(), [&]() { return char_dist(rng); });
  }

  for (size_t i = 0; i < kStringNum - 1; ++i) {
    const auto& cur_str = strings[i];
    const auto& next_str = strings[i + 1];
    VarLenData cur{cur_str};
    VarLenData next{next_str};

    EXPECT_EQ(cur, cur);
    if (cur_str < next_str) {
      EXPECT_LT(cur, next);
      EXPECT_NE(cur, next);
    } else if (cur_str > next_str) {
      EXPECT_GT(cur, next);
      EXPECT_NE(cur, next);
    } else {
      EXPECT_EQ(cur, next);
    }
  }
}

}  // namespace dbgroup::index_bench::test
