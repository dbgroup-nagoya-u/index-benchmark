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

#include "indexes/index_wrapper.hpp"

#include <algorithm>
#include <memory>

#include "gtest/gtest.h"

/*##############################################################################
 * Target index implementations
 *############################################################################*/

#include "b_tree/b_tree.hpp"
#include "bw_tree/bw_tree.hpp"
#include "bztree/bztree.hpp"

using Key_t = Key<k8>;
using Value_t = uint64_t;

using BTreePML_t = IndexWrapper<Key_t, Value_t, ::dbgroup::index::b_tree::BTreePML>;
using BwTree_t = IndexWrapper<Key_t, Value_t, ::dbgroup::index::bw_tree::BwTreeVarLen>;
using BwTreeOpt_t = IndexWrapper<Key_t, Value_t, ::dbgroup::index::bw_tree::BwTreeFixLen>;
using BzTreeInPace_t = IndexWrapper<Key_t, Value_t, ::dbgroup::index::bztree::BzTree>;
using BzTreeAppend_t = IndexWrapper<Key_t, int64_t, ::dbgroup::index::bztree::BzTree>;

#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
#include "indexes/yakushima_wrapper.hpp"
using Yakushima_t = YakushimaWrapper<Key_t, Value_t>;
#else
using Yakushima_t = void;
#endif

#ifdef INDEX_BENCH_BUILD_BTREE_OLC
#include "indexes/btree_olc_wrapper.hpp"
using BTreeOLC_t = BTreeOLCWrapper<Key_t, Value_t>;
#else
using BTreeOLC_t = void;
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE
#include "indexes/masstree_wrapper.hpp"
using Masstree_t = MasstreeWrapper<Key_t, Value_t>;
#else
using Masstree_t = void;
#endif

/*##############################################################################
 * Global constants
 *############################################################################*/

constexpr size_t kExecNum = 1E6;
constexpr uint32_t kMaxKey = (~0L);
constexpr size_t kRandAccessSeed = 20;
constexpr bool kExpectSucceeded = true;
constexpr bool kExpectFailed = false;
constexpr bool kSeqAccess = true;
constexpr bool kRandAccess = false;

/*##############################################################################
 * Fixture class definition
 *############################################################################*/

template <class Index>
class IndexWrapperFixture : public ::testing::Test
{
 public:
  using Index_t = Index;

 protected:
  /*############################################################################
   * Setup/Teardown
   *##########################################################################*/

  void
  SetUp() override
  {
    index = std::make_unique<Index>(1);
    index->SetUp();
  }

  void
  TearDown() override
  {
    index->TearDown();
  }

  /*############################################################################
   * Functions for verification
   *##########################################################################*/

  auto
  PrepareKeys(const bool is_sequential)  //
      -> std::vector<Key_t>
  {
    std::vector<Key_t> keys{};
    keys.reserve(kExecNum);

    for (uint32_t i = 0; i < kExecNum; ++i) {
      keys.emplace_back(i);
    }

    if (is_sequential) {
      std::mt19937_64 rand_engine{kRandAccessSeed};
      std::shuffle(keys.begin(), keys.end(), rand_engine);
    }

    return keys;
  }

  void
  PerformReads(  //
      const Value_t expected_value,
      const bool expect_success,
      const bool is_sequential)
  {
    for (const auto &key : PrepareKeys(is_sequential)) {
      const auto &read_val = index->Read(key);
      if (expect_success) {
        ASSERT_TRUE(read_val);
        EXPECT_EQ(read_val.value(), expected_value);
      } else {
        EXPECT_FALSE(read_val);
      }
    }
  }

  void
  PerformWrites(  //
      const Value_t written_value,
      const bool is_sequential)
  {
    for (const auto &key : PrepareKeys(is_sequential)) {
      index->Write(key, written_value);
    }
  }

  void
  PerformDeletes(  //
      const bool expect_success,
      const bool is_sequential)
  {
    for (const auto &key : PrepareKeys(is_sequential)) {
      const auto rc = index->Delete(key);
      if (expect_success) {
        EXPECT_EQ(0, rc);
      } else {
        EXPECT_NE(0, rc);
      }
    }
  }

  /*############################################################################
   * Internal member variables
   *##########################################################################*/

  /// a target index instance
  std::unique_ptr<Index> index{nullptr};
};

/*##############################################################################
 * Preparation for typed testing
 *############################################################################*/

using Indexes = ::testing::Types<  //
#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
    Yakushima_t,
#endif
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
    BTreeOLC_t,
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
    Masstree_t,
#endif
    BTreePML_t,
    BwTree_t,
    BwTreeOpt_t,
    BzTreeInPace_t,
    BzTreeAppend_t  //
    >;

TYPED_TEST_CASE(IndexWrapperFixture, Indexes);

/*##############################################################################
 * Unit test definitions
 *############################################################################*/

TYPED_TEST(IndexWrapperFixture, WriteReadSequentially)
{
  TestFixture::PerformWrites(0, kSeqAccess);
  TestFixture::PerformReads(0, kExpectSucceeded, kSeqAccess);
}

TYPED_TEST(IndexWrapperFixture, WriteWriteReadSequentially)
{
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) return;

  TestFixture::PerformWrites(0, kSeqAccess);
  TestFixture::PerformWrites(1, kSeqAccess);
  TestFixture::PerformReads(1, kExpectSucceeded, kSeqAccess);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteReadSequentially)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::PerformWrites(0, kSeqAccess);
  TestFixture::PerformDeletes(kExpectSucceeded, kSeqAccess);
  TestFixture::PerformReads(0, kExpectFailed, kSeqAccess);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteDeleteSequentially)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::PerformWrites(0, kSeqAccess);
  TestFixture::PerformDeletes(kExpectSucceeded, kSeqAccess);
  TestFixture::PerformDeletes(kExpectFailed, kSeqAccess);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteWriteReadSequentially)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) return;

  TestFixture::PerformWrites(0, kSeqAccess);
  TestFixture::PerformDeletes(kExpectSucceeded, kSeqAccess);
  TestFixture::PerformWrites(1, kSeqAccess);
  TestFixture::PerformReads(1, kExpectSucceeded, kSeqAccess);
}

TYPED_TEST(IndexWrapperFixture, WriteReadRandomly)
{
  TestFixture::PerformWrites(0, kRandAccess);
  TestFixture::PerformReads(0, kExpectSucceeded, kRandAccess);
}

TYPED_TEST(IndexWrapperFixture, WriteWriteReadRandomly)
{
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) return;

  TestFixture::PerformWrites(0, kRandAccess);
  TestFixture::PerformWrites(1, kRandAccess);
  TestFixture::PerformReads(1, kExpectSucceeded, kRandAccess);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteReadRandomly)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::PerformWrites(0, kRandAccess);
  TestFixture::PerformDeletes(kExpectSucceeded, kRandAccess);
  TestFixture::PerformReads(0, kExpectFailed, kRandAccess);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteDeleteRandomly)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::PerformWrites(0, kRandAccess);
  TestFixture::PerformDeletes(kExpectSucceeded, kRandAccess);
  TestFixture::PerformDeletes(kExpectFailed, kRandAccess);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteWriteReadRandomly)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) return;

  TestFixture::PerformWrites(0, kRandAccess);
  TestFixture::PerformDeletes(kExpectSucceeded, kRandAccess);
  TestFixture::PerformWrites(1, kRandAccess);
  TestFixture::PerformReads(1, kExpectSucceeded, kRandAccess);
}
