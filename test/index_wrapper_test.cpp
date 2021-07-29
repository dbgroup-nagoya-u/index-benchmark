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

#include "gtest/gtest.h"

/*##################################################################################################
 * Target index implementations
 *################################################################################################*/

#include "bztree_wrapper.hpp"
using BzTree_t = BzTreeWrapper<Key, Value>;

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
#include "open_bwtree_wrapper.hpp"
using OpenBwTree_t = OpenBwTreeWrapper<Key, Value>;
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE
#include "masstree_wrapper.hpp"
using Masstree_t = MasstreeWrapper<Key, Value>;
#endif

#ifdef INDEX_BENCH_BUILD_PTREE
#include "ptree_wrapper.hpp"
using PTree_t = PTreeWrapper<Key, Value>;
#endif

template <class Index>
class IndexWrapperFixture : public ::testing::Test
{
 protected:
  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

  static constexpr size_t kExecNum = 1e6;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // a target index instance
  std::unique_ptr<Index> index;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    index = std::make_unique<Index>();

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
    if constexpr (std::is_same_v<Index, OpenBwTree_t>) {
      index->ReserveThreads(1);
      index->RegisterThread(0);
    }
#endif
  }

  void
  TearDown() override
  {
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
    if constexpr (std::is_same_v<Index, OpenBwTree_t>) {
      index->UnregisterThread(0);
    }
#endif
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyRead(  //
      const Key key,
      const Value expected,
      const bool expect_fail = false)
  {
    const auto [rc, actual] = index->Read(key);

    if (expect_fail) {
      EXPECT_NE(0, rc);
    } else {
      EXPECT_EQ(0, rc);
      EXPECT_EQ(expected, actual);
    }
  }

  void
  VerifyInsert(  //
      const Key key,
      const Value payload,
      const bool expect_fail = false)
  {
    const auto rc = index->Insert(key, payload);

    if (expect_fail) {
      EXPECT_NE(0, rc);
    } else {
      EXPECT_EQ(0, rc);
    }
  }

  void
  VerifyUpdate(  //
      const Key key,
      const Value payload,
      const bool expect_fail = false)
  {
    const auto rc = index->Update(key, payload);

    if (expect_fail) {
      EXPECT_NE(0, rc);
    } else {
      EXPECT_EQ(0, rc);
    }
  }

  void
  VerifyDelete(  //
      const Key key,
      const bool expect_fail = false)
  {
    const auto rc = index->Delete(key);

    if (expect_fail) {
      EXPECT_NE(0, rc);
    } else {
      EXPECT_EQ(0, rc);
    }
  }
};

/*##################################################################################################
 * Preparation for typed testing
 *################################################################################################*/

using Indexes = ::testing::Types<BzTree_t
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
                                 ,
                                 OpenBwTree_t
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
                                 ,
                                 Masstree_t
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
                                 ,
                                 PTree_t
#endif
                                 >;
TYPED_TEST_CASE(IndexWrapperFixture, Indexes);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

/*--------------------------------------------------------------------------------------------------
 * Write operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(IndexWrapperFixture, Write_UniqueKeys_ReadInsertedPayloads)
{
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::index->Write(i, i);
  }
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(IndexWrapperFixture, Write_DuplicateKeys_ReadUpdatedPayloads)
{
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::index->Write(i, i);
  }
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::index->Write(i, i + 1);
  }
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

/*--------------------------------------------------------------------------------------------------
 * Insert operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(IndexWrapperFixture, Insert_UniqueKeys_ReadInsertedPayloads)
{
#ifdef INDEX_BENCH_BUILD_MASSTREE
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) {
    return;
  }
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
  if constexpr (std::is_same_v<TypeParam, PTree_t>) {
    return;
  }
#endif

  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(IndexWrapperFixture, Insert_DuplicateKeys_InsertFail)
{
#ifdef INDEX_BENCH_BUILD_MASSTREE
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) {
    return;
  }
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
  if constexpr (std::is_same_v<TypeParam, PTree_t>) {
    return;
  }
#endif

  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyInsert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyInsert(i, i, true);
  }
}

/*--------------------------------------------------------------------------------------------------
 * Update operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(IndexWrapperFixture, Update_UniqueKeys_UpdateFail)
{
#ifdef INDEX_BENCH_BUILD_MASSTREE
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) {
    return;
  }
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  if constexpr (std::is_same_v<TypeParam, OpenBwTree_t>) {
    // update is not implemented in OpenBw-Tree
    return;
  }
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
  if constexpr (std::is_same_v<TypeParam, PTree_t>) {
    return;
  }
#endif

  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyUpdate(i, i + 1, true);
  }
}

TYPED_TEST(IndexWrapperFixture, Update_DuplicateKeys_ReadUpdatedPayloads)
{
#ifdef INDEX_BENCH_BUILD_MASSTREE
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) {
    return;
  }
#endif
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  if constexpr (std::is_same_v<TypeParam, OpenBwTree_t>) {
    // update is not implemented in OpenBw-Tree
    return;
  }
#endif
#ifdef INDEX_BENCH_BUILD_PTREE
  if constexpr (std::is_same_v<TypeParam, PTree_t>) {
    return;
  }
#endif

  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::index->Insert(i, i);
  }
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyUpdate(i, i + 1);
  }
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}

/*--------------------------------------------------------------------------------------------------
 * Delete operation
 *------------------------------------------------------------------------------------------------*/

TYPED_TEST(IndexWrapperFixture, Delete_UniqueKeys_DeleteFail)
{
#ifdef INDEX_BENCH_BUILD_PTREE
  if constexpr (std::is_same_v<TypeParam, PTree_t>) {
    return;
  }
#endif

  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyDelete(i, true);
  }
}

TYPED_TEST(IndexWrapperFixture, Delete_DuplicateKeys_ReadFailWithDeletedKeys)
{
#ifdef INDEX_BENCH_BUILD_PTREE
  if constexpr (std::is_same_v<TypeParam, PTree_t>) {
    return;
  }
#endif

  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::index->Write(i, i);
  }
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyDelete(i);
  }
  for (size_t i = 0; i < TestFixture::kExecNum; ++i) {
    TestFixture::VerifyRead(i, i, true);
  }
}
