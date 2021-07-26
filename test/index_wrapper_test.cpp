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

  // constant values for testing
  static constexpr size_t kTotalKeyNum = 8192;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // a target index instance
  std::unique_ptr<Index> index;

  // actual keys and values for testing
  Key keys[kTotalKeyNum];
  Value payloads[kTotalKeyNum];

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    index = std::make_unique<Index>();

    for (size_t i = 0; i < kTotalKeyNum; ++i) {
      keys[i] = i;
      payloads[i] = i;
    }

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
   * Functions for wrapping APIs
   *##############################################################################################*/

  void
  Write(  //
      const size_t key_id,
      const size_t payload_id)
  {
    index->Write(keys[key_id], payloads[payload_id]);
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyRead(  //
      const size_t key_id,
      const size_t expected_id,
      const bool expect_fail = false)
  {
    const auto [rc, payload] = index->Read(keys[key_id]);

    if (expect_fail) {
      EXPECT_NE(0, rc);
    } else {
      EXPECT_EQ(0, rc);
      EXPECT_EQ(payloads[expected_id], payload);
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
#ifdef INDEX_BENCH_BUILD_PTREE
                                 ,
                                 PTree_t
#endif
                                 >;
TYPED_TEST_CASE(IndexWrapperFixture, Indexes);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(IndexWrapperFixture, Write_UniqueKeys_ReadInsertedPayloads)
{
  for (size_t i = 0; i < TestFixture::kTotalKeyNum; ++i) {
    TestFixture::Write(i, i);
  }
  for (size_t i = 0; i < TestFixture::kTotalKeyNum; ++i) {
    TestFixture::VerifyRead(i, i);
  }
}

TYPED_TEST(IndexWrapperFixture, Write_DuplicateKeys_ReadUpdatedPayloads)
{
  for (size_t i = 0; i < TestFixture::kTotalKeyNum - 1; ++i) {
    TestFixture::Write(i, i);
  }
  for (size_t i = 0; i < TestFixture::kTotalKeyNum - 1; ++i) {
    TestFixture::Write(i, i + 1);
  }
  for (size_t i = 0; i < TestFixture::kTotalKeyNum - 1; ++i) {
    TestFixture::VerifyRead(i, i + 1);
  }
}
