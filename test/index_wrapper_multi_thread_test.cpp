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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "index.hpp"
#include "indexes/index_wrapper.hpp"

/*######################################################################################
 * Target index implementations
 *####################################################################################*/

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

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr size_t kThreadNum = INDEX_BENCH_TEST_THREAD_NUM;
constexpr size_t kExecNum = 1E6;
constexpr size_t kRandAccessSeed = 20;
constexpr bool kExpectSucceeded = true;
constexpr bool kExpectFailed = false;
constexpr bool kSeqAccess = true;
constexpr bool kRandAccess = false;
constexpr bool kRangePart = true;
constexpr bool kStripePart = false;

/*######################################################################################
 * Fixture class definition
 *####################################################################################*/

template <class Index>
class IndexWrapperFixture : public ::testing::Test
{
 protected:
  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    index = std::make_unique<Index>(kThreadNum * 4);
    is_ready = false;
  }

  void
  TearDown() override
  {
    index = nullptr;
  }

  /*####################################################################################
   * Utility functions
   *##################################################################################*/

  auto
  PrepareKeys(const uint32_t worker_id)  //
      -> std::vector<Key_t>
  {
    std::vector<Key_t> keys{};
    {
      std::shared_lock guard{s_mtx};

      keys.reserve(kExecNum);
      for (uint32_t i = 0; i < kExecNum; ++i) {
        const auto id =
            (is_range_partition) ? worker_id * kExecNum + i : kThreadNum * i + worker_id;
        keys.emplace_back(id);
      }

      if (is_sequential) {
        std::mt19937_64 rand_engine{kRandAccessSeed};
        std::shuffle(keys.begin(), keys.end(), rand_engine);
      }
    }

    std::unique_lock lock{x_mtx};
    cond.wait(lock, [this] { return is_ready; });

    return keys;
  }

  void
  ReadByWorker(  //
      const uint32_t worker_id,
      const bool expect_success)
  {
    index->SetUp();

    for (const auto &key : PrepareKeys(worker_id)) {
      const auto &read_val = index->Read(key);
      if (expect_success) {
        ASSERT_TRUE(read_val);
        EXPECT_EQ(read_val.value(), worker_id);
      } else {
        EXPECT_FALSE(read_val);
      }
    }

    index->TearDown();
  }

  void
  WriteByWorker(const uint32_t worker_id)
  {
    index->SetUp();

    for (const auto &key : PrepareKeys(worker_id)) {
      index->Write(key, worker_id);
    }

    index->TearDown();
  }

  void
  DeleteByWorker(  //
      const uint32_t worker_id,
      const bool expect_success)
  {
    index->SetUp();

    for (const auto &key : PrepareKeys(worker_id)) {
      const auto rc = index->Delete(key);
      if (expect_success) {
        EXPECT_EQ(0, rc);
      } else {
        EXPECT_NE(0, rc);
      }
    }

    index->TearDown();
  }

  void
  PerformReads(const bool expect_success)
  {
    std::vector<std::thread> threads{};
    for (uint32_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(&IndexWrapperFixture::ReadByWorker, this, i, expect_success);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    std::lock_guard guard{s_mtx};

    is_ready = true;
    cond.notify_all();

    for (auto &&t : threads) {
      t.join();
    }
  }

  void
  PerformWrites()
  {
    std::vector<std::thread> threads{};
    for (uint32_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(&IndexWrapperFixture::WriteByWorker, this, i);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    std::lock_guard guard{s_mtx};

    is_ready = true;
    cond.notify_all();

    for (auto &&t : threads) {
      t.join();
    }
  }

  void
  PerformDeletes(const bool expect_success)
  {
    std::vector<std::thread> threads{};
    for (uint32_t i = 0; i < kThreadNum; ++i) {
      threads.emplace_back(&IndexWrapperFixture::DeleteByWorker, this, i, expect_success);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    std::lock_guard guard{s_mtx};

    is_ready = true;
    cond.notify_all();

    for (auto &&t : threads) {
      t.join();
    }
  }

  void
  SetParameters(  //
      const bool access_pattern,
      const bool partitioning)
  {
    is_sequential = access_pattern;
    is_range_partition = partitioning;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a target index instance.
  std::unique_ptr<Index> index{nullptr};

  /// access pattern.
  bool is_sequential{};

  /// partitioning.
  bool is_range_partition{};

  /// a mutex for notifying worker threads.
  std::mutex x_mtx{};

  /// a shared mutex for blocking main process.
  std::shared_mutex s_mtx{};

  /// a flag for indicating ready.
  bool is_ready{false};

  /// a condition variable for notifying worker threads.
  std::condition_variable cond{};
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using Indexes = ::testing::Types<  //
#ifdef INDEX_BENCH_BUILD_YAKUSHIMA
    Yakushima_t,
#endif
#ifdef INDEX_BENCH_BUILD_BTREE_OLC
    BTreeOLC_t,
#endif
    // BTreePML_t,  // too slow
    BwTree_t,
    BwTreeOpt_t,
    BzTreeInPace_t,
    BzTreeAppend_t  //
    >;

TYPED_TEST_CASE(IndexWrapperFixture, Indexes);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

TYPED_TEST(IndexWrapperFixture, WriteReadSequentiallyOnRangePartition)
{
  TestFixture::SetParameters(kSeqAccess, kRangePart);

  TestFixture::PerformWrites();
  TestFixture::PerformReads(kExpectSucceeded);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteReadSequentiallyOnRangePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kSeqAccess, kRangePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformReads(kExpectFailed);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteDeleteSequentiallyOnRangePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kSeqAccess, kRangePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformDeletes(kExpectFailed);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteWriteReadSequentiallyOnRangePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kSeqAccess, kRangePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformWrites();
  TestFixture::PerformReads(kExpectSucceeded);
}

TYPED_TEST(IndexWrapperFixture, WriteReadSequentiallyOnStripePartition)
{
  TestFixture::SetParameters(kSeqAccess, kStripePart);

  TestFixture::PerformWrites();
  TestFixture::PerformReads(kExpectSucceeded);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteReadSequentiallyOnStripePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kSeqAccess, kStripePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformReads(kExpectFailed);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteDeleteSequentiallyOnStripePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kSeqAccess, kStripePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformDeletes(kExpectFailed);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteWriteReadSequentiallyOnStripePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kSeqAccess, kStripePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformWrites();
  TestFixture::PerformReads(kExpectSucceeded);
}

TYPED_TEST(IndexWrapperFixture, WriteReadRandomlyOnRangePartition)
{
  TestFixture::SetParameters(kRandAccess, kRangePart);

  TestFixture::PerformWrites();
  TestFixture::PerformReads(kExpectSucceeded);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteReadRandomlyOnRangePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kRandAccess, kRangePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformReads(kExpectFailed);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteDeleteRandomlyOnRangePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kRandAccess, kRangePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformDeletes(kExpectFailed);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteWriteReadRandomlyOnRangePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kRandAccess, kRangePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformWrites();
  TestFixture::PerformReads(kExpectSucceeded);
}

TYPED_TEST(IndexWrapperFixture, WriteReadRandomlyOnStripePartition)
{
  TestFixture::SetParameters(kRandAccess, kStripePart);

  TestFixture::PerformWrites();
  TestFixture::PerformReads(kExpectSucceeded);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteReadRandomlyOnStripePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kRandAccess, kStripePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformReads(kExpectFailed);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteDeleteRandomlyOnStripePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kRandAccess, kStripePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformDeletes(kExpectFailed);
}

TYPED_TEST(IndexWrapperFixture, WriteDeleteWriteReadRandomlyOnStripePartition)
{
  if constexpr (std::is_same_v<TypeParam, BTreeOLC_t>) return;

  TestFixture::SetParameters(kRandAccess, kStripePart);

  TestFixture::PerformWrites();
  TestFixture::PerformDeletes(kExpectSucceeded);
  TestFixture::PerformWrites();
  TestFixture::PerformReads(kExpectSucceeded);
}
