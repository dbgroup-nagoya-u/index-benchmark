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

#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "index_wrapper.hpp"

/*##################################################################################################
 * Target index implementations
 *################################################################################################*/

#include "bztree/bztree.hpp"
using BzTree_t = IndexWrapper<Key, Value, ::dbgroup::index::bztree::BzTree>;

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
#include "open_bwtree_wrapper.hpp"
using OpenBwTree_t = OpenBwTreeWrapper<Key, Value>;
#endif

#ifdef INDEX_BENCH_BUILD_MASSTREE
#include "masstree_wrapper.hpp"
using Masstree_t = MasstreeWrapper<Key, Value>;
#endif

template <class Index>
class IndexWrapperFixture : public ::testing::Test
{
 protected:
  /*################################################################################################
   * Internal struct
   *##############################################################################################*/

  enum WriteType
  {
    kWrite,
    kInsert,
    kUpdate,
    kDelete
  };

  struct Operation {
    Key key;
    Value payload;
  };

  /*################################################################################################
   * Internal constants
   *##############################################################################################*/

#ifdef INDEX_BENCH_TEST_THREAD_NUM
  static constexpr size_t kThreadNum = INDEX_BENCH_TEST_THREAD_NUM;
#else
  static constexpr size_t kThreadNum = 8;
#endif
  static constexpr size_t kExecNum = 1e6;
  static constexpr size_t kTotalExecNum = kExecNum * kThreadNum;
  static constexpr size_t kRandomSeed = 10;
  static constexpr size_t kSuccess = 0;

  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  // a target index instance
  std::unique_ptr<Index> index;

  // a uniform random generator
  std::uniform_int_distribution<size_t> id_dist{0, kTotalExecNum - 1};

  // a lock to stop a main thread
  std::shared_mutex main_lock;

  // a lock to stop worker threads
  std::shared_mutex worker_lock;

  /*################################################################################################
   * Setup/Teardown
   *##############################################################################################*/

  void
  SetUp() override
  {
    index = std::make_unique<Index>();
  }

  void
  TearDown() override
  {
  }

  /*################################################################################################
   * Utility functions
   *##############################################################################################*/

  auto
  PerformWriteOperation(  //
      const WriteType w_type,
      const Operation &ops)
  {
    switch (w_type) {
      case kInsert:
        return index->Insert(ops.key, ops.payload);
      case kUpdate:
        return index->Update(ops.key, ops.payload);
      case kDelete:
        return index->Delete(ops.key);
      case kWrite:
        break;
    }
    return index->Write(ops.key, ops.payload);
  }

  Operation
  PrepareOperation(  //
      const WriteType w_type,
      std::mt19937_64 &rand_engine)
  {
    const auto rand_val = id_dist(rand_engine);

    switch (w_type) {
      case kWrite:
      case kInsert:
      case kDelete:
        break;
      case kUpdate:
        return Operation{rand_val, rand_val + 1};
    }
    return Operation{rand_val, rand_val};
  }

  void
  WriteRandomKeys(  //
      const WriteType w_type,
      const size_t rand_seed)
  {
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
    if constexpr (std::is_same_v<Index, OpenBwTree_t>) {
      index->RegisterThread();
    }
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
    if constexpr (std::is_same_v<Index, Masstree_t>) {
      index->RegisterThread();
    }
#endif

    std::vector<Operation> operations;
    std::vector<Key> written_keys;
    operations.reserve(kExecNum);
    written_keys.reserve(kExecNum);

    {  // create a lock to prevent a main thread
      const std::shared_lock<std::shared_mutex> guard{main_lock};

      // prepare operations to be executed
      std::mt19937_64 rand_engine{rand_seed};
      for (size_t i = 0; i < kExecNum; ++i) {
        operations.emplace_back(PrepareOperation(w_type, rand_engine));
      }
    }

    {  // wait for a main thread to release a lock
      const std::shared_lock<std::shared_mutex> lock{worker_lock};

      // perform and gather results
      for (auto &&ops : operations) {
        if (PerformWriteOperation(w_type, ops) == kSuccess) {
          written_keys.emplace_back(ops.key);
        }
      }
    }

    for (auto &&key : written_keys) {
      VerifyRead(w_type, key);
    }

#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
    if constexpr (std::is_same_v<Index, OpenBwTree_t>) {
      index->UnregisterThread();
    }
#endif
  }

  void
  RunOverMultiThread(const WriteType w_type)
  {
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
    if constexpr (std::is_same_v<Index, OpenBwTree_t>) {
      index->ReserveThreads(kThreadNum);
      open_bw_thread_counter.store(0);
    }
#endif

    std::vector<std::thread> threads;

    {  // create a lock to prevent workers from executing
      const std::unique_lock<std::shared_mutex> guard{worker_lock};

      // run a function over multi-threads with promise
      std::mt19937_64 rand_engine(kRandomSeed);
      for (size_t i = 0; i < kThreadNum; ++i) {
        const auto rand_seed = rand_engine();
        threads.emplace_back(&IndexWrapperFixture::WriteRandomKeys, this, w_type, rand_seed);
      }

      // wait for all workers to finish initialization
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      const std::unique_lock<std::shared_mutex> lock{main_lock};
    }

    // wait for all the worker threads to finish
    for (auto &&t : threads) t.join();
  }

  /*################################################################################################
   * Functions for verification
   *##############################################################################################*/

  void
  VerifyRead(  //
      const WriteType w_type,
      const Key key)
  {
    const auto [rc, actual] = index->Read(key);

    switch (w_type) {
      case kWrite:
      case kInsert:
        EXPECT_EQ(0, rc);
        EXPECT_EQ(key, actual);
        break;
      case kUpdate:
        EXPECT_EQ(0, rc);
        EXPECT_EQ(key + 1, actual);
        break;
      case kDelete:
        EXPECT_NE(0, rc);
        break;
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
                                 >;
TYPED_TEST_CASE(IndexWrapperFixture, Indexes);

/*##################################################################################################
 * Unit test definitions
 *################################################################################################*/

TYPED_TEST(IndexWrapperFixture, Write_MultiThreads_ReadWrittenPayloads)
{
  TestFixture::RunOverMultiThread(TestFixture::WriteType::kWrite);
}

TYPED_TEST(IndexWrapperFixture, Insert_MultiThreads_ReadInsertedPayloads)
{
#ifdef INDEX_BENCH_BUILD_MASSTREE
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) {
    return;
  }
#endif

  TestFixture::RunOverMultiThread(TestFixture::WriteType::kInsert);
}

TYPED_TEST(IndexWrapperFixture, Update_MultiThreads_ReadUpdatedPayloads)
{
#ifdef INDEX_BENCH_BUILD_OPEN_BWTREE
  if constexpr (std::is_same_v<TypeParam, OpenBwTree_t>) {
    return;
  }
#endif
#ifdef INDEX_BENCH_BUILD_MASSTREE
  if constexpr (std::is_same_v<TypeParam, Masstree_t>) {
    return;
  }
#endif

  TestFixture::RunOverMultiThread(TestFixture::WriteType::kWrite);
  TestFixture::RunOverMultiThread(TestFixture::WriteType::kUpdate);
}

TYPED_TEST(IndexWrapperFixture, Delete_MultiThreads_ReadFailWithDeletedKeys)
{
  TestFixture::RunOverMultiThread(TestFixture::WriteType::kWrite);
  TestFixture::RunOverMultiThread(TestFixture::WriteType::kDelete);
}
