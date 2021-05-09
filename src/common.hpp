// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

using Key = uint64_t;
using Value = uint64_t;

/**
 * @brief A list of index read/write operations.
 *
 */
enum IndexOperation
{
  kRead,
  kScan,
  kWrite,
  kInsert,
  kUpdate,
  kDelete
};

/**
 * @brief A list of thread-safe index implementations.
 *
 */
enum BenchTarget
{
  kOpenBwTree,
  kBzTree,
  kPTree
};
