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

#ifndef INDEX_BENCHMARK_COMMON_HPP
#define INDEX_BENCHMARK_COMMON_HPP

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

constexpr size_t kGCInterval = 100000;

constexpr size_t kGCThreadNum = 8;

constexpr bool kClosed = true;

#endif  // INDEX_BENCHMARK_COMMON_HPP
