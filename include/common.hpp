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

#ifndef INDEX_BENCHMARK_COMMON_HPP_
#define INDEX_BENCHMARK_COMMON_HPP_

// C++ standard libraries
#include <cstddef>
#include <cstdint>

namespace dbgroup::index_bench
{
/*############################################################################*
 * Global types
 *############################################################################*/

/**
 * @brief A list of index read/write operations.
 *
 */
enum OPType {
  kRead,
  kScan,
  kScanLatest,
  kWrite,
  kUpsert,
  kInsert,
  kUpdate,
  kInsertRelevant,
  kUpdateRelevant,
  kDelete,
  kDeleteAndInsert,
  kTotalNum,
};

/// @brief Using 64-bit integers as keys.
using UIntKey = uint64_t;

/// @brief Using cstrings as keys.
using StrKey = char*;

/// @brief Using 64-bit integers as payloads.
using Payload = uint64_t;

/*############################################################################*
 * Global constants
 *############################################################################*/

/// @brief The expected maximum length of variable-length data.
constexpr size_t kMaxVarLenSize = static_cast<size_t>(INDEX_BENCH_MAX_VARLEN_DATA_SIZE);

/// @brief The expected maximum number of cores.
constexpr size_t kMaxCoreNum = static_cast<size_t>(INDEX_BENCH_MAX_CORE_NUM);

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_COMMON_HPP_
