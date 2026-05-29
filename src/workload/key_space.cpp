/*
 * Copyright 2025 Database Group, Nagoya University
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
#include "workload/key_space.hpp"

// C++ standard libraries
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <vector>

// local sources
#include "common.hpp"
#ifdef INDEX_BENCH_BUILD_ART_OLC
#include "wrappers/art_olc_wrapper.hpp"
#endif

namespace dbgroup::index_bench
{
/*############################################################################*
 * Constructors
 *############################################################################*/

template <>
KeySpace<UIntKey>::KeySpace(  //
    const size_t key_num,
    const std::optional<size_t>& rand_seed)
{
  keys_.reserve(key_num);
  for (uint32_t i = 0; i < key_num; ++i) {
    keys_.emplace_back(i);
  }

  PrepareMapping(key_num, rand_seed);
}

template <>
KeySpace<StrKey>::KeySpace(  //
    const size_t key_num,
    const std::optional<size_t>& rand_seed)
{
  constexpr uint32_t kRepNum = (kMaxVarLenSize - 1) / 10;
  constexpr uint32_t kRepWNull = kRepNum + 1;

  std::array<char, kMaxVarLenSize> src{};
  keys_.reserve(key_num);
  for (uint32_t len = 0; true;) {
    auto& ch = src[len++];
    if (ch == 0) {
      ch = '0';
    } else if (++ch > '9') {
      len -= kRepWNull;
      continue;
    }
    for (uint32_t i = 1; i < kRepNum; ++i) {
      src[len++] = ch;
    }
    src[len] = 0;
    keys_.emplace_back(&src, len + 1);

    if (keys_.size() >= key_num) break;
    if (len >= kMaxVarLenSize - kRepWNull) {
      len -= kRepNum;
    }
  }

  PrepareMapping(key_num, rand_seed);

#ifdef INDEX_BENCH_BUILD_ART_OLC
  ARTOLCWrapper<StrKey, Payload, index::CompareAsCString>::key_space = this;
#endif
}

/*############################################################################*
 * Internal APIs
 *############################################################################*/

template <class Key>
void
KeySpace<Key>::PrepareMapping(  //
    const size_t key_num,
    const std::optional<size_t>& rand_seed)
{
  mapping_.reserve(key_num);
  for (uint32_t i = 0; i < key_num; ++i) {
    mapping_.emplace_back(i);
  }

  if (rand_seed) {
    std::mt19937_64 rand_eng{*rand_seed};
    std::shuffle(mapping_.begin(), mapping_.end(), rand_eng);
  }
}

/*############################################################################*
 * Explicit instantiation definitions
 *############################################################################*/

template class KeySpace<UIntKey>;
template class KeySpace<StrKey>;

}  // namespace dbgroup::index_bench
