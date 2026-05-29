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
#include <cstddef>
#include <optional>
#include <random>
#include <utility>
#include <vector>

// local sources
#include "common.hpp"
#include "workload/var_len_data.hpp"

namespace dbgroup::index_bench
{
/*############################################################################*
 * Internal APIs
 *############################################################################*/

template <>
void
KeySpace<Payload>::CreateIntegerKeys(  //
    const size_t key_num)
{
  keys_.reserve(key_num);
  for (size_t i = 0; i < key_num; ++i) {
    keys_.emplace_back(i);
  }
}

template <class Key>
void
KeySpace<Key>::PrepareMapping(  //
    const size_t key_num,
    const std::optional<size_t> &rand_seed)
{
  mapping_.reserve(key_num);
  for (size_t i = 0; i < key_num; ++i) {
    mapping_.emplace_back(i);
  }

  if (rand_seed) {
    std::mt19937_64 rand_eng{*rand_seed};
    std::shuffle(mapping_.begin(), mapping_.end(), rand_eng);
  }
}

/*############################################################################*
 * Public APIs
 *############################################################################*/

template <class Key>
auto
KeySpace<Key>::Size() const  //
    -> size_t
{
  return keys_.size();
}

template <class Key>
auto
KeySpace<Key>::GetKey(       //
    const size_t pos) const  //
    -> std::pair<const Key &, size_t>
{
  const auto &key = keys_[mapping_[pos]];
  if constexpr (std::is_same_v<Key, VarLenData>) {
    return {key, key.len};
  } else {
    return {key, sizeof(Key)};
  }
}

template <class Key>
auto
KeySpace<Key>::GetSortedKey(  //
    const size_t pos) const   //
    -> std::pair<const Key &, size_t>
{
  const auto &key = keys_[pos];
  if constexpr (std::is_same_v<Key, VarLenData>) {
    return {key, key.len};
  } else {
    return {key, sizeof(Key)};
  }
}

/*############################################################################*
 * Constructors
 *############################################################################*/

template <>
KeySpace<Payload>::KeySpace(  //
    const size_t key_num,
    const std::optional<size_t> &rand_seed)
{
  CreateIntegerKeys(key_num);
  PrepareMapping(key_num, rand_seed);
}

/*############################################################################*
 * Explicit instantiation definitions
 *############################################################################*/

template class KeySpace<UIntKey>;
template class KeySpace<StrKey>;

}  // namespace dbgroup::index_bench
