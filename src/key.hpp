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

#ifndef INDEX_BENCHMARK_KEY_HPP
#define INDEX_BENCHMARK_KEY_HPP

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr size_t kKeySeedSize = sizeof(uint32_t);

/*######################################################################################
 * Global utilities
 *####################################################################################*/

template <size_t kKeyLen>
constexpr void
ExtendToKey(  //
    const uint32_t val,
    uint8_t key[kKeyLen])
{
  constexpr size_t kCopyLen = kKeyLen / kKeySeedSize;

  const auto *arr = reinterpret_cast<const uint8_t *>(&val);
  for (size_t i = 0, j = kKeyLen - kCopyLen; i < kKeySeedSize; ++i, j -= kCopyLen) {
    key[j] = arr[i];
  }
}

template <size_t kKeyLen>
constexpr auto
CompressKey(const uint8_t key[kKeyLen])  //
    -> uint32_t
{
  constexpr size_t kCopyLen = kKeyLen / kKeySeedSize;

  uint32_t val{0};
  auto *arr = reinterpret_cast<uint8_t *>(&val);
  for (size_t i = 0, j = kKeyLen - kCopyLen; i < kKeySeedSize; ++i, j -= kCopyLen) {
    arr[i] = key[j];
  }

  return val;
}

/*######################################################################################
 * Static variable-length key definition
 *####################################################################################*/

template <size_t kKeyLen>
class Key
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Key() = default;

  constexpr explicit Key(const uint32_t key) { ExtendToKey<kKeyLen>(key, key_); }

  constexpr Key(const Key &) = default;
  constexpr Key(Key &&) noexcept = default;
  constexpr auto operator=(const Key &) -> Key & = default;
  constexpr auto operator=(Key &&) -> Key & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Key() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  operator<(const Key &obj) const  //
      -> bool
  {
    return memcmp(&key_, &(obj.key_), kKeyLen) < 0;
  }

  constexpr auto
  operator>(const Key &obj) const  //
      -> bool
  {
    return memcmp(&key_, &(obj.key_), kKeyLen) > 0;
  }

  constexpr auto
  operator==(const Key &obj) const  //
      -> bool
  {
    return memcmp(&key_, &(obj.key_), kKeyLen) == 0;
  }

  constexpr auto
  operator+(const size_t val) const  //
      -> Key
  {
    const auto new_key = static_cast<uint32_t>(GetValue() + val);
    return Key{new_key};
  }

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return CompressKey<kKeyLen>(key_);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  uint8_t key_[kKeyLen]{};
};

/*######################################################################################
 * Hash definitions for OpenBw-Tree
 *####################################################################################*/

namespace std
{
template <>
struct hash<Key<8>> {
  auto
  operator()(const Key<8> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key<16>> {
  auto
  operator()(const Key<16> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key<32>> {
  auto
  operator()(const Key<32> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key<64>> {
  auto
  operator()(const Key<64> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key<128>> {
  auto
  operator()(const Key<128> &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};
}  // namespace std

#endif  // INDEX_BENCHMARK_KEY_HPP
