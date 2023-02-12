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

template <size_t kKeyLen>
class Key
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Key() = default;

  explicit Key(const uint32_t key) { ExtendToKey(key); }

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

  auto
  operator<(const Key &obj) const  //
      -> bool
  {
    return memcmp(&key_, &(obj.key_), kKeyLen) < 0;
  }

  auto
  operator>(const Key &obj) const  //
      -> bool
  {
    return memcmp(&key_, &(obj.key_), kKeyLen) > 0;
  }

  auto
  operator==(const Key &obj) const  //
      -> bool
  {
    return memcmp(&key_, &(obj.key_), kKeyLen) == 0;
  }

  auto
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
    return CompressKey();
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr size_t kKeySeedSize = sizeof(uint32_t);

  /*####################################################################################
   * Internal utilities
   *##################################################################################*/

  void
  ExtendToKey(const uint32_t val)
  {
    constexpr size_t kCopyLen = kKeyLen / kKeySeedSize;

    const auto *arr = reinterpret_cast<const uint8_t *>(&val);
    for (size_t i = 0, j = kKeyLen - kCopyLen; i < kKeySeedSize; ++i, j -= kCopyLen) {
      memset(&(key_[j]), arr[i], kCopyLen);
    }
  }

  constexpr auto
  CompressKey() const  //
      -> uint32_t
  {
    constexpr size_t kCopyLen = kKeyLen / kKeySeedSize;

    uint32_t val{0};
    auto *arr = reinterpret_cast<uint8_t *>(&val);
    for (size_t i = 0, j = kKeyLen - kCopyLen; i < kKeySeedSize; ++i, j -= kCopyLen) {
      arr[i] = key_[j];
    }

    return val;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  uint8_t key_[kKeyLen]{};
};

#endif  // INDEX_BENCHMARK_KEY_HPP
