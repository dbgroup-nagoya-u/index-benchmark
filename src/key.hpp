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

// C++ standard libraries
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>

namespace dbgroup
{

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

  static constexpr size_t kWordSize = 8;
  static constexpr size_t kSeedSize = sizeof(uint32_t);
  static constexpr size_t kSeedBitNum = 8;
  static constexpr size_t kPartLen = kKeyLen / kSeedSize;
  static constexpr size_t kCopyLen = (kPartLen <= kWordSize) ? kPartLen : kWordSize;
  static constexpr size_t kCopyNum = kPartLen / kWordSize;

  /*####################################################################################
   * Internal utilities
   *##################################################################################*/

  void
  ExtendToKey(const uint32_t val)
  {
    const auto *arr = reinterpret_cast<const uint8_t *>(&val);
    for (size_t i = 0, j = kKeyLen - kCopyLen; i < kSeedSize; ++i) {
      if constexpr (kCopyNum <= 1) {
        memset(&(key_[j]), arr[i], kCopyLen);
        j -= kCopyLen;
      } else {
        constexpr size_t kMaskBitSize = kSeedBitNum / kCopyNum;
        constexpr uint8_t kBitMask = ~(~0UL << kMaskBitSize);

        auto mask = kBitMask;
        for (size_t k = 0; k < kCopyNum; ++k, j -= kCopyLen) {
          memset(&(key_[j]), arr[i] & mask, kCopyLen);
          mask <<= kMaskBitSize;
        }
      }
    }
  }

  constexpr auto
  CompressKey() const  //
      -> uint32_t
  {
    uint32_t val{0};
    auto *arr = reinterpret_cast<uint8_t *>(&val);
    for (size_t i = 0, j = kKeyLen - kCopyLen; i < kSeedSize; ++i) {
      if constexpr (kCopyNum <= 1) {
        arr[i] = key_[j];
        j -= kCopyLen;
      } else {
        for (size_t k = 0; k < kCopyNum; ++k, j -= kCopyLen) {
          arr[i] |= key_[j];
        }
      }
    }

    return val;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  uint8_t key_[kKeyLen]{};
};

}  // namespace dbgroup

#endif  // INDEX_BENCHMARK_KEY_HPP
