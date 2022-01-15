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

/*######################################################################################
 * Global enum and constants
 *####################################################################################*/

/**
 * @brief A list of index read/write operations.
 *
 */
enum class IndexOperation : uint32_t
{
  kRead,
  kScan,
  kWrite,
  kInsert,
  kUpdate,
  kDelete
};

/**
 * @brief A list of the size of target keys.
 *
 */
enum KeySize
{
  k8 = 8,
  k16 = 16,
  k32 = 32,
  k64 = 64,
  k128 = 128
};

constexpr size_t kGCInterval = 100000;

constexpr size_t kGCThreadNum = 8;

constexpr bool kClosed = true;

constexpr size_t kArrNum = 32;

constexpr size_t kOneBit = 1;

/*######################################################################################
 * Global utilities
 *####################################################################################*/

constexpr void
ExtendToKey16(  //
    uint32_t val,
    uint64_t arr[2])
{
  for (int64_t i = 1; i >= 0; --i) {
    arr[i] |= (val & kOneBit);
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 4UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 8UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 12UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 16UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 20UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 24UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 28UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 32UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 36UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 40UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 44UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 48UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 52UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 56UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 60UL;
    val >>= 1UL;
  }
}

constexpr void
ExtendToKey32(  //
    uint32_t val,
    uint64_t arr[4])
{
  for (int64_t i = 3; i >= 0; --i) {
    arr[i] |= (val & kOneBit);
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 8UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 16UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 24UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 32UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 40UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 48UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 56UL;
    val >>= 1UL;
  }
}

constexpr void
ExtendToKey64(  //
    uint32_t val,
    uint64_t arr[8])
{
  for (int64_t i = 7; i >= 0; --i) {
    arr[i] |= (val & kOneBit);
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 16UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 32UL;
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 48UL;
    val >>= 1UL;
  }
}

constexpr void
ExtendToKey128(  //
    uint32_t val,
    uint64_t arr[16])
{
  for (int64_t i = 15; i >= 0; --i) {
    arr[i] |= (val & kOneBit);
    val >>= 1UL;
    arr[i] |= (val & kOneBit) << 32UL;
    val >>= 1UL;
  }
}

constexpr auto
CompressKey16(const uint64_t arr[2])  //
    -> uint64_t
{
  uint64_t val = 0;

  for (size_t i = 0; i < 4; ++i) {
    val |= (arr[i] >> 60UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 56UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 52UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 48UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 44UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 40UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 36UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 32UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 28UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 24UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 20UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 16UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 12UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 8UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 4UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] & kOneBit);
    val <<= 1UL;
  }

  return val;
}

constexpr auto
CompressKey32(const uint64_t arr[4])  //
    -> uint64_t
{
  uint64_t val = 0;

  for (size_t i = 0; i < 4; ++i) {
    val |= (arr[i] >> 56UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 48UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 40UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 32UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 24UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 16UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 8UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] & kOneBit);
    val <<= 1UL;
  }

  return val;
}

constexpr auto
CompressKey64(const uint64_t arr[8])  //
    -> uint64_t
{
  uint64_t val = 0;

  for (size_t i = 0; i < 8; ++i) {
    val |= (arr[i] >> 48UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 32UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] >> 16UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] & kOneBit);
    val <<= 1UL;
  }

  return val;
}

constexpr auto
CompressKey128(const uint64_t arr[16])  //
    -> uint64_t
{
  uint64_t val = 0;

  for (size_t i = 0; i < 16; ++i) {
    val |= (arr[i] >> 32UL) & kOneBit;
    val <<= 1UL;
    val |= (arr[i] & kOneBit);
    val <<= 1UL;
  }

  return val;
}

/*######################################################################################
 * Key/Value definitions
 *####################################################################################*/

class Key8
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Key8() = default;

  constexpr explicit Key8(const uint32_t key) : key_{key} {}

  constexpr Key8(const Key8 &) = default;
  constexpr Key8(Key8 &&) noexcept = default;
  constexpr auto operator=(const Key8 &) -> Key8 & = default;
  constexpr auto operator=(Key8 &&) -> Key8 & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Key8() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  operator<(const Key8 &obj) const  //
      -> bool
  {
    return key_ < obj.key_;
  }

  constexpr auto
  operator>(const Key8 &obj) const  //
      -> bool
  {
    return key_ > obj.key_;
  }

  constexpr auto
  operator==(const Key8 &obj) const  //
      -> bool
  {
    return key_ == obj.key_;
  }

  constexpr auto
  operator+(const size_t val) const  //
      -> Key8
  {
    auto obj = *this;
    obj.key_ += val;
    return obj;
  }

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return key_;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t key_{};
};

class Key16
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Key16() = default;

  constexpr explicit Key16(const uint32_t key) { ExtendToKey16(key, key_); }

  constexpr Key16(const Key16 &) = default;
  constexpr Key16(Key16 &&) noexcept = default;
  constexpr auto operator=(const Key16 &) -> Key16 & = default;
  constexpr auto operator=(Key16 &&) -> Key16 & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Key16() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  operator<(const Key16 &obj) const  //
      -> bool
  {
    return key_[0] < obj.key_[0]  //
           || (key_[0] <= obj.key_[0] && key_[1] < obj.key_[1]);
  }

  constexpr auto
  operator>(const Key16 &obj) const  //
      -> bool
  {
    return key_[0] > obj.key_[0]  //
           || (key_[0] >= obj.key_[0] && key_[1] > obj.key_[1]);
  }

  constexpr auto
  operator==(const Key16 &obj) const  //
      -> bool
  {
    return key_[0] == obj.key_[0]  //
           && key_[1] == obj.key_[1];
  }

  constexpr auto
  operator+(const size_t val) const  //
      -> Key16
  {
    return Key16{static_cast<uint32_t>(CompressKey16(key_) + val)};
  }

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return CompressKey16(key_);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t key_[2] = {0, 0};
};

class Key32
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Key32() = default;

  constexpr explicit Key32(const uint32_t key) { ExtendToKey32(key, key_); }

  constexpr Key32(const Key32 &) = default;
  constexpr Key32(Key32 &&) noexcept = default;
  constexpr auto operator=(const Key32 &) -> Key32 & = default;
  constexpr auto operator=(Key32 &&) -> Key32 & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Key32() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  operator<(const Key32 &obj) const  //
      -> bool
  {
    for (size_t i = 0; i < 4; ++i) {
      if (key_[i] < obj.key_[i]) return true;
      if (key_[i] > obj.key_[i]) return false;
    }
    return false;
  }

  constexpr auto
  operator>(const Key32 &obj) const  //
      -> bool
  {
    for (size_t i = 0; i < 4; ++i) {
      if (key_[i] > obj.key_[i]) return true;
      if (key_[i] < obj.key_[i]) return false;
    }
    return false;
  }

  constexpr auto
  operator==(const Key32 &obj) const  //
      -> bool
  {
    for (size_t i = 0; i < 4; ++i) {
      if (key_[i] != obj.key_[i]) return false;
    }
    return true;
  }

  constexpr auto
  operator+(const size_t val) const  //
      -> Key32
  {
    return Key32{static_cast<uint32_t>(CompressKey32(key_) + val)};
  }

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return CompressKey32(key_);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t key_[4] = {0, 0, 0, 0};
};

class Key64
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Key64() = default;

  constexpr explicit Key64(const uint32_t key) { ExtendToKey64(key, key_); }

  constexpr Key64(const Key64 &) = default;
  constexpr Key64(Key64 &&) noexcept = default;
  constexpr auto operator=(const Key64 &) -> Key64 & = default;
  constexpr auto operator=(Key64 &&) -> Key64 & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Key64() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  operator<(const Key64 &obj) const  //
      -> bool
  {
    for (size_t i = 0; i < 8; ++i) {
      if (key_[i] < obj.key_[i]) return true;
      if (key_[i] > obj.key_[i]) return false;
    }
    return false;
  }

  constexpr auto
  operator>(const Key64 &obj) const  //
      -> bool
  {
    for (size_t i = 0; i < 8; ++i) {
      if (key_[i] > obj.key_[i]) return true;
      if (key_[i] < obj.key_[i]) return false;
    }
    return false;
  }

  constexpr auto
  operator==(const Key64 &obj) const  //
      -> bool
  {
    for (size_t i = 0; i < 8; ++i) {
      if (key_[i] != obj.key_[i]) return false;
    }
    return true;
  }

  constexpr auto
  operator+(const size_t val) const  //
      -> Key64
  {
    return Key64{static_cast<uint32_t>(CompressKey64(key_) + val)};
  }

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return CompressKey64(key_);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t key_[8] = {0, 0, 0, 0, 0, 0, 0, 0};
};

class Key128
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr Key128() = default;

  constexpr explicit Key128(const uint32_t key) { ExtendToKey128(key, key_); }

  constexpr Key128(const Key128 &) = default;
  constexpr Key128(Key128 &&) noexcept = default;
  constexpr auto operator=(const Key128 &) -> Key128 & = default;
  constexpr auto operator=(Key128 &&) -> Key128 & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~Key128() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  operator<(const Key128 &obj) const  //
      -> bool
  {
    for (size_t i = 0; i < 16; ++i) {
      if (key_[i] < obj.key_[i]) return true;
      if (key_[i] > obj.key_[i]) return false;
    }
    return false;
  }

  constexpr auto
  operator>(const Key128 &obj) const  //
      -> bool
  {
    for (size_t i = 0; i < 16; ++i) {
      if (key_[i] > obj.key_[i]) return true;
      if (key_[i] < obj.key_[i]) return false;
    }
    return false;
  }

  constexpr auto
  operator==(const Key128 &obj) const  //
      -> bool
  {
    for (size_t i = 0; i < 16; ++i) {
      if (key_[i] != obj.key_[i]) return false;
    }
    return true;
  }

  constexpr auto
  operator+(const size_t val) const  //
      -> Key128
  {
    return Key128{static_cast<uint32_t>(CompressKey128(key_) + val)};
  }

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return CompressKey128(key_);
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t key_[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
};

class InPlaceVal
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr InPlaceVal() : value_{}, control_bits_{0} {}

  constexpr explicit InPlaceVal(const size_t val) : value_{val}, control_bits_{0} {}

  constexpr InPlaceVal(const InPlaceVal &) = default;
  constexpr InPlaceVal(InPlaceVal &&) noexcept = default;
  constexpr auto operator=(const InPlaceVal &) -> InPlaceVal & = default;
  constexpr auto operator=(InPlaceVal &&) -> InPlaceVal & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~InPlaceVal() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return value_;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t value_ : 61;

  size_t control_bits_ : 3;
};

class AppendVal
{
 public:
  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  constexpr AppendVal() : value_{}, control_bits_{0} {}

  constexpr explicit AppendVal(const size_t val) : value_{val}, control_bits_{0} {}

  constexpr AppendVal(const AppendVal &) = default;
  constexpr AppendVal(AppendVal &&) noexcept = default;
  constexpr auto operator=(const AppendVal &) -> AppendVal & = default;
  constexpr auto operator=(AppendVal &&) -> AppendVal & = default;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  ~AppendVal() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  constexpr auto
  GetValue() const  //
      -> size_t
  {
    return value_;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  size_t value_ : 61;

  size_t control_bits_ : 3;
};

namespace std
{
template <>
struct hash<Key8> {
  auto
  operator()(const Key8 &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key16> {
  auto
  operator()(const Key16 &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key32> {
  auto
  operator()(const Key32 &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key64> {
  auto
  operator()(const Key64 &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<Key128> {
  auto
  operator()(const Key128 &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<InPlaceVal> {
  auto
  operator()(const InPlaceVal &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};

template <>
struct hash<AppendVal> {
  auto
  operator()(const AppendVal &key) const  //
      -> size_t
  {
    return std::hash<size_t>{}(key.GetValue());
  }
};
}  // namespace std

#endif  // INDEX_BENCHMARK_COMMON_HPP
