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

#ifndef INDEX_BENCHMARK_WRAPPERS_YAKUSHIMA_WRAPPER_HPP_
#define INDEX_BENCHMARK_WRAPPERS_YAKUSHIMA_WRAPPER_HPP_

// C++ standard libraries
#include <cstddef>
#include <cstring>
#include <functional>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

// external libraries
#include <kvs.h>

// external C++ libraries
#include <dbgroup/index/utility.hpp>

// local sources
#include "common.hpp"  // IWYU pragma: keep

namespace dbgroup::index_bench
{
template <class Key, class Payload, class Comp = std::less<Key>>
class YakushimaWrapper
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using status = yakushima::status;
  using Token = yakushima::Token;
  using ScanKey = std::optional<std::tuple<Key, size_t, bool>>;
  using ScanRecord = std::tuple<std::string, Payload*, size_t>;

 public:
  /*##########################################################################*
   * Public class declarations
   *##########################################################################*/

  class Iterator;

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  YakushimaWrapper()
  {
    yakushima::init();
    yakushima::create_storage(kTableName);
  }

  YakushimaWrapper(const YakushimaWrapper&) = delete;
  YakushimaWrapper(YakushimaWrapper&&) = delete;

  auto operator=(const YakushimaWrapper&) -> YakushimaWrapper& = delete;
  auto operator=(YakushimaWrapper&&) -> YakushimaWrapper& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~YakushimaWrapper()
  {  //
    yakushima::fin();
  }

  /*##########################################################################*
   * Public utilities
   *##########################################################################*/

  void
  SetUp()
  {
    yakushima::enter(token);
  }

  void
  TearDown()
  {
    yakushima::leave(token);
  }

  /*##########################################################################*
   * Public read/write APIs
   *##########################################################################*/

  auto
  Read(  //
      const Key& key,
      [[maybe_unused]] const size_t key_len)  //
      -> std::optional<Payload>
  {
    // get a value/size pair
    const auto& bin_key = GetBinKey(key, key_len);
    std::pair<Payload*, size_t> ret{};
    const auto rc = yakushima::get(kTableName, bin_key, ret);
    if (rc != status::OK) return std::nullopt;

    // copy a gotten value if exist
    Payload value{};
    std::memcpy(&value, &ret.first, sizeof(Payload));
    return value;
  }

  auto
  Scan(  //
      const ScanKey& begin_key = std::nullopt,
      const ScanKey& end_key = std::nullopt)  //
      -> Iterator
  {
    thread_local std::vector<ScanRecord> records{kScanSize};
    records.clear();

    std::string e_key;
    yakushima::scan_endpoint e_flg;
    std::tie(e_key, e_flg) = GetScanKey(end_key);
    const auto& [b_key, b_flg] = GetScanKey(begin_key);
    yakushima::scan(kTableName, b_key, b_flg, e_key, e_flg,  //
                    records, nullptr, kScanSize);

    return Iterator{&records, std::move(e_key), e_flg};
  }

  void
  Write(  //
      const Key& key,
      const Payload& value,
      [[maybe_unused]] const size_t key_len)
  {
    // put a key/value pair
    const auto& bin_key = GetBinKey(key, key_len);
    auto* value_v = const_cast<Payload*>(&value);
    yakushima::put(token, kTableName, bin_key, value_v);
  }

  auto
  Insert(  //
      const Key& key,
      const Payload& value,
      [[maybe_unused]] const size_t key_len)  //
      -> std::optional<Payload>
  {
    // put a key/value pair
    const auto& bin_key = GetBinKey(key, key_len);
    auto* value_v = const_cast<Payload*>(&value);
    std::optional<Payload> ret{};
    if (yakushima::put(token, kTableName, bin_key, value_v, sizeof(Payload),
                       static_cast<Payload**>(nullptr),
                       static_cast<std::align_val_t>(alignof(Payload)), true)
        != status::OK) {
      ret.emplace(1);
    }
    return ret;
  }

  auto
  Delete(  //
      const Key& key,
      [[maybe_unused]] const size_t key_len)  //
      -> std::optional<Payload>
  {
    const auto& bin_key = GetBinKey(key, key_len);
    std::optional<Payload> ret{};
    if (yakushima::remove(token, kTableName, bin_key) == status::OK) {
      ret.emplace(1);
    }
    return ret;
  }

  auto
  MemoryUsage()  //
      -> std::pair<size_t, size_t>
  {
    size_t total_used{};
    size_t total_alloc{};
    const auto& usage = yakushima::mem_usage(kTableName);
    for (const auto [_, used, allocated] : usage) {
      total_used += used;
      total_alloc += allocated;
    }
    return {total_used, total_alloc};
  }

  /*##########################################################################*
   * Public class definitions
   *##########################################################################*/

  class Iterator
  {
   public:
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    constexpr Iterator() noexcept = default;

    constexpr explicit Iterator(  //
        std::vector<ScanRecord>* records,
        std::string e_key,
        const yakushima::scan_endpoint e_flg) noexcept
        : records_{records}
        , e_key_{std::move(e_key)}
        , e_flg_{e_flg}
    {
    }

    constexpr Iterator(Iterator&&) noexcept = default;
    constexpr auto operator=(Iterator&&) noexcept -> Iterator& = default;

    // forbit copying
    Iterator(const Iterator&) = delete;
    auto operator=(const Iterator&) -> Iterator& = delete;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~Iterator() = default;

    /*########################################################################*
     * Public operators
     *########################################################################*/

    explicit
    operator bool()
    {
      while (true) {
        const auto size = records_->size();
        if (pos_ < size) return true;        // records remain in this node
        if (size < kScanSize) return false;  // this node is the end of range-scan

        const auto key = std::move(std::get<0>(records_->back()));
        records_->clear();
        yakushima::scan(kTableName, key, yakushima::scan_endpoint::EXCLUSIVE,  //
                        e_key_, e_flg_, *records_, nullptr, kScanSize);
        pos_ = 0;
      }
    }

    constexpr void
    operator++() noexcept
    {
      ++pos_;
    }

    auto
    operator*() const  //
        -> std::pair<Key, Payload>
    {
      const auto& rec = (*records_)[pos_];
      const auto& key = index::ConvertFromBinaryData<Key>(std::get<0>(rec).c_str());
      Payload payload;
      std::memcpy(&payload, &(std::get<1>(rec)), sizeof(Payload));

      return {key, payload};
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    std::vector<ScanRecord>* records_{};

    size_t pos_{};

    std::string e_key_{};

    yakushima::scan_endpoint e_flg_{};
  };

 private:
  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kScanSize = 1000;

  static constexpr std::string_view kDummyKey{};

  static constexpr const char* kTableName{"T"};

  /*##########################################################################*
   * Internal utilities
   *##########################################################################*/

  static auto
  GetBinKey(  //
      const Key& key,
      const size_t key_len)  //
      -> std::string_view
  {
    return std::string_view{index::ConvertToBinaryData<Key, char>(key), key_len};
  }

  static auto
  GetScanKey(                   //
      const ScanKey& scan_key)  //
      -> std::pair<std::string_view, yakushima::scan_endpoint>
  {
    std::string_view bin_key;
    yakushima::scan_endpoint flg;
    if (scan_key) {
      const auto& [key, key_len, closed] = *scan_key;
      bin_key = GetBinKey(key, key_len);
      flg = closed ? yakushima::scan_endpoint::INCLUSIVE : yakushima::scan_endpoint::EXCLUSIVE;
    } else {
      bin_key = {};
      flg = yakushima::scan_endpoint::INF;
    }
    return {bin_key, flg};
  }

  /*##########################################################################*
   * Internal static variables
   *##########################################################################*/

  inline static thread_local Token token{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WRAPPERS_YAKUSHIMA_WRAPPER_HPP_
