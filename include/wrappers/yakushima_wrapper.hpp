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
#include <optional>
#include <stdexcept>
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
template <class Key, class Payload>
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
      const ScanKey& begin_key = std::nullopt)
  {
    thread_local std::vector<ScanRecord> records{kScanSize};
    records.clear();

    // scan target tuples
    Key key;
    size_t key_len;
    if (begin_key) {
      std::tie(key, key_len, std::ignore) = *begin_key;
    } else {
      key = {};
      key_len = 0;
    }
    const auto& bin_key = GetBinKey(key, key_len);

    yakushima::scan(kTableName,                                    //
                    bin_key, yakushima::scan_endpoint::INCLUSIVE,  //
                    kDummyKey, yakushima::scan_endpoint::INF,      //
                    records, nullptr, kScanSize);

    return Iterator{&records};
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
    yakushima::put(token(), kTableName, bin_key, value_v);
  }

  auto
  Upsert(  //
      const Key& key,
      const Payload& value,
      [[maybe_unused]] const size_t key_len)
  {
    // put a key/value pair
    const auto& bin_key = GetBinKey(key, key_len);
    auto* value_v = const_cast<Payload*>(&value);
    yakushima::put(token(), kTableName, bin_key, value_v);
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key& key,
      [[maybe_unused]] const Payload& value,
      [[maybe_unused]] const size_t key_len)
  {
    throw std::runtime_error{"ERROR: the insert operation is not implemented."};
  }

  auto
  Update(  //
      [[maybe_unused]] const Key& key,
      [[maybe_unused]] const Payload& value,
      [[maybe_unused]] const size_t key_len)
  {
    throw std::runtime_error{"ERROR: the update operation is not implemented."};
  }

  auto
  Delete(  //
      const Key& key,
      [[maybe_unused]] const size_t key_len)
  {
    const auto& bin_key = GetBinKey(key, key_len);
    yakushima::remove(token(), kTableName, bin_key);
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

    constexpr explicit Iterator(  //
        std::vector<ScanRecord>* records) noexcept
        : records_{records}
    {
    }

    Iterator(const Iterator&) = delete;
    Iterator(Iterator&&) = delete;

    auto operator=(const Iterator&) -> Iterator& = delete;
    auto operator=(Iterator&&) -> Iterator& = delete;

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

        const auto& key = std::move(std::get<0>(records_->back()));
        records_->clear();
        yakushima::scan(kTableName,                                //
                        key, yakushima::scan_endpoint::EXCLUSIVE,  //
                        kDummyKey, yakushima::scan_endpoint::INF,  //
                        *records_, nullptr, kScanSize);
        pos_ = 0;
      }
    }

    constexpr void
    operator++() noexcept
    {
      ++pos_;
    }

    /*########################################################################*
     * Public getters
     *########################################################################*/

    [[nodiscard]]
    auto
    GetPayload() const  //
        -> Payload
    {
      Payload payload;
      std::memcpy(&payload, &(std::get<1>(records_->at(pos_))), sizeof(Payload));
      return payload;
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    /// the scanned records.
    std::vector<ScanRecord>* records_{};

    /// the position of a current record.
    size_t pos_{};
  };

 private:
  /*##########################################################################*
   * Internal classes
   *##########################################################################*/

  class TokenHolder
  {
   public:
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    TokenHolder()
    {  //
      yakushima::enter(token_);
    }

    TokenHolder(const TokenHolder&) = delete;
    TokenHolder(TokenHolder&&) = delete;

    auto operator=(const TokenHolder&) -> TokenHolder& = delete;
    auto operator=(TokenHolder&&) -> TokenHolder& = delete;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~TokenHolder()
    {  //
      yakushima::leave(token_);
    }

    /*########################################################################*
     * Public operators
     *########################################################################*/

    constexpr auto
    operator()() noexcept  //
        -> Token&
    {
      return token_;
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    Token token_{};
  };

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

  /*##########################################################################*
   * Internal static variables
   *##########################################################################*/

  inline static thread_local TokenHolder token{};
};

/*############################################################################*
 * Specialization for wrappers
 *############################################################################*/

template <>
constexpr auto
HasBulkload<YakushimaWrapper>()  //
    -> bool
{
  return false;
}

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WRAPPERS_YAKUSHIMA_WRAPPER_HPP_
