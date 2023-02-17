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

#ifndef INDEX_BENCHMARK_INDEXES_YAKUSHIMA_WRAPPER_HPP
#define INDEX_BENCHMARK_INDEXES_YAKUSHIMA_WRAPPER_HPP

// C++ standard libraries
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

// external sources
#include "yakushima/include/kvs.h"

// local sources
#include "common.hpp"

namespace dbgroup
{

template <class Key, class Payload>
class YakushimaWrapper
{
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using status = ::yakushima::status;
  using Token = ::yakushima::Token;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;

 public:
  /*####################################################################################
   * Public inner classes
   *##################################################################################*/

  /**
   * @brief A class for representing an iterator of scan results.
   *
   */
  class RecordIterator
  {
   public:
    /*##################################################################################
     * Public constructors and assignment operators
     *################################################################################*/

    /**
     * @brief Construct a new object as an initial iterator.
     *
     * @param index a pointer to an index.
     */
    RecordIterator(std::vector<std::tuple<std::string, Payload *, size_t>> *records)
        : records_{records}
    {
    }

    RecordIterator(const RecordIterator &) = delete;
    RecordIterator(RecordIterator &&) = delete;

    auto operator=(const RecordIterator &) -> RecordIterator & = delete;
    auto operator=(RecordIterator &&obj) -> RecordIterator & = delete;

    /*##################################################################################
     * Public destructors
     *################################################################################*/

    /**
     * @brief Destroy the iterator and a retained node if exist.
     *
     */
    ~RecordIterator() = default;

    /*##################################################################################
     * Public operators for iterators
     *################################################################################*/

    /**
     * @retval true if this iterator indicates a live record.
     * @retval false otherwise.
     */
    explicit operator bool()
    {
      while (true) {
        const auto size = records_->size();
        if (pos_ < size) return true;        // records remain in this node
        if (size < kScanSize) return false;  // this node is the end of range-scan

        const auto &key = std::move(std::get<0>(records_->back()));
        records_->clear();
        ::yakushima::scan(table_name_,                                 //
                          key, ::yakushima::scan_endpoint::EXCLUSIVE,  //
                          kDummyKey, ::yakushima::scan_endpoint::INF,  //
                          *records_, nullptr, kScanSize);
        pos_ = 0;
      }
    }

    /**
     * @brief Forward this iterator.
     *
     */
    constexpr void
    operator++()
    {
      ++pos_;
    }

    /*##################################################################################
     * Public getters/setters
     *################################################################################*/

    /**
     * @return a payload of a current record
     */
    [[nodiscard]] auto
    GetPayload() const  //
        -> Payload
    {
      Payload payload{};
      memcpy(&payload, std::get<1>(records_->at(pos_)), sizeof(Payload));
      return payload;
    }

   private:
    /*##################################################################################
     * Internal member variables
     *################################################################################*/

    /// the scanned records.
    std::vector<std::tuple<std::string, Payload *, size_t>> *records_{nullptr};

    /// the position of a current record.
    size_t pos_{0};
  };

  /*####################################################################################
   * Public constructors/destructors
   *##################################################################################*/

  YakushimaWrapper(  //
      [[maybe_unused]] const size_t gc_interval,
      [[maybe_unused]] const size_t gc_thread_num)
  {
    ::yakushima::init();
    ::yakushima::create_storage(table_name_);
  }

  ~YakushimaWrapper() { ::yakushima::fin(); }

  /*####################################################################################
   * Public utility functions
   *##################################################################################*/

  constexpr void
  SetUp()
  {
    ::yakushima::enter(token_);
  }

  constexpr void
  TearDown()
  {
    ::yakushima::leave(token_);
  }

  constexpr auto
  Bulkload(  //
      [[maybe_unused]] const std::vector<std::pair<Key, Payload>> &entries,
      [[maybe_unused]] const size_t thread_num)  //
      -> int
  {
    return kFailed;
  }

  /*####################################################################################
   * Public read/write APIs
   *##################################################################################*/

  auto
  Read(const Key &key)  //
      -> std::optional<Payload>
  {
    // get a value/size pair
    std::pair<Payload *, size_t> ret{};
    const auto rc = ::yakushima::get(table_name_, ToStrView(key), ret);
    if (rc != status::OK) return std::nullopt;

    // copy a gotten value if exist
    Payload value{};
    memcpy(&value, ret.first, sizeof(Payload));
    return value;
  }

  auto
  Scan(const ScanKey &begin_key = std::nullopt)  //
      -> RecordIterator
  {
    thread_local std::vector<std::tuple<std::string, Payload *, size_t>> records{kScanSize};
    records.clear();

    // scan target tuples
    const auto &key = (begin_key) ? std::get<0>(*begin_key) : Key{};
    ::yakushima::scan(table_name_,                                            //
                      ToStrView(key), ::yakushima::scan_endpoint::INCLUSIVE,  //
                      kDummyKey, ::yakushima::scan_endpoint::INF,             //
                      records, nullptr, kScanSize);

    return RecordIterator{&records};
  }

  auto
  Write(  //
      const Key &key,
      const Payload &value)
  {
    // put a key/value pair
    auto *value_v = const_cast<Payload *>(&value);
    const auto rc = ::yakushima::put(token_, table_name_, ToStrView(key), value_v);

    return (rc == status::OK) ? kSuccess : kFailed;
  }

  auto
  Insert(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)
  {
    throw std::runtime_error{"ERROR: the insert operation is not implemented."};
    return kFailed;
  }

  auto
  Update(  //
      [[maybe_unused]] const Key &key,
      [[maybe_unused]] const Payload &value)
  {
    throw std::runtime_error{"ERROR: the update operation is not implemented."};
    return kFailed;
  }

  auto
  Delete(const Key &key)
  {
    // delete a tuple by a given key
    const auto rc = ::yakushima::remove(token_, table_name_, ToStrView(key));

    return (rc == status::OK) ? kSuccess : kFailed;
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  static constexpr std::string_view kDummyKey{};

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  template <class T>
  static constexpr auto
  ToStrView(const T &data)  //
      -> std::string_view
  {
    return std::string_view{reinterpret_cast<const char *>(&data), sizeof(T)};
  }

  template <class T>
  static constexpr auto
  ToKey(const std::string_view &data)  //
      -> T
  {
    return *(reinterpret_cast<const T *>(data.data()));
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// a table name for identifying a unique storage
  inline static const std::string table_name_{"T"};

  /// a token for each thread
  inline static thread_local Token token_{};
};

template <>
constexpr auto
HasSetUpTearDown<YakushimaWrapper>()  //
    -> bool
{
  return true;
}

}  // namespace dbgroup

#endif  // INDEX_BENCHMARK_INDEXES_YAKUSHIMA_WRAPPER_HPP
