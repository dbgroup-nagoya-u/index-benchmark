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

#ifndef INDEX_BENCHMARK_WRAPPERS_MASSTREE_BETA_WRAPPER_HPP_
#define INDEX_BENCHMARK_WRAPPERS_MASSTREE_BETA_WRAPPER_HPP_

// C++ standard libraries
#include <cstddef>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

// external C++ libraries
#include <dbgroup/index/utility.hpp>
#include <dbgroup/thread/id_manager.hpp>
#include <json.hh>
#include <kvrow.hh>
#include <kvthread.hh>
#include <masstree_insert.hh>
#include <masstree_remove.hh>
#include <masstree_scan.hh>
#include <masstree_tcursor.hh>
#include <query_masstree.hh>
#include <timestamp.hh>

// local sources
#include "common.hpp"  // IWYU pragma: keep

/*############################################################################*
 * Global variables for Masstree
 *############################################################################*/
// NOLINTBEGIN

/// @brief A global epoch, regularly updated by the main thread.
relaxed_atomic<mrcu_epoch_type> globalepoch = timestamp() >> 16;

/// @brief The current epoch.
relaxed_atomic<mrcu_epoch_type> active_epoch = globalepoch.load();

/// @brief Don't add log entries, and free old value immediately.
constexpr bool recovering = false;

/// @brief The global epoch only used in logging.
kvepoch_t global_log_epoch = 0;

/// @brief The initial timestamp only used in debugging.
kvtimestamp_t initial_timestamp = 0;

// NOLINTEND
namespace dbgroup::index_bench
{
/*############################################################################*
 * Class definition
 *############################################################################*/

template <class Key, class Payload>
class MasstreeBetaWrapper
{
  /*##########################################################################*
   * Type aliases
   *##########################################################################*/

  using Query = query<row_type>;
  using Table = Masstree::default_table;
  using Str = lcdf::Str;
  using Json = lcdf::Json;
  using ScanKey = std::optional<std::tuple<Key, size_t, bool>>;

 public:
  /*##########################################################################*
   * Public class declarations
   *##########################################################################*/

  class Iterator;
  class Scanner;

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  MasstreeBetaWrapper()
  {
    // assume that a main thread construct this instance
    thread_info = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*thread_info);
  }

  MasstreeBetaWrapper(const MasstreeBetaWrapper&) = delete;
  MasstreeBetaWrapper(MasstreeBetaWrapper&&) = delete;

  auto operator=(const MasstreeBetaWrapper&) -> MasstreeBetaWrapper& = delete;
  auto operator=(MasstreeBetaWrapper&&) -> MasstreeBetaWrapper& = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~MasstreeBetaWrapper()
  {  //
    table_.destroy(*thread_info);
  }

  /*##########################################################################*
   * Public read/write APIs
   *##########################################################################*/

  auto
  Read(  //
      const Key& key,
      const size_t key_len)  //
      -> std::optional<Payload>
  {
    Str out_v{};
    auto found = index_.run_get1(table_.table(), ToBinKey(key, key_len), 0, out_v, *thread_info);

    if (found) {
      Payload payload;
      std::memcpy(&payload, out_v.s, sizeof(Payload));
      return index::ByteSwap(payload);
    }
    return std::nullopt;
  }

  auto
  Scan(  //
      const ScanKey& begin_key = std::nullopt)
  {
    thread_local std::vector<Payload> payloads{kScanSize};

    Key key;
    size_t key_len;
    if (begin_key) {
      std::tie(key, key_len, std::ignore) = *begin_key;
    } else {
      key = {};
      key_len = 0;
    }

    Scanner scanner{kScanSize, &payloads};
    table_.table().scan(ToBinKey(key, key_len), true, scanner, *thread_info);
    return Iterator{&table_, std::move(key), &payloads};
  }

  void
  Write(  //
      const Key& key,
      const Payload& value,
      const size_t key_len)
  {
    index_.run_replace(table_.table(), ToBinKey(key, key_len), ToBinVal(value), *thread_info);
  }

  auto
  Upsert(  //
      const Key& key,
      const Payload& value,
      const size_t key_len)
  {
    index_.run_replace(table_.table(), ToBinKey(key, key_len), ToBinVal(value), *thread_info);
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
      const size_t key_len)
  {
    index_.run_remove(table_.table(), ToBinKey(key, key_len), *thread_info);
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

    Iterator(  //
        Table* table,
        Key key,
        std::vector<Payload>* payloads)
        : table_{table}
        , payloads_{payloads}
        , key_{std::move(key)}
    {
    }

    Iterator(const Iterator&) = delete;
    Iterator(Iterator&&) = delete;

    auto operator=(const Iterator&) -> Iterator& = delete;
    auto operator=(Iterator&& obj) -> Iterator& = delete;

    /*########################################################################*
     * Public destructors
     *########################################################################*/

    ~Iterator() = default;

    /*########################################################################*
     * Public operators for iterators
     *########################################################################*/

    explicit
    operator bool()
    {
      while (true) {
        const size_t size = payloads_->size();
        if (pos_ < size) return true;        // records remain in this node
        if (size < kScanSize) return false;  // this node is the end of range-scan

        key_ = key_ + kScanSize;
        Scanner scanner{kScanSize, payloads_};
        table_->table().scan(ToBinKey(key_, sizeof(Key)), true, scanner, *thread_info);
        pos_ = 0;
      }
    }

    constexpr void
    operator++() noexcept
    {
      ++pos_;
    }

    /*########################################################################*
     * Public getters/setters
     *########################################################################*/

    [[nodiscard]]
    constexpr auto
    GetPayload() const  //
        -> Payload
    {
      return payloads_->at(pos_);
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    Table* table_{};

    /// @brief The scanned payloads.
    std::vector<Payload>* payloads_{};

    /// @brief The position of a current record.
    size_t pos_{};

    /// @brief The current scan key.
    Key key_{};
  };

  class Scanner
  {
   public:
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    Scanner(  //
        const int32_t scan_size,
        std::vector<Payload>* payloads)
        : num_remain_(scan_size)
        , payloads_(payloads)
    {
      payloads_->clear();
    }

    /*########################################################################*
     * Public utilities
     *########################################################################*/

    template <typename SS, typename K>
    void
    visit_leaf(  // NOLINT
        [[maybe_unused]] const SS& ss,
        [[maybe_unused]] const K& k,
        [[maybe_unused]] const threadinfo& th)
    {
      // do nothing for single-version scanning
    }

    auto
    visit_value(  // NOLINT
        [[maybe_unused]] Str str,
        row_type* value,
        [[maybe_unused]] threadinfo& th)  //
        -> bool
    {
      if (row_is_marker(value)) return true;

      Payload payload{};
      memcpy(&payload, value->col(0).data(), sizeof(Payload));
      payloads_->emplace_back(std::move(payload));

      return (--num_remain_) > 0;
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    int32_t num_remain_{};

    std::vector<Payload>* payloads_{};
  };

 private:
  struct ThreadInfoHolder {
    threadinfo* th{};
  };

  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  static constexpr size_t kScanSize = 1000;

  /*##########################################################################*
   * Internal utility functions
   *##########################################################################*/

  static auto
  ToBinKey(  //
      const Key& key,
      const size_t key_len)  //
      -> Str
  {
    return Str{index::ConvertToBinaryData<Key, char>(key), static_cast<int>(key_len)};
  }

  static auto
  ToBinVal(                    //
      const Payload& payload)  //
      -> Str
  {
    thread_local Payload swapped{};
    swapped = index::ByteSwap(payload);
    return Str{std::bit_cast<char*>(&swapped), sizeof(Payload)};
  }

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  /// @brief Information of each worker thread.
  static thread_local inline threadinfo* thread_info{threadinfo::make(  // NOLINT
      threadinfo::TI_PROCESS,
      static_cast<int32_t>(thread::IDManager::GetThreadID()))};

  Query index_{};

  Table table_{};
};

/*############################################################################*
 * Specialization for wrappers
 *############################################################################*/

template <>
constexpr auto
HasBulkload<MasstreeBetaWrapper>()  //
    -> bool
{
  return false;
}

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WRAPPERS_MASSTREE_BETA_WRAPPER_HPP_
