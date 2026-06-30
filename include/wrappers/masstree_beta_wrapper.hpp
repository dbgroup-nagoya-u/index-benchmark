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
#include <functional>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

//
#include <cassert>

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

template <class Key, class Payload, class Comp = std::less<Key>>
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
  using ScanRecord = std::pair<std::string, Payload>;

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
    ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    table_.initialize(*ti);
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
    table_.destroy(*ti);
  }

  /*##########################################################################*
   * Public utilities
   *##########################################################################*/

  void
  SetUp()
  {
    const auto thread_id = static_cast<int32_t>(thread::IDManager::GetThreadID());
    ti = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
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
    auto found = index_.run_get1(table_.table(), ToBinKey(key, key_len), 0, out_v, *ti);

    if (found) {
      Payload payload;
      std::memcpy(&payload, out_v.s, sizeof(Payload));
      return index::ByteSwap(payload);
    }
    return std::nullopt;
  }

  auto
  Scan(  //
      const ScanKey& begin_key = std::nullopt,
      const ScanKey& end_key = std::nullopt)
  {
    thread_local std::vector<ScanRecord> records{kScanSize};

    std::optional<std::string> e_key{};
    auto e_closed = false;
    if (end_key) {
      Key key;
      size_t key_len;
      std::tie(key, key_len, e_closed) = *end_key;
      e_key.emplace(index::ConvertToBinaryData<Key, char>(key), key_len);
    }

    Str b_key{};
    auto b_closed = true;
    if (begin_key) {
      Key key;
      size_t key_len;
      std::tie(key, key_len, b_closed) = *begin_key;
      b_key = ToBinKey(key, key_len);
    }

    Scanner scanner{kScanSize, &records};
    table_.table().scan(b_key, b_closed, scanner, *ti);
    return Iterator{&table_, &records, std::move(e_key), e_closed};
  }

  void
  Write(  //
      const Key& key,
      const Payload& value,
      const size_t key_len)
  {
    index_.run_replace(table_.table(), ToBinKey(key, key_len), ToBinVal(value), *ti);
  }

  auto
  Delete(  //
      const Key& key,
      const size_t key_len)  //
      -> std::optional<Payload>
  {
    std::optional<Payload> ret{};
    if (index_.run_remove(table_.table(), ToBinKey(key, key_len), *ti)) {
      ret.emplace(1);
    }
    return ret;
  }

  auto
  MemoryUsage() const  //
      -> std::pair<size_t, size_t>
  {
    size_t total_used{};
    size_t total_alloc{};
    const auto& usage = MemoryUsageDetailed();
    for (const auto& usage_at : usage) {
      for (const auto& [used, allocated, _] : usage_at) {
        total_used += used;
        total_alloc += allocated;
      }
    }
    return {total_used, total_alloc};
  }

  [[nodiscard]]
  auto
  MemoryUsageDetailed() const  //
      -> std::vector<std::vector<std::tuple<size_t, size_t, size_t>>>
  {
    std::vector<std::vector<std::tuple<size_t, size_t, size_t>>> usage{};
    usage.reserve(kMaxDepth);

    using params = Masstree::default_query_table_params;
    using base_node = Masstree::node_base<params>;
    using interior_node = Masstree::internode<params>;
    using border_node = Masstree::leaf<params>;

    auto* root = table_.table().root();
    std::vector<std::vector<std::pair<base_node*, int>>> layers{};
    layers.reserve(kMaxDepth);
    layers.emplace_back().emplace_back(root, 0);
    while (!layers.empty()) {
      auto& stack = layers.back();
      stack.reserve(kInitialHeight);
      while (!stack.empty()) {
        auto& [node, pos] = stack.back();
        auto* interior = std::bit_cast<interior_node*>(node);
        auto* border = std::bit_cast<border_node*>(node);
        if (!node->isleaf() && pos <= interior->size()) {
          auto* child = interior->child_[pos++];
          stack.emplace_back(child, 0);
          continue;
        }
        if (node->isleaf()) {
          const auto permutation = border->permutation();
          while (pos < border->size()) {
            const auto rec_pos = permutation[pos++];
            if (border->is_layer(rec_pos)) {
              root = border->lv_[rec_pos].layer();
              layers.emplace_back().emplace_back(root, 0);
              goto out;
            }
          }
        }

        const auto layer = layers.size() - 1;
        const auto level = node->isleaf() ? 0 : interior->height_;
        while (usage.size() <= layer) [[unlikely]] {
          usage.emplace_back();
          usage.back().reserve(kInitialHeight);
        }
        auto&& usage_at = usage[layer];
        while (usage_at.size() <= level) [[unlikely]] {
          usage_at.emplace_back();
        }
        auto& [used, allocated, node_cnt] = usage_at[level];

        if (node->isleaf()) {
          constexpr size_t kBaseSize = 50;
          const auto n = border->size();
          const auto permutation = border->permutation();
          used += kBaseSize + n * (1 + kPaySize);
          for (int32_t pos = 0; pos < n; ++pos) {
            const auto rec_pos = permutation[pos];
            if (border->is_layer(rec_pos)) {
              used += kPartKeySize;
            } else {
              used += border->get_key(rec_pos).length();
            }
          }

          const auto& iksuf = border->iksuf_[0];
          allocated += sizeof(border_node) + iksuf.capacity();
          if (border->ksuf_) {
            allocated += border->ksuf_->capacity();
          }
        } else {
          constexpr size_t kBaseSize = 21;
          const auto n = interior->size();
          used += kBaseSize + n * kPartKeySize + (n + 1) * kPaySize;
          allocated += sizeof(interior_node);
        }
        ++node_cnt;

        stack.pop_back();
      }
      layers.pop_back();
    out:;
    }

    return usage;
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

    Iterator(  //
        Table* table,
        std::vector<ScanRecord>* records,
        std::optional<std::string> e_key,
        const bool e_closed)
        : table_{table}
        , records_{records}
        , e_key_{std::move(e_key)}
        , e_closed_{e_closed}
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
     * Public operators for iterators
     *########################################################################*/

    explicit
    operator bool()
    {
      while (true) {
        const size_t size = records_->size();
        if (pos_ < size) {
          if (!e_key_) return true;
          const auto& key = (*records_)[pos_].first;
          const auto& end_key = *e_key_;
          if (key < end_key || (e_closed_ && key == end_key)) return true;
          records_->clear();
          return false;
        }
        if (size < kScanSize) return false;

        const auto key = records_->back().first;
        Scanner scanner{kScanSize, records_};
        table_->table().scan(key, false, scanner, *ti);
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
      const auto& [bin_key, payload] = (*records_)[pos_];
      const auto& key = index::ConvertFromBinaryData<Key>(bin_key.data());
      return {key, payload};
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    Table* table_{};

    std::vector<ScanRecord>* records_{};

    size_t pos_{};

    std::optional<std::string> e_key_{};

    bool e_closed_{};
  };

  class Scanner
  {
   public:
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    Scanner(  //
        const int32_t scan_size,
        std::vector<ScanRecord>* records)
        : num_remain_(scan_size)
        , records_(records)
    {
      records_->clear();
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
        Str str,
        row_type* value,
        [[maybe_unused]] threadinfo& th)  //
        -> bool
    {
      if (row_is_marker(value)) return true;

      Payload payload{};
      memcpy(&payload, value->col(0).s, sizeof(Payload));
      payload = index::ByteSwap(payload);
      records_->emplace_back(std::string{str.s, static_cast<size_t>(str.len)}, payload);

      return (--num_remain_) > 0;
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    int32_t num_remain_{};

    std::vector<ScanRecord>* records_{};
  };

 private:
  /*##########################################################################*
   * Internal constants
   *##########################################################################*/

  static constexpr uint32_t kInitialHeight = 8;

  static constexpr uint32_t kMaxDepth = kMaxVarLenSize / sizeof(uint64_t);

  static constexpr size_t kPartKeySize = 8;

  static constexpr size_t kPaySize = 8;

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
   * Internal static variables
   *##########################################################################*/

  static thread_local inline threadinfo* ti{};

  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  Query index_{};

  Table table_{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WRAPPERS_MASSTREE_BETA_WRAPPER_HPP_
