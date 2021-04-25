// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <chrono>
#include <random>
#include <utility>
#include <vector>

#include "common.hpp"
#include "worker.hpp"

class WorkerSingleCAS : public Worker
{
 protected:
  /*################################################################################################
   * Inherited utility functions
   *##############################################################################################*/

  void
  ReadMwCASField(const size_t index) override
  {
    const auto addr = shared_fields_ + index;
    reinterpret_cast<std::atomic_size_t *>(addr)->load(std::memory_order_relaxed);
  }

  void
  PerformMwCAS(const std::array<size_t, kMaxTargetNum> &target_fields) override
  {
    for (size_t i = 0; i < target_field_num_; ++i) {
      const auto addr = shared_fields_ + target_fields[i];
      auto target = reinterpret_cast<std::atomic_size_t *>(addr);
      auto old_val = target->load(std::memory_order_relaxed);
      size_t new_val;
      do {
        new_val = old_val + 1;
      } while (!target->compare_exchange_weak(old_val, new_val, std::memory_order_relaxed));
    }
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  WorkerSingleCAS(  //
      size_t *shared_fields,
      const size_t shared_field_num,
      const size_t target_field_num,
      const size_t read_ratio,
      const size_t operation_counts,
      const size_t loop_num,
      const size_t random_seed = 0)
      : Worker{shared_fields,    shared_field_num, target_field_num, read_ratio,
               operation_counts, loop_num,         random_seed}
  {
  }
};
