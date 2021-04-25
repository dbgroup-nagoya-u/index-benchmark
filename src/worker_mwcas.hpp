// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <chrono>
#include <random>
#include <utility>
#include <vector>

#include "common.hpp"
#include "mwcas/mwcas_descriptor.hpp"
#include "worker.hpp"

class WorkerMwCAS : public Worker
{
 protected:
  /*################################################################################################
   * Inherited utility functions
   *##############################################################################################*/

  void
  ReadMwCASField(const size_t index) override
  {
    const auto addr = shared_fields_ + index;
    dbgroup::atomic::mwcas::ReadMwCASField<size_t>(addr);
  }

  void
  PerformMwCAS(const std::array<size_t, kMaxTargetNum>& target_fields) override
  {
    while (true) {
      dbgroup::atomic::mwcas::MwCASDescriptor desc;
      for (size_t i = 0; i < target_field_num_; ++i) {
        const auto addr = shared_fields_ + target_fields[i];
        const auto old_val = dbgroup::atomic::mwcas::ReadMwCASField<size_t>(addr);
        const auto new_val = old_val + 1;
        desc.AddMwCASTarget(addr, old_val, new_val);
      }
      if (desc.MwCAS()) break;
    }
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  WorkerMwCAS(  //
      size_t* shared_fields,
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
