// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <chrono>
#include <random>
#include <utility>
#include <vector>

#include "common.hpp"
#include "mwcas/mwcas.h"
#include "worker.hpp"

class WorkerPMwCAS : public Worker
{
 private:
  pmwcas::DescriptorPool& desc_pool_;

 protected:
  /*################################################################################################
   * Inherited utility functions
   *##############################################################################################*/

  void
  ReadMwCASField(const size_t index) override
  {
    const auto addr = shared_fields_ + index;
    auto epoch = desc_pool_.GetEpoch();
    reinterpret_cast<pmwcas::MwcTargetField<size_t>*>(addr)->GetValue(epoch);
  }

  void
  PerformMwCAS(const std::array<size_t, kMaxTargetNum>& target_fields) override
  {
    while (true) {
      auto desc = desc_pool_.AllocateDescriptor();
      auto epoch = desc_pool_.GetEpoch();
      epoch->Protect();
      for (size_t i = 0; i < target_field_num_; ++i) {
        const auto addr = shared_fields_ + target_fields[i];
        const auto old_val =
            reinterpret_cast<pmwcas::MwcTargetField<size_t>*>(addr)->GetValueProtected();
        const auto new_val = old_val + 1;
        desc->AddEntry(addr, old_val, new_val);
      }
      auto success = desc->MwCAS();
      epoch->Unprotect();
      if (success) break;
    }
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  WorkerPMwCAS(  //
      pmwcas::DescriptorPool& desc_pool,
      size_t* shared_fields,
      const size_t shared_field_num,
      const size_t target_field_num,
      const size_t read_ratio,
      const size_t operation_counts,
      const size_t loop_num,
      const size_t random_seed = 0)
      : Worker{shared_fields,    shared_field_num, target_field_num, read_ratio,
               operation_counts, loop_num,         random_seed},
        desc_pool_{desc_pool}
  {
  }
};
