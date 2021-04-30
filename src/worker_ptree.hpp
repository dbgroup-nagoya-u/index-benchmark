// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <random>
#include <utility>
#include <vector>

#include "common.hpp"
#include "worker.hpp"

class WorkerPTree : public Worker
{
 private:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

 protected:
  /*################################################################################################
   * Inherited utility functions
   *##############################################################################################*/

  void
  Read(const Key key) override
  {
  }

  void
  Scan(const Key begin_key, const Key end_key) override
  {
  }

  void
  Write(const Key key, const Value value) override
  {
  }

  void
  Insert(const Key key, const Value value) override
  {
  }

  void
  Update(const Key key, const Value value) override
  {
  }

  void
  Delete(const Key key) override
  {
  }

 public:
  /*################################################################################################
   * Public constructors/destructors
   *##############################################################################################*/

  WorkerPTree(  //
      const Workload workload,
      const size_t total_key_num,
      const double skew_parameter,
      const size_t operation_counts,
      const size_t random_seed = 0)
      : Worker{workload, total_key_num, skew_parameter, operation_counts, random_seed}
  {
  }
};
