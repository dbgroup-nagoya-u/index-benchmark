// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string>

#include "common.hpp"

/**
 * @brief A class to represent workload for benchmarking.
 *
 */
struct Workload {
 public:
  /*################################################################################################
   * Public member variables
   *##############################################################################################*/

  const size_t read_ratio = 100;

  const size_t scan_ratio = 100;

  const size_t write_ratio = 100;

  const size_t insert_ratio = 100;

  const size_t update_ratio = 100;

  const size_t delete_ratio = 100;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  static Workload
  CreateWorkloadFromJson([[maybe_unused]] const std::string &filename)
  {
    return Workload{};
  }
};
