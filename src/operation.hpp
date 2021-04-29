// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "common.hpp"

/**
 * @brief A class to represent index read/write operations.
 *
 */
struct Operation {
 public:
  /*################################################################################################
   * Public member variables
   *##############################################################################################*/

  /// a read/write operation to perform
  const IndexOperation type;

  /// a target key of this operation
  const uint64_t key;

  /// a target data of this operation
  const uint64_t value = 0;

  ///  a key to represent the end of a scan operation
  const uint64_t end_key = 0;
};
