// Copyright (c) Database Group, Nagoya University. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>

enum Operation
{
  kRead,
  kWrite
};

enum BenchTarget
{
  kOurs,
  kMicrosoft,
  kSingleCAS
};

#ifdef MWCAS_BENCH_MAX_FIELD_NUM
constexpr size_t kMaxTargetNum = MWCAS_BENCH_MAX_FIELD_NUM;
#else
constexpr size_t kMaxTargetNum = 8;
#endif
