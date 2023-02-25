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

#ifndef INDEX_BENCHMARK_CLA_VALIDATOR_HPP
#define INDEX_BENCHMARK_CLA_VALIDATOR_HPP

#include <filesystem>
#include <iostream>
#include <string>

#include "common.hpp"

template <class Number>
auto
ValidateNonZero(  //
    const char *flagname,
    const Number value)  //
    -> bool
{
  if (value != 0) return true;

  std::cerr << "A value must be not zero for " << flagname << std::endl;
  return false;
}

auto
ValidateKeySize(  //
    [[maybe_unused]] const char *flagname,
    const uint64_t value)  //
    -> bool
{
  if (value == 8) return true;

  if (dbgroup::kUseIntegerKeys) {
    std::cerr << "The key size is invalid (only 8 is allowed for comparing with the SOTA indexes)."
              << std::endl;
    return false;
  }

  if (!dbgroup::kBuildLongKeys) {
    std::cerr << "The key size is invalid (long keys have not been built)." << std::endl;
    return false;
  }

  if (value == 16 || value == 32 || value == 64 || value == 128) return true;
  std::cerr << "The specified key size is invalid (only 8, 16, 32, 64, and 128 are allowed)."
            << std::endl;
  return false;
}

auto
ValidateRandomSeed(  //
    [[maybe_unused]] const char *flagname,
    const std::string &seed)  //
    -> bool
{
  if (seed.empty()) return true;

  for (size_t i = 0; i < seed.size(); ++i) {
    if (!std::isdigit(seed[i])) {
      std::cerr << "A random seed must be unsigned integer type" << std::endl;
      return false;
    }
  }
  return true;
}

auto
ValidateWorkload(  //
    [[maybe_unused]] const char *flagname,
    const std::string &workload)  //
    -> bool
{
  if (workload.empty()) {
    std::cerr << "A workload file is not specified." << std::endl;
    return false;
  }

  const auto abs_path = std::filesystem::absolute(workload);
  if (!std::filesystem::exists(abs_path)) {
    std::cerr << "The specified file does not exist." << std::endl;
    return false;
  }

  return true;
}

#endif  // INDEX_BENCHMARK_CLA_VALIDATOR_HPP
