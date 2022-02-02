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
ValidatePositiveVal(const char *flagname, const Number value)  //
    -> bool
{
  if (value >= 0) {
    return true;
  }
  std::cout << "A value must be positive for " << flagname << std::endl;
  return false;
}

template <class Number>
auto
ValidateNonZero(const char *flagname, const Number value)  //
    -> bool
{
  if (value != 0) {
    return true;
  }
  std::cout << "A value must be not zero for " << flagname << std::endl;
  return false;
}

auto
ValidateRandomSeed([[maybe_unused]] const char *flagname, const std::string &seed)  //
    -> bool
{
  if (seed.empty()) {
    return true;
  }

  for (size_t i = 0; i < seed.size(); ++i) {
    if (!std::isdigit(seed[i])) {
      std::cout << "A random seed must be unsigned integer type" << std::endl;
      return false;
    }
  }
  return true;
}

auto
ValidateWorkload(const std::string &workload)  //
    -> bool
{
  if (workload.empty()) {
    std::cout << "NOTE: a workload file is not specified." << std::endl;
    std::cout << "NOTE: use a read-only workload." << std::endl << std::endl;
    return false;
  }

  const auto abs_path = std::filesystem::absolute(workload);
  if (!std::filesystem::exists(abs_path)) {
    std::cout << "NOTE: the specified file does not exist." << std::endl;
    std::cout << "NOTE: use a read-only workload." << std::endl << std::endl;
    return false;
  }

  return true;
}

#endif  // INDEX_BENCHMARK_CLA_VALIDATOR_HPP