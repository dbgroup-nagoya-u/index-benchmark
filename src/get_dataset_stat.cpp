/*
 * Copyright 2026 Database Group, Nagoya University
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

// C++ standard libraries
#include <algorithm>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

auto
main(  //
    int argc,
    char** argv)  //
    -> int
{
  if (argc != 2) {
    std::cerr << "This binary requires a dataset file path.\n";
    return 1;
  }

  constexpr size_t kDefaultRowNum = 1E7;
  std::ifstream ifs{argv[1]};
  std::vector<size_t> lengths{};
  lengths.reserve(kDefaultRowNum);
  std::string line{};
  while (std::getline(ifs, line)) {
    lengths.emplace_back(line.size() + 1);
  }

  const auto n = lengths.size();
  std::sort(lengths.begin(), lengths.end());
  // NOLINTBEGIN
  std::cout << "# of rows: " << n << "\n"
            << "length: " << "\n"
            << "  min: " << lengths[0] << "\n"
            << "  p25: " << lengths[n * 0.25] << "\n"
            << "  p50: " << lengths[n * 0.50] << "\n"
            << "  p75: " << lengths[n * 0.75] << "\n"
            << "  p90: " << lengths[n * 0.90] << "\n"
            << "  p95: " << lengths[n * 0.95] << "\n"
            << "  p99: " << lengths[n * 0.99] << "\n"
            << "  max: " << lengths[n - 1] << "\n";
  // NOLINTEND
  return 0;
}
