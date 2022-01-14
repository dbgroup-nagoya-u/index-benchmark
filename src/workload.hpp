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

#ifndef INDEX_BENCHMARK_WORKLOAD_HPP
#define INDEX_BENCHMARK_WORKLOAD_HPP

#include <fstream>
#include <iostream>
#include <string>

#include "common.hpp"
#include "nlohmann/json.hpp"

/**
 * @brief A class to represent workload for benchmarking.
 *
 */
struct Workload {
 public:
  /*####################################################################################
   * Public builders
   *##################################################################################*/

  static auto
  CreateWorkloadFromJson(const std::string &filename)  //
      -> Workload
  {
    std::ifstream workload_in{filename};

    ::nlohmann::json workload_json;
    workload_in >> workload_json;
    workload_json = workload_json["operation_ratio"];

    size_t read = workload_json["read"];
    size_t scan = workload_json["scan"];
    scan += read;
    size_t write = workload_json["write"];
    write += scan;
    size_t insert = workload_json["insert"];
    insert += write;
    size_t update = workload_json["update"];
    update += insert;
    size_t del = workload_json["delete"];
    del += update;

    if (del != 100UL) {
      std::cout << "WARN: The sum of the ratios of a workload is a hundred." << std::endl;
    }

    return Workload{read, scan, write, insert, update, del};
  }

  /*####################################################################################
   * Public member variables
   *##################################################################################*/

  size_t read_ratio{100};

  size_t scan_ratio{0};

  size_t write_ratio{0};

  size_t insert_ratio{0};

  size_t update_ratio{0};

  size_t delete_ratio{0};
};

#endif  // INDEX_BENCHMARK_WORKLOAD_HPP
