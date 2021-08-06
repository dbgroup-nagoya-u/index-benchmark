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

#pragma once

#include <fstream>
#include <string>

#include "common.hpp"
#include "nlohmann/json.hpp"

/**
 * @brief A class to represent workload for benchmarking.
 *
 */
struct Workload {
 public:
  /*################################################################################################
   * Internal member variables
   *##############################################################################################*/

  const size_t read_ratio;

  const size_t scan_ratio;

  const size_t write_ratio;

  const size_t insert_ratio;

  const size_t update_ratio;

  const size_t delete_ratio;

  /*################################################################################################
   * Public builders
   *##############################################################################################*/

  static Workload
  CreateWorkloadFromJson(const std::string &filename)
  {
    std::ifstream workload_in{filename};

    ::nlohmann::json workload_json;
    workload_in >> workload_json;
    workload_json = workload_json["operation_ratio"];

    return Workload{workload_json["read"],   workload_json["scan"],   workload_json["write"],
                    workload_json["insert"], workload_json["update"], workload_json["delete"]};
  }
};
