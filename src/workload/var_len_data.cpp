/*
 * Copyright 2025 Database Group, Nagoya University
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

// the corresponding header
#include "workload/var_len_data.hpp"

// C++ standard libraries
#include <cstdint>
#include <cstring>
#include <string>

namespace dbgroup::index_bench
{
/*############################################################################*
 * Constructors
 *############################################################################*/

VarLenData::VarLenData(  //
    const char *src)
    : len{static_cast<uint8_t>(std::strlen(src))}
{
  std::memcpy(data, src, len);
}

VarLenData::VarLenData(  //
    const std::string &src)
    : len{static_cast<uint8_t>(src.length())}
{
  std::memcpy(data, src.data(), len);
}

/*############################################################################*
 * Comparison operators
 *############################################################################*/

auto
VarLenData::operator<(            //
    const VarLenData &rhs) const  //
    -> bool
{
  return std::strcmp(data, rhs.data) < 0;
}

auto
VarLenData::operator>(            //
    const VarLenData &rhs) const  //
    -> bool
{
  return std::strcmp(data, rhs.data) > 0;
}

auto
VarLenData::operator==(           //
    const VarLenData &rhs) const  //
    -> bool
{
  return std::strcmp(data, rhs.data) == 0;
}

auto
VarLenData::operator!=(           //
    const VarLenData &rhs) const  //
    -> bool
{
  return std::strcmp(data, rhs.data) != 0;
}

}  // namespace dbgroup::index_bench
