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

#ifndef INDEX_BENCHMARK_WORKLOAD_OPERATION_ENGINE_HPP
#define INDEX_BENCHMARK_WORKLOAD_OPERATION_ENGINE_HPP

// C++ standard libraries
#include <chrono>
#include <fstream>
#include <random>
#include <tuple>

// external libraries
#include "yaml-cpp/yaml.h"

// local sources

namespace dbgroup::index_bench
{
/**
 * @brief A class to represent index read/write operations.
 *
 */
template <class Workload>
class OperationEngine
{
 public:
  /*##########################################################################*
   * Public types
   *##########################################################################*/

  using Key = Workload::Key_t;
  using OPType = dbgroup::index_bench::OPType;
  using Operation = std::tuple<const Key &, size_t, size_t>;

  /**
   * @brief A class for iterating an operation queue.
   *
   * @note Our benchmark template requires this type.
   */
  class OPIter
  {
   public:
    /*########################################################################*
     * Public constructors and assignment operators
     *########################################################################*/

    /**
     * @param rand_seed A random seed.
     */
    OPIter(  //
        const size_t thread_id,
        const size_t rand_seed,
        const Workload &workload)
        : thread_id_{thread_id}, rand_{rand_seed}, workload_{workload}
    {
    }

    OPIter(const OPIter &) = delete;
    OPIter(OPIter &&) noexcept = default;

    auto operator=(const OPIter &obj) -> OPIter & = delete;
    auto operator=(OPIter &&) noexcept -> OPIter & = default;

    /*########################################################################*
     * Public destructor
     *########################################################################*/

    ~OPIter() = default;

    /*########################################################################*
     * Public APIs
     *########################################################################*/

    /**
     * @retval true if this iterator has other operations.
     * @retval false otherwise.
     */
    [[nodiscard]] explicit
    operator bool() const
    {
      return static_cast<bool>(workload_);
    }

    /**
     * @retval 1st: The current operation type.
     * @retval 2nd: Operation arguments.
     */
    [[nodiscard]] auto
    operator*()  //
        -> std::pair<OPType, Operation>
    {
      return {workload_.GetType(thread_id_, rand_), workload_.GetOps(rand_)};
    }

    /**
     * @brief Advance this iterator.
     *
     * @return Oneself.
     */
    constexpr auto
    operator++()  //
        -> OPIter &
    {
      return *this;
    }

   private:
    /*########################################################################*
     * Internal member variables
     *########################################################################*/

    size_t thread_id_{};

    /// @brief A random value generator.
    std::mt19937_64 rand_{};

    const Workload &workload_;
  };

  /*##########################################################################*
   * Public constructors and assignment operators
   *##########################################################################*/

  OperationEngine() = default;

  OperationEngine(  //
      Workload workload)
      : workload_{std::move(workload)}
  {
  }

  OperationEngine(OperationEngine &&) noexcept = default;
  auto operator=(OperationEngine &&) noexcept -> OperationEngine & = default;

  // disable copying
  OperationEngine(const OperationEngine &) = delete;
  auto operator=(const OperationEngine &) -> OperationEngine & = delete;

  /*##########################################################################*
   * Public destructors
   *##########################################################################*/

  ~OperationEngine() = default;

  /*##########################################################################*
   * Public getters
   *##########################################################################*/

  auto
  GetOPIter(  //
      const size_t thread_id,
      const size_t rand_seed) const  //
      -> OPIter
  {
    return OPIter{thread_id, rand_seed, workload_};
  }

  auto
  CreateInitData() const  //
      -> std::tuple<size_t, bool, std::vector<std::tuple<const Key &, Payload, size_t>>>
  {
    return workload_.CreateInitData();
  }

 private:
  /*##########################################################################*
   * Internal member variables
   *##########################################################################*/

  Workload workload_{};
};

}  // namespace dbgroup::index_bench

#endif  // INDEX_BENCHMARK_WORKLOAD_OPERATION_ENGINE_HPP
