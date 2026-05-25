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

// the corresponding header
#include "wrappers/masstree_beta_wrapper.hpp"

// external libraries
#include <dbgroup/index_fixtures/index_fixture.hpp>

namespace dbgroup::index::test
{
/*############################################################################*
 * Preparation for typed testing
 *############################################################################*/

template <class K, class V, class C, class... Others>
using Index = index_bench::MasstreeBetaWrapper<K, V>;

using TestTargets = ::testing::Types<IndexInfo<Index, UInt8, UInt8> >;
TYPED_TEST_SUITE(IndexFixture, TestTargets);

/*############################################################################*
 * Unit test definitions
 *############################################################################*/

#include <dbgroup/index_fixtures/index_fixture_test_definitions.hpp>

}  // namespace dbgroup::index::test
