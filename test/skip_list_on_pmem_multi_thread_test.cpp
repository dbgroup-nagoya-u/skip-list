/*
 * Copyright 2023 Database Group, Nagoya University
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

#include "index_wrapper.hpp"

// external sources
#include "external/index-fixtures/index_fixture_multi_thread.hpp"

namespace dbgroup::index::test
{
// prepare a temporary directory
auto *const env = testing::AddGlobalTestEnvironment(new TmpDirManager);

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

template <class K, class V, class C>
using Index = IndexWrapper<K, V, C>;

using UInt8 = ::dbgroup::index::test::UInt8;
using UInt4 = ::dbgroup::index::test::UInt4;
using Int8 = ::dbgroup::index::test::Int8;
using Var = ::dbgroup::index::test::Var;
using Ptr = ::dbgroup::index::test::Ptr;
using Original = ::dbgroup::index::test::Original;

using TestTargets = ::testing::Types<     //
    IndexInfo<Index, Int8, Int8>,         // 8byte keys/payloads
    IndexInfo<Index, UInt4, Int8>,        // small keys
    IndexInfo<Index, Int8, UInt4>,        // small payloads
    IndexInfo<Index, UInt4, UInt4>,       // small keys/payloads
    IndexInfo<Index, Var, Int8>,          // variable length keys
    IndexInfo<Index, Int8, Var>,          // variable length payloads
    IndexInfo<Index, Var, Var>,           // variable length keys/payloads
    IndexInfo<Index, Ptr, Ptr>,           // pointer keys/payloads
    IndexInfo<Index, Original, Original>  // original class keys/payloads
    >;

TYPED_TEST_SUITE(IndexMultiThreadFixture, TestTargets);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

#include "external/index-fixtures/index_fixture_multi_thread_test_definitions.hpp"

}  // namespace dbgroup::index::test
