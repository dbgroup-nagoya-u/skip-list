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

#include "skip_list/skip_list.hpp"

// external sources
#include "external/index-fixtures/index_fixture_multi_thread.hpp"

namespace dbgroup::index::test
{
/*######################################################################################
 * Dummy struct for preventing some tests
 *####################################################################################*/

struct ImplStat {
};

template <>
constexpr auto
HasBulkloadOperation<ImplStat>()  //
    -> bool
{
  return false;
}

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

template <class K, class V, class C>
using SkipList = ::dbgroup::index::skip_list::SkipList<K, V, C>;

using UInt8 = ::dbgroup::index::test::UInt8;
using UInt4 = ::dbgroup::index::test::UInt4;
using Int8 = ::dbgroup::index::test::Int8;
using Var = ::dbgroup::index::test::Var;
using Ptr = ::dbgroup::index::test::Ptr;
using Original = ::dbgroup::index::test::Original;

using TestTargets = ::testing::Types<                  //
    IndexInfo<SkipList, Int8, Int8, ImplStat>,         // 8byte keys/payloads
    IndexInfo<SkipList, UInt4, Int8, ImplStat>,        // small keys
    IndexInfo<SkipList, Int8, UInt4, ImplStat>,        // small payloads
    IndexInfo<SkipList, UInt4, UInt4, ImplStat>,       // small keys/payloads
    IndexInfo<SkipList, Var, Int8, ImplStat>,          // variable length keys
    IndexInfo<SkipList, Int8, Var, ImplStat>,          // variable length payloads
    IndexInfo<SkipList, Var, Var, ImplStat>,           // variable length keys/payloads
    IndexInfo<SkipList, Ptr, Ptr, ImplStat>,           // pointer keys/payloads
    IndexInfo<SkipList, Original, Original, ImplStat>  // original class keys/payloads
    >;

TYPED_TEST_SUITE(IndexMultiThreadFixture, TestTargets);

/*######################################################################################
 * Unit test definitions
 *####################################################################################*/

#include "external/index-fixtures/index_fixture_multi_thread_test_definitions.hpp"

}  // namespace dbgroup::index::test
