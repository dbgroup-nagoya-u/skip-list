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

// the corresponding header
#include "skip_list/component/node_on_pmem.hpp"

// external sources
#include "gtest/gtest.h"
#include "pmwcas/descriptor_pool.hpp"

// our fixture files
#include "external/index-fixtures/common.hpp"

// local sources
#include "common_pmem.hpp"

namespace dbgroup::index::skip_list::component::test
{
// prepare a temporary directory
auto *const env = testing::AddGlobalTestEnvironment(new TmpDirManager);

/*######################################################################################
 * Global constants
 *####################################################################################*/

constexpr size_t kKeyNumForTest = 10;

/*######################################################################################
 * Fixture classes
 *####################################################################################*/

template <class KeyType, class PayloadType>
struct KeyPayload {
  using Key = KeyType;
  using Payload = PayloadType;
};

template <class KeyPayload>
class NodeFixture : public testing::Test
{
 protected:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using Key = typename KeyPayload::Key::Data;
  using Payload = typename KeyPayload::Payload::Data;
  using KeyComp = typename KeyPayload::Key::Comp;
  using PayComp = typename KeyPayload::Payload::Comp;
  using Node_t = NodeOnPMEM<Key, Payload, KeyComp>;
  using DescriptorPool = ::dbgroup::atomic::pmwcas::DescriptorPool;

  /*####################################################################################
   * Constants
   *##################################################################################*/

  static constexpr size_t kLevel = 10;
  static constexpr auto kDelBit = Node_t::kDelBit;

  /*####################################################################################
   * Setup/Teardown
   *##################################################################################*/

  void
  SetUp() override
  {
    keys_ = ::dbgroup::index::test::PrepareTestData<Key>(kKeyNumForTest);
    payloads_ = ::dbgroup::index::test::PrepareTestData<Payload>(kKeyNumForTest);

    // prepare initial data
    const auto &key = keys_.at(0);
    const auto &payload = payloads_.at(0);
    const auto key_len = ::dbgroup::index::test::GetLength(key);
    const auto pay_len = ::dbgroup::index::test::GetLength(payload);

    // create a persistent pool for testing
    auto &&pool_path = GetTmpPoolPath();
    pool_path /= kTestName;
    if (std::filesystem::exists(pool_path)) {
      pop_ = pmemobj_open(pool_path.c_str(), kTestName);
    } else {
      pop_ = pmemobj_create(pool_path.c_str(), kTestName, PMEMOBJ_MIN_POOL, kModeRW);
    }

    // prepare a node
    auto &&root = pmemobj_root(pop_, sizeof(Node_t) + kLevel * kWordSize);
    node_ = new (pmemobj_direct(root))
        Node_t{root.pool_uuid_lo, kLevel, key, key_len, payload, pay_len, pop_};

    // prepare a temporary region
    AllocatePmem(pop_, &oid_, sizeof(PMEMoid));

    // prepare a PMwCAS descriptor pool
    auto &&desc_path = GetTmpPoolPath();
    desc_path /= "pmwcas";
    desc_pool_ = std::make_unique<DescriptorPool>(desc_path);
  }

  void
  TearDown() override
  {
    pmemobj_free(&oid_);
    pmemobj_close(pop_);

    ::dbgroup::index::test::ReleaseTestData(keys_);
    ::dbgroup::index::test::ReleaseTestData(payloads_);
  }

  /*####################################################################################
   * Utility functions
   *##################################################################################*/

  void
  ConstructorTest()
  {
    EXPECT_EQ(node_->GetLevel(), kLevel);
    EXPECT_FALSE(node_->IsDeleted());

    // check a key
    EXPECT_TRUE(node_->LT(keys_.at(1)));
    EXPECT_FALSE(node_->GT(keys_.at(1)));

    // check a payload
    Payload payload{};
    EXPECT_TRUE(node_->Read(payload));
    EXPECT_TRUE(IsEqual<PayComp>(payloads_.at(0), payload));
  }

  void
  StoreNextTest()
  {
    for (size_t i = 0; i < kLevel; ++i) {
      node_->StoreNext(i, node_);
      EXPECT_EQ(node_, node_->GetNext(i));
    }
  }

  void
  CASNextTest()
  {
    for (size_t i = 0; i < kLevel; ++i) {
      node_->StoreNext(i, nullptr);
      EXPECT_FALSE(node_->CASNext(i, node_, node_, desc_pool_.get()));
      EXPECT_TRUE(node_->CASNext(i, nullptr, node_, desc_pool_.get()));
      EXPECT_EQ(node_, node_->GetNext(i));
    }
  }

  void
  DeleteNextTest()
  {
    for (size_t i = 0; i < kLevel; ++i) {
      node_->StoreNext(i, node_);
      EXPECT_EQ(node_, node_->DeleteNext(i, desc_pool_.get()));
      EXPECT_EQ(node_, node_->GetNext(i));
    }
  }

  void
  UpdateTest()
  {
    Payload tmp_pay{};
    const auto &payload = payloads_.at(1);
    const auto pay_len = ::dbgroup::index::test::GetLength(payload);

    auto *oid = reinterpret_cast<PMEMoid *>(pmemobj_direct(oid_));
    const auto old_v = node_->Update(payload, pay_len, desc_pool_.get(), pop_, oid);
    ASSERT_EQ(0UL, old_v & kDelBit);
    EXPECT_FALSE(node_->IsDeleted());
    EXPECT_TRUE(node_->Read(tmp_pay));
    EXPECT_TRUE(IsEqual<PayComp>(payload, tmp_pay));

    if constexpr (!CanCAS<Payload>()) {
      pmemobj_free(oid);
    }
  }

  void
  DeleteTest()
  {
    Payload tmp_pay;
    const auto &payload = payloads_.at(1);
    const auto pay_len = ::dbgroup::index::test::GetLength(payload);

    auto *oid = reinterpret_cast<PMEMoid *>(pmemobj_direct(oid_));
    oid->off = 0UL;
    EXPECT_TRUE(node_->Delete(desc_pool_.get(), oid));
    EXPECT_TRUE(node_->IsDeleted());
    EXPECT_FALSE(node_->Read(tmp_pay));
    const auto old_v = node_->Update(payload, pay_len, desc_pool_.get(), pop_, oid);
    EXPECT_EQ(kDelBit, old_v & kDelBit);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::vector<Key> keys_{};

  std::vector<Payload> payloads_{};

  std::unique_ptr<DescriptorPool> desc_pool_{nullptr};

  PMEMobjpool *pop_{nullptr};

  PMEMoid oid_{OID_NULL};

  Node_t *node_{nullptr};
};

/*######################################################################################
 * Preparation for typed testing
 *####################################################################################*/

using UInt8 = ::dbgroup::index::test::UInt8;
using UInt4 = ::dbgroup::index::test::UInt4;
using Int8 = ::dbgroup::index::test::Int8;
using Var = ::dbgroup::index::test::Var;
using Ptr = ::dbgroup::index::test::Ptr;
using Original = ::dbgroup::index::test::Original;

using KeyPayloadPairs = ::testing::Types<  //
    KeyPayload<Int8, Int8>,                // 8byte keys/payloads
    KeyPayload<UInt4, Int8>,               // small keys
    KeyPayload<Int8, UInt4>,               // small payloads
    KeyPayload<UInt4, UInt4>,              // small keys/payloads
    KeyPayload<Var, Int8>,                 // variable length keys
    KeyPayload<Int8, Var>,                 // variable length payloads
    KeyPayload<Var, Var>,                  // variable length keys/payloads
    KeyPayload<Original, Original>         // original class keys/payloads
    >;
TYPED_TEST_SUITE(NodeFixture, KeyPayloadPairs);

/*######################################################################################
 * Test definitions
 *####################################################################################*/

TYPED_TEST(NodeFixture, ConstructorSetInitialValues) { TestFixture::ConstructorTest(); }

TYPED_TEST(NodeFixture, StoreNextSetNewNextNodes) { TestFixture::StoreNextTest(); }

TYPED_TEST(NodeFixture, CASNextSetNewNextNodes) { TestFixture::CASNextTest(); }

TYPED_TEST(NodeFixture, DeleteNextDeleteNextLinks) { TestFixture::DeleteNextTest(); }

TYPED_TEST(NodeFixture, UpdatePayloadValue) { TestFixture::UpdateTest(); }

TYPED_TEST(NodeFixture, DeletePayloadValue) { TestFixture::DeleteTest(); }

}  // namespace dbgroup::index::skip_list::component::test
