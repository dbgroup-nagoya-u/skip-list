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
#include "skip_list/component/node.hpp"

// external sources
#include "gtest/gtest.h"

// our fixture files
#include "external/index-fixtures/common.hpp"

namespace dbgroup::index::skip_list::component::test
{
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
  using Node_t = Node<Key, Payload, KeyComp>;

  /*####################################################################################
   * Constants
   *##################################################################################*/

  static constexpr size_t kLevel = 10;

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

    // prepare a node
    node_ = ::dbgroup::memory::Allocate<Node_t>(sizeof(Node_t) + kLevel * kWordSize);
    new (node_) Node_t{kLevel, key, key_len, payload, pay_len};
  }

  void
  TearDown() override
  {
    ::dbgroup::memory::Release<Node_t>(node_);

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
      EXPECT_FALSE(node_->CASNext(i, node_, node_));
      EXPECT_TRUE(node_->CASNext(i, nullptr, node_));
      EXPECT_EQ(node_, node_->GetNext(i));
    }
  }

  void
  DeleteNextTest()
  {
    for (size_t i = 0; i < kLevel; ++i) {
      node_->StoreNext(i, node_);
      EXPECT_EQ(node_, node_->DeleteNext(i));
      EXPECT_EQ(node_, node_->GetNext(i));
    }
  }

  void
  UpdateTest()
  {
    Payload tmp_pay{};
    const auto &payload = payloads_.at(1);
    const auto pay_len = ::dbgroup::index::test::GetLength(payload);

    const auto old_v = node_->Update(payload, pay_len);
    ASSERT_EQ(0UL, old_v & kDelBit);
    EXPECT_FALSE(node_->IsDeleted());
    EXPECT_TRUE(node_->Read(tmp_pay));
    EXPECT_TRUE(IsEqual<PayComp>(payload, tmp_pay));

    if constexpr (!CanCAS<Payload>()) {
      using PayWOPtr = std::remove_pointer_t<Payload>;
      ::dbgroup::memory::Release<PayWOPtr>(reinterpret_cast<PayWOPtr *>(old_v));
    }
  }

  void
  DeleteTest()
  {
    Payload tmp_pay;
    const auto &payload = payloads_.at(1);
    const auto pay_len = ::dbgroup::index::test::GetLength(payload);

    EXPECT_TRUE(node_->Delete());
    EXPECT_TRUE(node_->IsDeleted());
    EXPECT_FALSE(node_->Read(tmp_pay));
    const auto old_v = node_->Update(payload, pay_len);
    EXPECT_EQ(kDelBit, old_v & kDelBit);
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  std::vector<Key> keys_{};

  std::vector<Payload> payloads_{};

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
    KeyPayload<Ptr, Ptr>,                  // pointer keys/payloads
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
