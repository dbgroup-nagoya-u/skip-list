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

#ifndef SKIP_LIST_COMPONENT_NODE_ON_PMEM_HPP
#define SKIP_LIST_COMPONENT_NODE_ON_PMEM_HPP

// C++ standard libraries
#include <atomic>
#include <utility>

// external system libraries
#include <libpmem.h>
#include <libpmemobj.h>

// external sources
#include "memory/utility.hpp"
#include "pmwcas/descriptor_pool.hpp"

// local sources
#include "skip_list/component/common.hpp"

namespace dbgroup::index::skip_list::component
{
/**
 * @brief A class to represent record metadata.
 *
 */
template <class Key, class Payload, class Comp>
class NodeOnPMEM
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using KeyWOPtr = std::remove_pointer_t<Key>;
  using PayWOPtr = std::remove_pointer_t<Payload>;
  using DescriptorPool = ::dbgroup::atomic::pmwcas::DescriptorPool;

  /*####################################################################################
   * Public constants
   *##################################################################################*/

  /// @brief The most significant bit represents a deleted value.
  static constexpr uint64_t kDelBit = 1UL << 62UL;

  /*####################################################################################
   * Public classes
   *##################################################################################*/

  /**
   * @brief A dummy struct for representing internal pages.
   *
   */
  struct Target : public ::dbgroup::memory::DefaultTarget {
    // release keys/payloads in nodes
    using T = NodeOnPMEM;

    // nodes are stored in persistent memory
    static constexpr bool kOnPMEM = true;
  };

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new NodeOnPMEM object for a dummy head.
   *
   * @param pool_uuid_lo The UUID of a PMEMobjpool.
   * @param level The top level of this node.
   */
  NodeOnPMEM(  //
      const uint64_t pool_uuid_lo,
      const size_t level)
      : pool_id_{pool_uuid_lo}, level_{level}
  {
    // reset all the pointers
    memset(next_nodes_, 0, kWordSize * level);

    // flush the initial data
    pmem_flush(this, sizeof(NodeOnPMEM) + level * kWordSize);
  }

  /**
   * @brief Construct a new NodeOnPMEM object.
   *
   * @param pool_uuid_lo The UUID of a PMEMobjpool.
   * @param level The top level of this node.
   * @param key A target key.
   * @param key_len The length of the given key.
   * @param payload A target payload.
   * @param pay_len The length of the given payload.
   * @param pop A pool for managing persistent memory.
   */
  NodeOnPMEM(  //
      const uint64_t pool_uuid_lo,
      const size_t level,
      const Key &key,
      [[maybe_unused]] const size_t key_len,
      const Payload &payload,
      const size_t pay_len,
      PMEMobjpool *pop)
      : pool_id_{pool_uuid_lo}, level_{level}
  {
    // set a key value
    if constexpr (KeyIsInline()) {
      memcpy(&key_, &key, sizeof(Key));
    } else if constexpr (IsVarLenData<Key>()) {
      AllocatePmem(pop, &key_, key_len);
      memcpy(pmemobj_direct(key_), key, key_len);
    } else {
      AllocatePmem(pop, &key_, sizeof(Key));
      memcpy(pmemobj_direct(key_), &key, sizeof(Key));
    }

    // store a payload value
    PrepareWord(payload, pay_len, pop, &data_);

    // flush the initial data
    pmem_flush(this, sizeof(NodeOnPMEM) + level * kWordSize);
  }

  NodeOnPMEM(const NodeOnPMEM &) = delete;
  NodeOnPMEM(NodeOnPMEM &&) = delete;

  NodeOnPMEM &operator=(const NodeOnPMEM &) = delete;
  NodeOnPMEM &operator=(NodeOnPMEM &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the metadata object.
   *
   */
  ~NodeOnPMEM()
  {
    if constexpr (!KeyIsInline()) {
      pmemobj_free(&key_);
    }

    if constexpr (!CanCAS<Payload>()) {
      data_.off = data_.off & ~kDelBit;
      pmemobj_free(&data_);
    }
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @return The maximum level of this node.
   */
  [[nodiscard]] constexpr auto
  GetLevel() const  //
      -> size_t
  {
    return level_;
  }

  /**
   * @return The key stored in this node.
   */
  [[nodiscard]] constexpr auto
  GetKey() const  //
      -> Key
  {
    if constexpr (KeyIsInline()) {
      Key key{};
      memcpy(&key, reinterpret_cast<const void *>(&key_), sizeof(Key));
      return key;
    } else if constexpr (IsVarLenData<Key>()) {
      return reinterpret_cast<Key>(pmemobj_direct(key_));
    } else {
      return *reinterpret_cast<Key *>(pmemobj_direct(key_));
    }
  }

  /**
   * @param key A target key.
   * @retval true if the key of this node is less than the given key.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  LT(const Key &key) const  //
      -> bool
  {
    return Comp{}(GetKey(), key);
  }

  /**
   * @param key A target key.
   * @retval true if the key of this node is greater than the given key.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  GT(const Key &key) const  //
      -> bool
  {
    return Comp{}(key, GetKey());
  }

  /**
   * @retval true if the node is active.
   * @retval false if the payload has already been deleted.
   */
  [[nodiscard]] auto
  IsDeleted() const  //
      -> bool
  {
    const auto v = atomic::pmwcas::Read<uint64_t>(&(data_.off), std::memory_order_relaxed);
    return (v & kDelBit) > 0;
  }

  /**
   * @param level The level of the next node.
   * @return The pointer to the next node.
   */
  [[nodiscard]] auto
  GetNext(const size_t level) const  //
      -> NodeOnPMEM *
  {
    auto off = atomic::pmwcas::Read<uint64_t>(&(next_nodes_[level]), std::memory_order_acquire);
    return reinterpret_cast<NodeOnPMEM *>(pmemobj_direct(PMEMoid{pool_id_, off & ~kDelBit}));
  }

  /**
   * @brief Store the new pointer to the next node.
   *
   * @param level The level of the next node.
   * @param next The pointer to the next node.
   */
  void
  StoreNext(  //
      const size_t level,
      NodeOnPMEM *next)
  {
    next_nodes_[level] = pmemobj_oid(next).off;
    pmem_flush(&(next_nodes_[level]), kWordSize);
  }

  /**
   * @brief Compare and swap the next node.
   *
   * @param level The level of the next node.
   * @param expected The pointer to the current next node.
   * @param desired The pointer to the new next node.
   * @param pool A pool of PMwCAS descriptors.
   * @retval true if CAS succeeds.
   * @retval false otherwise.
   */
  auto
  CASNext(  //
      const size_t level,
      NodeOnPMEM *expected,
      NodeOnPMEM *desired,
      DescriptorPool *pool)  //
      -> bool
  {
    const auto expected_off = pmemobj_oid(expected).off;
    const auto new_off = pmemobj_oid(desired).off;

    auto *desc = pool->Get();
    desc->Add(&(next_nodes_[level]), expected_off, new_off, std::memory_order_release);
    return desc->PMwCAS();
  }

  /**
   * @brief Compare and delete the next node.
   *
   * @param level The level of the next node.
   * @param pool A pool of PMwCAS descriptors.
   * @return The next node.
   */
  auto
  DeleteNext(  //
      const size_t level,
      DescriptorPool *pool)  //
      -> NodeOnPMEM *
  {
    auto *addr = &(next_nodes_[level]);
    while (true) {
      const auto next = atomic::pmwcas::Read<uint64_t>(addr, std::memory_order_relaxed);
      assert((next & kDelBit) == 0);

      const auto del = next | kDelBit;
      auto *desc = pool->Get();
      desc->Add(addr, next, del, std::memory_order_acquire);
      if (desc->PMwCAS()) {
        return reinterpret_cast<NodeOnPMEM *>(pmemobj_direct(PMEMoid{pool_id_, next}));
      }
      SKIP_LIST_SPINLOCK_HINT
    }
  }

  /**
   * @brief Remove all the next pointers.
   *
   */
  void
  RemoveAllNextPointers()
  {
    pmem_memset_persist(next_nodes_, 0, kWordSize * level_);
  }

  /*####################################################################################
   * Read/Write APIs
   *##################################################################################*/

  /**
   * @brief Read the payload value in this node.
   *
   * @param[out] out_payload The container for a read payload.
   * @retval true if the payload is read.
   * @retval false if the payload has already been deleted.
   */
  auto
  Read(Payload &out_payload) const  //
      -> bool
  {
    auto off = atomic::pmwcas::Read<uint64_t>(&(data_.off), std::memory_order_acquire);
    if ((off & kDelBit) != 0) return false;  // the value has been deleted

    if constexpr (CanCAS<Payload>()) {
      memcpy(&out_payload, &off, kWordSize);
    } else if constexpr (IsVarLenData<Payload>()) {
      thread_local std::unique_ptr<PayWOPtr, std::function<void(Payload)>> tls_payload{
          ::dbgroup::memory::Allocate<PayWOPtr>(kMaxVarDataSize),
          ::dbgroup::memory::Release<PayWOPtr>};

      auto *ptr = reinterpret_cast<size_t *>(pmemobj_direct(PMEMoid{pool_id_, off}));
      out_payload = tls_payload.get();
      memcpy(out_payload, ShiftAddr(ptr, kHeaderLen), *ptr);
    } else {
      auto *ptr = reinterpret_cast<Payload *>(pmemobj_direct(PMEMoid{pool_id_, off}));
      out_payload = *ptr;
    }

    return true;
  }

  /**
   * @brief Update the stored payload by the given value.
   *
   * @param payload A payload value to be stored.
   * @param pay_len The length of the given payload.
   * @param pool A pool of PMwCAS descriptors.
   * @param pop A pool for managing persistent memory.
   * @param tmp_oid A temporary OID to avoid memory leaks.
   * @retval The old value if the payload is updated.
   * @retval kDelBit if the payload has already been deleted.
   */
  auto
  Update(  //
      const Payload &payload,
      const size_t pay_len,
      DescriptorPool *pool,
      PMEMobjpool *pop,
      PMEMoid *tmp_oid)  //
      -> uint64_t
  {
    auto *addr = &(data_.off);
    auto old_v = atomic::pmwcas::Read<uint64_t>(addr, std::memory_order_relaxed);
    if ((old_v & kDelBit) > 0) return kDelBit;  // the value has been deleted

    // the node is still active, so try updating a payload
    PrepareWord(payload, pay_len, pop, tmp_oid);
    do {
      auto *desc = pool->Get();
      desc->Add(addr, old_v, tmp_oid->off, std::memory_order_release);
      if constexpr (!CanCAS<Payload>()) {
        desc->Add(&(tmp_oid->off), tmp_oid->off, kNullPtr, std::memory_order_relaxed);
      }
      if (desc->PMwCAS()) return old_v;

      SKIP_LIST_SPINLOCK_HINT
      old_v = atomic::pmwcas::Read<uint64_t>(addr, std::memory_order_relaxed);
    } while ((old_v & kDelBit) == 0);

    // the value has been deleted
    if constexpr (!CanCAS<Payload>()) {
      pmemobj_free(tmp_oid);
    }
    return kDelBit;
  }

  /**
   * @brief Delete the stored payload.
   *
   * @param pool A pool of PMwCAS descriptors.
   * @param oid A temporary OID to avoid memory leak.
   * @retval true if the payload is deleted.
   * @retval false if the payload has already been deleted.
   */
  auto
  Delete(  //
      DescriptorPool *pool,
      PMEMoid *oid)  //
      -> bool
  {
    const auto del_off = pmemobj_oid(this).off;
    auto *addr = &(data_.off);
    auto old_v = atomic::pmwcas::Read<uint64_t>(addr, std::memory_order_relaxed);
    while ((old_v & kDelBit) == 0) {
      const auto del_v = old_v | kDelBit;
      auto *desc = pool->Get();
      desc->Add(addr, old_v, del_v, std::memory_order_relaxed);
      desc->Add(&(oid->off), kNullPtr, del_off, std::memory_order_relaxed);
      if (desc->PMwCAS()) return true;
      SKIP_LIST_SPINLOCK_HINT
    }

    return false;  // the value has been deleted
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// @brief The unsigned long of nullptr.
  static constexpr uintptr_t kNullPtr = 0;

  /// @brief The alignment for payload values.
  static constexpr size_t kAlign = alignof(PayWOPtr);

  /// @brief The size of a dynamically allocated payload header.
  static constexpr size_t kHeaderLen = kAlign > kWordSize ? kAlign : kWordSize;

  /*####################################################################################
   * Internal utilities
   *##################################################################################*/

  /**
   * @retval true if keys can be embedded in nodes.
   * @retval false otherwise.
   */
  static constexpr auto
  KeyIsInline()  //
      -> bool
  {
    return !IsVarLenData<Key>()               //
           && sizeof(Key) <= sizeof(PMEMoid)  //
           && alignof(Key) <= alignof(PMEMoid);
  }

  /**
   * @brief Create an inlined/pointer payload for the given value.
   *
   * @param payload A payload value to be stored.
   * @param pay_len The length of the given payload value.
   * @param pop A pool for managing persistent memory.
   * @param oid An OID to be stored a payload.
   */
  static void
  PrepareWord(  //
      const Payload &payload,
      [[maybe_unused]] const size_t pay_len,
      [[maybe_unused]] PMEMobjpool *pop,
      [[maybe_unused]] PMEMoid *oid)
  {
    if constexpr (CanCAS<Payload>()) {
      memcpy(&(oid->off), &payload, kWordSize);
      assert((oid->off & kDelBit) == 0);
    } else if constexpr (IsVarLenData<Payload>()) {
      AllocatePmem(pop, oid, kHeaderLen + pay_len);
      auto *ptr = pmemobj_direct(*oid);
      *reinterpret_cast<size_t *>(ptr) = pay_len;
      memcpy(ShiftAddr(ptr, kHeaderLen), payload, pay_len);
    } else {
      AllocatePmem(pop, oid, sizeof(Payload));
      memcpy(pmemobj_direct(*oid), &payload, sizeof(Payload));
    }
  }

  /*####################################################################################
   * Static assertions
   *##################################################################################*/

  static_assert((IsVarLenData<Key>() && alignof(KeyWOPtr) <= sizeof(PMEMoid))
                || (!IsVarLenData<Key>() && alignof(Key) <= sizeof(PMEMoid)));

  static_assert((IsVarLenData<Payload>() && alignof(PayWOPtr) <= sizeof(PMEMoid))
                || (!IsVarLenData<Payload>() && alignof(Payload) <= sizeof(PMEMoid)));

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// @brief The UUID of a PMEMobjpool.
  size_t pool_id_{0};

  /// @brief The top level of this node.
  size_t level_{0};

  /// @brief The key value stored in this node.
  PMEMoid key_{OID_NULL};

  /// @brief The payload value stored in this node.
  PMEMoid data_{OID_NULL};

  /// @brief The pointers to the next nodes in each level.
  uint64_t next_nodes_[0];
};

}  // namespace dbgroup::index::skip_list::component

#endif  // SKIP_LIST_COMPONENT_NODE_ON_PMEM_HPP
