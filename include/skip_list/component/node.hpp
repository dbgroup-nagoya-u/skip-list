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

#ifndef SKIP_LIST_COMPONENT_NODE_HPP
#define SKIP_LIST_COMPONENT_NODE_HPP

// C++ standard libraries
#include <atomic>
#include <utility>

// external sources
#include "memory/utility.hpp"

// local sources
#include "skip_list/component/common.hpp"

namespace dbgroup::index::skip_list::component
{
/**
 * @brief A class to represent record metadata.
 *
 */
template <class Key, class Payload, class Comp>
class Node
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using KeyWOPtr = std::remove_pointer_t<Key>;
  using PayWOPtr = std::remove_pointer_t<Payload>;

  /*####################################################################################
   * Public classes
   *##################################################################################*/

  /**
   * @brief A dummy struct for representing internal pages.
   *
   */
  struct Target : public ::dbgroup::memory::DefaultTarget {
    // release keys/payloads in nodes
    using T = Node;
  };

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new Node object for a dummy head.
   *
   * @param level The top level of this node.
   */
  explicit Node(const size_t level) : level_{level}
  {
    // reset all the pointers
    for (size_t i = 0; i < level_; ++i) {
      next_nodes_[i].store(kNullPtr, std::memory_order_relaxed);
    }
  }

  /**
   * @brief Construct a new Node object.
   *
   * @param level The top level of this node.
   * @param key A target key.
   * @param key_len The length of the given key.
   * @param payload A target payload.
   * @param pay_len The length of the given payload.
   */
  Node(  //
      const size_t level,
      const Key &key,
      [[maybe_unused]] const size_t key_len,
      const Payload &payload,
      const size_t pay_len)
      : level_{level}
  {
    // set a key value
    if constexpr (IsVarLenData<Key>()) {
      key_ = ::dbgroup::memory::Allocate<KeyWOPtr>(key_len);
      memcpy(key_, key, key_len);
    } else {
      key_ = key;
    }

    // store a payload value
    data_.store(PrepareWord(payload, pay_len), std::memory_order_release);
  }

  Node(const Node &) = delete;
  Node(Node &&) = delete;

  Node &operator=(const Node &) = delete;
  Node &operator=(Node &&) = delete;

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the metadata object.
   *
   */
  ~Node()
  {
    if constexpr (IsVarLenData<Key>()) {
      ::dbgroup::memory::Release<KeyWOPtr>(key_);
    }

    if constexpr (!CanCAS<Payload>()) {
      const auto v = data_.load(std::memory_order_acquire) & ~kDelBit;
      ::dbgroup::memory::Release<PayWOPtr>(reinterpret_cast<PayWOPtr *>(v));
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
   * @param key A target key.
   * @retval true if the key of this node is less than the given key.
   * @retval false otherwise.
   */
  [[nodiscard]] constexpr auto
  LT(const Key &key) const  //
      -> bool
  {
    return Comp{}(key_, key);
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
    return Comp{}(key, key_);
  }

  /**
   * @retval true if the node is active.
   * @retval false if the payload has already been deleted.
   */
  [[nodiscard]] auto
  IsDeleted() const  //
      -> bool
  {
    const auto v = data_.load(std::memory_order_relaxed);
    return (v & kDelBit) > 0;
  }

  /**
   * @param level The level of the next node.
   * @return The pointer to the next node.
   */
  [[nodiscard]] auto
  GetNext(const size_t level) const  //
      -> Node *
  {
    auto ptr = next_nodes_[level].load(std::memory_order_acquire);
    return reinterpret_cast<Node *>(ptr & ~kDelBit);
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
      Node *next)
  {
    next_nodes_[level].store(reinterpret_cast<uintptr_t>(next), std::memory_order_relaxed);
  }

  /**
   * @brief Compare and swap the next node.
   *
   * @param level The level of the next node.
   * @param expected The pointer to the current next node.
   * @param desired The pointer to the new next node.
   * @retval true if CAS succeeds.
   * @retval false otherwise.
   */
  auto
  CASNext(  //
      const size_t level,
      Node *expected,
      Node *desired)  //
      -> bool
  {
    const auto expected_ptr = reinterpret_cast<uintptr_t>(expected);
    auto old_ptr = next_nodes_[level].load(std::memory_order_relaxed);
    if (old_ptr != expected_ptr) return false;

    const auto new_ptr = reinterpret_cast<uintptr_t>(desired);
    return next_nodes_[level].compare_exchange_strong(old_ptr, new_ptr, std::memory_order_release);
  }

  /**
   * @brief Compare and delete the next node.
   *
   * @param level The level of the next node.
   * @return The next node.
   */
  auto
  DeleteNext(const size_t level)  //
      -> Node *
  {
    auto old = next_nodes_[level].load(std::memory_order_acquire);
    while (true) {
      assert((old & kDelBit) == 0);

      const auto del = old | kDelBit;
      if (next_nodes_[level].compare_exchange_weak(old, del, std::memory_order_relaxed)) break;
      SKIP_LIST_SPINLOCK_HINT
    }

    return reinterpret_cast<Node *>(old);
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
    auto data = data_.load(std::memory_order_acquire);
    if ((data & kDelBit) != 0) return false;  // the value has been deleted

    if constexpr (CanCAS<Payload>()) {
      memcpy(&out_payload, &data, kWordSize);
    } else if constexpr (IsVarLenData<Payload>()) {
      thread_local std::unique_ptr<PayWOPtr, std::function<void(Payload)>> tls_payload{
          ::dbgroup::memory::Allocate<PayWOPtr>(kMaxVarDataSize),
          ::dbgroup::memory::Release<PayWOPtr>};

      auto *ptr = reinterpret_cast<size_t *>(data);
      out_payload = tls_payload.get();
      memcpy(out_payload, ShiftAddr(ptr, kHeaderLen), *ptr);
    } else {
      auto *ptr = reinterpret_cast<Payload *>(data);
      out_payload = *ptr;
    }

    return true;
  }

  /**
   * @brief Update the stored payload by the given value.
   *
   * @param payload A payload value to be stored.
   * @param pay_len The length of the given payload.
   * @retval The old value if the payload is updated.
   * @retval kDelBit if the payload has already been deleted.
   */
  auto
  Update(  //
      const Payload &payload,
      const size_t pay_len)  //
      -> uint64_t
  {
    const auto new_v = PrepareWord(payload, pay_len);
    auto old_v = data_.load(std::memory_order_acquire);
    while ((old_v & kDelBit) == 0) {
      if (data_.compare_exchange_weak(old_v, new_v, std::memory_order_release)) return old_v;
      SKIP_LIST_SPINLOCK_HINT
    }

    return kDelBit;  // the value has been deleted
  }

  /**
   * @brief Delete the stored payload.
   *
   * @retval true if the payload is deleted.
   * @retval false if the payload has already been deleted.
   */
  auto
  Delete()  //
      -> bool
  {
    auto old_v = data_.load(std::memory_order_acquire);
    while ((old_v & kDelBit) == 0) {
      const auto del_v = old_v | kDelBit;
      if (data_.compare_exchange_weak(old_v, del_v, std::memory_order_relaxed)) return true;
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
   * @brief Create an inlined/pointer payload for the given value.
   *
   * @param payload A payload value to be stored.
   * @param pay_len The length of the given payload value.
   * @return An inline payload or the pointer to a payload.
   */
  static auto
  PrepareWord(  //
      const Payload &payload,
      const size_t pay_len)  //
      -> uint64_t
  {
    uint64_t data;
    if constexpr (CanCAS<Payload>()) {
      memcpy(&data, &payload, kWordSize);
      assert((data & kDelBit) == 0);
    } else if constexpr (IsVarLenData<Payload>()) {
      auto *ptr = ::dbgroup::memory::Allocate<size_t>(kHeaderLen + pay_len);
      *ptr = pay_len;
      memcpy(ShiftAddr(ptr, kHeaderLen), payload, pay_len);
      data = reinterpret_cast<uint64_t>(ptr);
    } else {
      auto *ptr = ::dbgroup::memory::Allocate<Payload>();
      *ptr = payload;
      data = reinterpret_cast<uint64_t>(ptr);
    }

    return data;
  }

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// @brief The top level of this node.
  size_t level_{0};

  /// @brief The key value stored in this node.
  Key key_{};

  /// @brief The payload value stored in this node.
  std::atomic_uint64_t data_{0};

  /// @brief The pointers to the next nodes in each level.
  std::atomic_uintptr_t next_nodes_[0];
};

}  // namespace dbgroup::index::skip_list::component

#endif  // SKIP_LIST_COMPONENT_NODE_HPP
