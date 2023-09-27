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
  using PayloadWOPtr = std::remove_pointer_t<Payload>;

  /*####################################################################################
   * Public classes
   *##################################################################################*/

  /**
   * @brief A dummy struct for representing internal pages.
   *
   */
  struct Page {
    // Do not use as a general class.
    Page() = delete;
    Page(const Page &) = delete;
    Page(Page &&) = delete;
    auto operator=(const Page &) -> Page & = delete;
    auto operator=(Page &&) -> Page & = delete;
    ~Page() = delete;

    // filling zeros in reclaimed pages
    using T = Node;

    // reuse pages
    static constexpr bool kReusePages = false;

    // delete pages with alignments
    static const inline std::function<void(void *)> deleter{
        [](void *ptr) { ::operator delete(ptr); }};
  };

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

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
      key_ = Allocate<KeyWOPtr>(key_len);
      memcpy(key_, key, key_len);
    } else {
      key_ = key;
    }

    // store a payload value
    data_.store(PrepareWord(payload, pay_len), std::memory_order_release);

    // reset all the pointers
    for (size_t i = 0; i < level_; ++i) {
      next_nodes_[i] = nullptr;
    }
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
      Release<KeyWOPtr>(key_);
    }

    if constexpr (!CanCAS<Payload>()) {
      const auto v = data_.load(std::memory_order_acquire);
      if (v != kDelBit) {
        if constexpr (IsVarLenData<Payload>()) {
          Release<PayloadWOPtr>(reinterpret_cast<Payload>(v));
        } else {
          Release<Payload>(reinterpret_cast<Payload *>(v));
        }
      }
    }
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @param key A target key.
   * @retval true if the key of this node is less than the given key.
   * @retval false otherwise.
   */
  constexpr auto
  LT(const Key &key)  //
      -> bool
  {
    return Comp{}(key_, key);
  }

  /**
   * @param key A target key.
   * @retval true if the key of this node is greater than the given key.
   * @retval false otherwise.
   */
  constexpr auto
  GT(const Key &key)  //
      -> bool
  {
    return Comp{}(key, key_);
  }

  /**
   * @param level The level of the next node.
   * @return The pointer to the next node.
   */
  auto
  GetNext(const size_t level)  //
      -> Node *
  {
    return next_nodes_[level].load(std::memory_order_relaxed);
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
    next_nodes_[level].store(next, std::memory_order_relaxed);
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
    return next_nodes_[level].compare_exchange_strong(expected, desired, std::memory_order_release);
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
  Read(Payload &out_payload)  //
      -> bool
  {
    auto data = data_.load(std::memory_order_acquire);
    if ((data & kDelBit) != 0) return false;  // the value has been deleted

    if constexpr (CanCAS<Payload>()) {
      memcpy(&out_payload, &data, kWordSize);
    } else if constexpr (IsVarLenData<Payload>()) {
      thread_local std::unique_ptr<PayloadWOPtr, std::function<void(Payload)>> tls_payload{
          component::Allocate<PayloadWOPtr>(kMaxVarDataSize), component::Release<PayloadWOPtr>};

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
   * @retval true if the payload is updated.
   * @retval false if the payload has already been deleted.
   */
  auto
  Update(  //
      const Payload &payload,
      const size_t pay_len)  //
      -> bool
  {
    const auto new_v = PrepareWord(payload, pay_len);
    auto old_v = data_.load(std::memory_order_acquire);
    while (old_v != kDelBit) {
      if (data_.compare_exchange_weak(old_v, new_v, std::memory_order_release)) return true;
      SKIP_LIST_SPINLOCK_HINT
    }

    return false;  // the value has been deleted
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
    while (old_v != kDelBit) {
      if (data_.compare_exchange_weak(old_v, kDelBit, std::memory_order_relaxed)) return true;
      SKIP_LIST_SPINLOCK_HINT
    }

    return false;  // the value has been deleted
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// @brief The most significant bit represents a deleted value.
  static constexpr uint64_t kDelBit = 1UL << 63UL;

  /// @brief The alignment for payload values.
  static constexpr size_t kAlign = alignof(PayloadWOPtr);

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
  auto
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
      auto *ptr = Allocate<size_t>(kHeaderLen + pay_len);
      *ptr = pay_len;
      memcpy(ShiftAddr(ptr, kHeaderLen), payload, pay_len);
      data = reinterpret_cast<uint64_t>(ptr);
    } else {
      auto *ptr = Allocate<Payload>();
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
  std::atomic_uint64_t data_{};

  /// @brief The pointers to the next nodes in each level.
  std::atomic<Node *> next_nodes_[0]{};
};

}  // namespace dbgroup::index::skip_list::component

#endif  // SKIP_LIST_COMPONENT_NODE_HPP
