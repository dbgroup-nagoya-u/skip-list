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

#ifndef SKIP_LIST_COMPONENT_RECORD_ITERATOR_HPP
#define SKIP_LIST_COMPONENT_RECORD_ITERATOR_HPP

// external sources
#include "memory/component/epoch_guard.hpp"

// local sources
#include "skip_list/component/common.hpp"
#include "skip_list/component/node.hpp"

namespace dbgroup::index::skip_list
{
// forward declaratoin
template <class K, class V, class C>
class SkipList;

namespace component
{
/**
 * @brief A class to represent iterators in scanning.
 *
 */
template <class Key, class Payload, class Comp>
class RecordIterator
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using EpochGuard = ::dbgroup::memory::component::EpochGuard;
  using ScanKey = std::optional<std::tuple<Key, size_t, bool>>;
  using Node_t = Node<Key, Payload, Comp>;
  using SkipList_t = SkipList<Key, Payload, Comp>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Construct a new iterator.
   *
   */
  RecordIterator() = default;

  /**
   * @brief Construct a new iterator.
   *
   * @param skip_list An original skip list.
   * @param node A current node.
   * @param begin_key The end key for the search.
   * @param end_key The end key for the search.
   * @param guard The guard instance to retain removed nodes.
   */
  RecordIterator(  //
      SkipList_t *skip_list,
      Node_t *node,
      ScanKey begin_key,
      ScanKey end_key,
      EpochGuard &&guard)
      : skip_list_{skip_list},
        node_{node},
        begin_key_{std::move(begin_key)},
        end_key_{std::move(end_key)},
        guard_{std::move(guard)}
  {
  }

  RecordIterator(const RecordIterator &) = delete;
  RecordIterator(RecordIterator &&) = delete;

  RecordIterator &operator=(const RecordIterator &) = delete;

  RecordIterator &
  operator=(RecordIterator &&obj) noexcept
  {
    node_ = obj.node_;
    begin_key_ = std::move(obj.begin_key_);
    guard_ = std::move(obj.guard_);

    return *this;
  }

  /*####################################################################################
   * Public destructors
   *##################################################################################*/

  /**
   * @brief Destroy the RecordIterator object.
   *
   */
  ~RecordIterator() = default;

  /*##################################################################################
   * Public operators for iterators
   *################################################################################*/

  /**
   * @retval true if this iterator indicates a live record.
   * @retval false otherwise.
   */
  explicit
  operator bool()
  {
    return HasRecord();
  }

  /**
   * @return a current key and payload pair.
   */
  constexpr auto
  operator*() const  //
      -> std::pair<Key, Payload>
  {
    return {GetKey(), GetPayload()};
  }

  /**
   * @brief Forward this iterator.
   *
   */
  constexpr void
  operator++()
  {
    node_ = node_->GetNext(0);
  }

  /*##################################################################################
   * Public getters/setters
   *################################################################################*/

  /**
   * @brief Check if there are any records left.
   *
   * NOTE: this may call a scanning function internally to get a sibling node.
   *
   * @retval true if there are any records or next node left.
   * @retval false otherwise.
   */
  [[nodiscard]] auto
  HasRecord()  //
      -> bool
  {
    while (node_ != nullptr) {
      if (node_->Read(payload_)) {
        // the current node is alive, so check the scan range condition
        begin_key_ = std::make_tuple(node_->GetKey(), 0, false);
        if (!end_key_) return true;
        const auto &[end_key, dummy, closed] = *end_key_;
        if (node_->GT(end_key) || (!closed && !node_->LT(end_key))) {
          node_ = nullptr;
          break;
        }
        return true;
      }

      // the current node has been removed, so search for a node again
      *this = skip_list_->Scan(begin_key_, end_key_);
    }
    return false;
  }

  /**
   * @return a key of a current record
   */
  [[nodiscard]] constexpr auto
  GetKey() const  //
      -> Key
  {
    return std::get<0>((*begin_key_));
  }

  /**
   * @return a payload of a current record
   */
  [[nodiscard]] constexpr auto
  GetPayload() const  //
      -> Payload
  {
    return payload_;
  }

 private:
  /*####################################################################################
   * Public member variables
   *##################################################################################*/

  /// @brief An original skip list.
  SkipList_t *skip_list_{nullptr};

  /// @brief A current node.
  Node_t *node_{nullptr};

  /// @brief The begin key for the search.
  ScanKey begin_key_{std::nullopt};

  /// @brief The end key for the search.
  ScanKey end_key_{std::nullopt};

  /// @brief The payload of the current node.
  Payload payload_{};

  /// @brief The guard instance to retain removed nodes.
  EpochGuard guard_{};
};

}  // namespace component
}  // namespace dbgroup::index::skip_list

#endif  // SKIP_LIST_COMPONENT_RECORD_ITERATOR_HPP
