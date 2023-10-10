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

#ifndef SKIP_LIST_SKIP_LIST_ON_PMEM_HPP
#define SKIP_LIST_SKIP_LIST_ON_PMEM_HPP

// C++ standard libraries
#include <algorithm>
#include <filesystem>
#include <functional>
#include <future>
#include <random>
#include <thread>
#include <utility>
#include <vector>

// external sources
#include "memory/epoch_based_gc.hpp"
#include "pmwcas/descriptor_pool.hpp"

// local sources
#include "skip_list/component/node_on_pmem.hpp"
#include "skip_list/component/record_iterator.hpp"
#include "skip_list/utility.hpp"

namespace dbgroup::index::skip_list
{
/**
 * @brief A class for representing skip lists.
 *
 * @tparam Key A class of stored keys.
 * @tparam Payload A class of stored payloads.
 * @tparam Comp A class for ordering keys.
 */
template <class Key, class Payload, class Comp = std::less<Key>>
class SkipListOnPMEM
{
 public:
  /*####################################################################################
   * Public constants
   *##################################################################################*/

  static constexpr size_t kPayTargetAlign =
      (alignof(std::remove_pointer_t<Payload>) > ::dbgroup::memory::kDefaultAlignment)
          ? alignof(std::remove_pointer_t<Payload>)
          : ::dbgroup::memory::kDefaultAlignment;

  /*####################################################################################
   * Public classes
   *##################################################################################*/

  /**
   * @brief A dummy struct for representing garbage of payloads on persistent memory.
   *
   */
  struct alignas(kPayTargetAlign) PayloadTarget : public ::dbgroup::memory::DefaultTarget {
    // nodes are stored in persistent memory
    static constexpr bool kOnPMEM = true;
  };

  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using KeyWOPtr = std::remove_pointer_t<Key>;
  using PayWOPtr = std::remove_pointer_t<Payload>;
  using Node_t = component::NodeOnPMEM<Key, Payload, Comp>;
  using RecordIterator = component::RecordIterator<Key, Payload, Comp, true>;
  using Stack_t = std::vector<std::pair<Node_t *, Node_t *>>;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;
  using NodeTarget = typename Node_t::Target;
  using GC_t = std::conditional_t<CanCAS<Payload>(),
                                  ::dbgroup::memory::EpochBasedGC<NodeTarget>,
                                  ::dbgroup::memory::EpochBasedGC<NodeTarget, PayloadTarget>>;
  using DescriptorPool = ::dbgroup::atomic::pmwcas::DescriptorPool;

  template <class Entry>
  using BulkIter = typename std::vector<Entry>::const_iterator;
  using BulkPromise = std::promise<Stack_t>;
  using BulkFuture = std::future<Stack_t>;

  /*####################################################################################
   * Public constructors
   *##################################################################################*/

  /**
   * @brief Construct a new Skip List object
   *
   * @param pmem_dir The path to a directory on persistent memory to store an index.
   * @param max_size The maximum capacity in bytes for an index.
   * @param max_level The maximum level in this skip list.
   * @param p The probability of determining the levels of each node.
   * @param layout_name The layout name.
   * @param gc_interval_microsec GC internal [us] (default: 10ms).
   * @param gc_thread_num The number of GC threads (default: 1).
   */
  explicit SkipListOnPMEM(  //
      const std::string &pmem_dir,
      const size_t max_size,
      const size_t max_level = kDefaultMaxHeight,
      const double p = kDefaultProb,
      std::string layout_name = "skip_list",
      const size_t gc_size = PMEMOBJ_MIN_POOL * 2,
      const size_t gc_interval_micro_sec = kDefaultGCTime,
      const size_t gc_thread_num = kDefaultGCThreadNum)
      : max_level_{max_level},
        p_{p},
        index_path_{pmem_dir},
        gc_path_{pmem_dir},
        pmwcas_path_{pmem_dir},
        layout_name_{std::move(layout_name)},
        gc_size_{gc_size},
        gc_interval_{gc_interval_micro_sec},
        gc_num_{gc_thread_num}
  {
    // check arguments
    if (!std::filesystem::is_directory(pmem_dir)) {
      throw std::runtime_error{"The given path is not a directory"};
    }
    index_path_ /= "skip_list";
    gc_path_ /= "garbage_collection_pool";
    pmwcas_path_ /= "pmwcas_descriptor_pool";

    // prepare a PMEMobjpool
    constexpr auto kModeRW = S_IRUSR | S_IWUSR;  // NOLINT
    const auto exist = std::filesystem::exists(index_path_);
    pop_ = exist ? pmemobj_open(index_path_.c_str(), layout_name_.c_str())
                 : pmemobj_create(index_path_.c_str(), layout_name_.c_str(), max_size, kModeRW);
    if (pop_ == nullptr) {
      throw std::runtime_error{pmemobj_errormsg()};
    }

    // prepare a head node
    auto &&root = pmemobj_root(pop_, sizeof(Node_t) + max_level_ * kWordSize);
    pop_id_ = root.pool_uuid_lo;
    head_ = exist ? reinterpret_cast<Node_t *>(pmemobj_direct(root))
                  : new (pmemobj_direct(root)) Node_t{pop_id_, max_level_};

    // prepare external components
    desc_pool_ = std::make_unique<DescriptorPool>(pmwcas_path_, layout_name_);
    gc_ = std::make_unique<GC_t>(gc_path_, gc_size_, layout_name_, gc_interval_, gc_num_);

    // perform a recovery procedure if needed
    RecoveryIfNeeded();

    // start GC
    gc_->StartGC();
  }

  SkipListOnPMEM(const SkipListOnPMEM &) = delete;
  SkipListOnPMEM(SkipListOnPMEM &&) = delete;

  auto operator=(const SkipListOnPMEM &) -> SkipListOnPMEM & = delete;
  auto operator=(SkipListOnPMEM &&) -> SkipListOnPMEM & = delete;

  /*##################################################################################
   * Public destructors
   *################################################################################*/

  /**
   * @brief Destroy the SkipListOnPMEM object.
   *
   */
  ~SkipListOnPMEM()
  {
    gc_ = nullptr;
    desc_pool_ = nullptr;
    pmemobj_close(pop_);
  }

  /*####################################################################################
   * Public read APIs
   *##################################################################################*/

  /**
   * @brief Read the payload corresponding to a given key if it exists.
   *
   * @param key a target key.
   * @param key_len the length of a target key.
   * @retval the payload of a given key if it is in this tree.
   * @retval std::nullopt otherwise.
   */
  auto
  Read(  //
      const Key &key,
      [[maybe_unused]] const size_t key_len = sizeof(Key))  //
      -> std::optional<Payload>
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();
    Payload payload;

    auto &&[found, stack] = SearchNode(key);
    while (found) {
      if (stack.front().second->Read(payload)) return payload;
      found = SearchNodeAt(0, key, stack);
    }

    return std::nullopt;
  }

  /**
   * @brief Perform a range scan with given keys.
   *
   * @param begin_key a pair of a begin key and its openness (true=closed).
   * @param end_key a pair of an end key and its openness (true=closed).
   * @return an iterator to access scanned records.
   */
  auto
  Scan(  //
      const ScanKey &begin_key = std::nullopt,
      const ScanKey &end_key = std::nullopt)  //
      -> RecordIterator
  {
    auto &&guard = gc_->CreateEpochGuard();

    Node_t *node;
    if (begin_key) {
      const auto &[key, dummy, closed] = *begin_key;
      auto &&[found, stack] = SearchNode(key);
      node = stack.front().second;
      if (found && !closed) {
        node = node->GetNext(0);
      }
    } else {
      node = head_->GetNext(0);
    }

    return RecordIterator{this, node, begin_key, end_key, std::move(guard)};
  }

  /*####################################################################################
   * Public write APIs
   *##################################################################################*/

  /**
   * @brief Write (i.e., put) a given key/payload pair.
   *
   * @note This function always overwrites a payload and can be optimized for that
   * purpose; the procedure can omit the key uniqueness check.
   *
   * @param key a target key.
   * @param payload a target payload.
   * @param key_len the length of a target key.
   * @param pay_len the length of a target payload.
   * @return kSuccess.
   */
  auto
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key),
      const size_t pay_len = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();
    size_t level{};
    Node_t *new_node{nullptr};
    PMEMoid *oid{nullptr};

    auto &&[found, stack] = SearchNode(key);
    while (true) {
      if (found) {
        auto *node = stack.front().second;
        if constexpr (CanCAS<Payload>()) {
          // the same key has been found, so perform update
          PMEMoid p_oid{OID_NULL};
          if (node->Update(payload, pay_len, desc_pool_.get(), pop_, &p_oid) != kDelBit) break;
        } else {
          // the same key has been found, so perform update
          auto *p_oid = gc_->template GetTmpField<PayloadTarget>(kOldPos);
          if (node->Update(payload, pay_len, desc_pool_.get(), pop_, p_oid) != kDelBit) {
            gc_->template AddGarbage<PayloadTarget>(p_oid);
            break;
          }
        }
      } else {
        // create a new node if needed
        if (new_node == nullptr) {
          level = GetLevel();
          std::tie(new_node, oid) = AllocateNode(level);
          new (new_node) Node_t{pop_id_, level, key, key_len, payload, pay_len, pop_};
        }

        // try to install the new node
        auto [prev, next] = stack.front();
        new_node->StoreNext(0, next);
        if (prev->CASNext(0, next, new_node, desc_pool_.get())) {
          // link the new node at all the levels
          InsertNodeAtAllLevels(level, key, new_node, stack, oid);
          return kSuccess;
        }
      }

      // the previous node has been modified, so retry
      found = SearchNodeAt(0, key, stack);
    }

    if (oid != nullptr) {
      pmemobj_free(oid);
    }
    return kSuccess;
  }

  /**
   * @brief Insert a given key/payload pair.
   *
   * @note This function performs a uniqueness check on its processing. If the given key
   * does not exist in this tree, this function inserts a target payload into this tree.
   * If the given key exists in this tree, this function does nothing and returns
   * kKeyExist.
   *
   * @param key a target key.
   * @param payload a target payload.
   * @param key_len the length of a target key.
   * @param pay_len the length of a target payload.
   * @retval kSuccess if inserted.
   * @retval kKeyExist otherwise.
   */
  auto
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key),
      const size_t pay_len = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();
    size_t level{};
    Node_t *new_node{nullptr};
    PMEMoid *oid{nullptr};

    auto &&[found, stack] = SearchNode(key);
    while (!found) {
      // create a new node if needed
      if (new_node == nullptr) {
        level = GetLevel();
        std::tie(new_node, oid) = AllocateNode(level);
        new (new_node) Node_t{pop_id_, level, key, key_len, payload, pay_len, pop_};
      }

      // try to install the new node
      auto [prev, next] = stack.front();
      new_node->StoreNext(0, next);
      if (prev->CASNext(0, next, new_node, desc_pool_.get())) {
        // link the new node at all the levels
        InsertNodeAtAllLevels(level, key, new_node, stack, oid);
        return kSuccess;
      }

      // the previous node has been modified, so retry
      found = SearchNodeAt(0, key, stack);
    }

    if (oid != nullptr) {
      pmemobj_free(oid);
    }
    return kKeyExist;
  }

  /**
   * @brief Update the record corresponding to a given key with a given payload.
   *
   * @note This function performs a uniqueness check on its processing. If the given key
   * exists in this tree, this function updates the corresponding payload. If the given
   * key does not exist in this tree, this function does nothing and returns
   * kKeyNotExist.
   *
   * @param key a target key.
   * @param payload a target payload.
   * @param key_len the length of a target key.
   * @param pay_len the length of a target payload.
   * @retval kSuccess if updated.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Update(  //
      const Key &key,
      const Payload &payload,
      [[maybe_unused]] const size_t key_len = sizeof(Key),
      const size_t pay_len = sizeof(Payload))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();

    [[maybe_unused]] PMEMoid *oid = nullptr;
    auto &&[found, stack] = SearchNode(key);
    while (found) {
      uint64_t old_v;
      if constexpr (CanCAS<Payload>()) {
        PMEMoid p_oid{OID_NULL};
        old_v = stack.front().second->Update(payload, pay_len, desc_pool_.get(), pop_, &p_oid);
      } else {
        oid = gc_->template GetTmpField<PayloadTarget>(kOldPos);
        old_v = stack.front().second->Update(payload, pay_len, desc_pool_.get(), pop_, oid);
      }

      if (old_v != kDelBit) {
        if constexpr (!CanCAS<Payload>()) {
          gc_->template AddGarbage<PayloadTarget>(oid);
        }
        return kSuccess;
      }

      // the node has been removed, so retry
      found = SearchNodeAt(0, key, stack);
    }

    return kKeyNotExist;
  }

  /**
   * @brief Delete the record corresponding to a given key from this tree.
   *
   * @note This function performs a uniqueness check on its processing. If the given key
   * exists in this tree, this function deletes it. If the given key does not exist in
   * this tree, this function does nothing and returns kKeyNotExist.
   *
   * @param key a target key.
   * @param key_len the length of the target key.
   * @retval kSuccess if deleted.
   * @retval kKeyNotExist otherwise.
   */
  auto
  Delete(  //
      const Key &key,
      [[maybe_unused]] const size_t key_len = sizeof(Key))  //
      -> ReturnCode
  {
    [[maybe_unused]] const auto &guard = gc_->CreateEpochGuard();

    PMEMoid *oid = nullptr;
    auto &&[found, stack] = SearchNode(key);
    while (found) {
      if (oid == nullptr) {
        oid = gc_->template GetTmpField<NodeTarget>(kOldPos);
        oid->pool_uuid_lo = pop_id_;
        pmem_flush(&(oid->pool_uuid_lo), kWordSize);
      }

      auto *del_node = stack.front().second;
      if (del_node->Delete(desc_pool_.get(), oid)) {
        // unlink all the next pointers
        const auto max_level = del_node->GetLevel();
        for (size_t i = 0; i < max_level; ++i) {
          auto *next = del_node->DeleteNext(i, desc_pool_.get());
          while (true) {
            auto *prev = stack.at(i).first;
            if (prev->CASNext(i, del_node, next, desc_pool_.get())) break;

            // the previous node has been modified, so retry
            SearchNodeAt(i, key, stack);
          }
        }

        gc_->template AddGarbage<NodeTarget>(oid);
        return kSuccess;
      }

      // the node has been removed, so retry
      found = SearchNodeAt(0, key, stack);
    }

    return kKeyNotExist;
  }

  /*####################################################################################
   * Public bulkload API
   *##################################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs.
   *
   * This function loads the given entries into this index, assuming that the entries
   * are given as a vector of key/payload pairs (or the tuples key/payload/key-length
   * for variable-length keys). Note that keys in records are assumed to be unique and
   * sorted.
   *
   * @tparam Entry a container of a key/payload pair.
   * @param entries the vector of entries to be bulkloaded.
   * @param thread_num the number of threads used for bulk loading.
   * @return kSuccess.
   */
  template <class Entry>
  auto
  Bulkload(  //
      const std::vector<Entry> &entries,
      const size_t thread_num = 1)  //
      -> ReturnCode
  {
    if (entries.empty()) return ReturnCode::kSuccess;

    Stack_t stack;
    auto &&iter = entries.cbegin();
    const auto rec_num = entries.size();
    if (thread_num <= 1 || rec_num < thread_num) {
      // bulkloading with a single thread
      stack = BulkloadWithSingleThread<Entry>(iter, rec_num);
    } else {
      // bulkloading with multi-threads
      std::vector<BulkFuture> futures{};
      futures.reserve(thread_num);

      // a lambda function for bulkloading with multi-threads
      auto loader = [&](BulkPromise p, BulkIter<Entry> iter, size_t n) {
        p.set_value(BulkloadWithSingleThread<Entry>(iter, n));
      };

      // create threads to construct partial lists
      for (size_t i = 0; i < thread_num; ++i) {
        BulkPromise p{};
        futures.emplace_back(p.get_future());
        const size_t n = (rec_num + i) / thread_num;
        std::thread{loader, std::move(p), iter, n}.detach();
        iter += n;
      }

      // wait for the worker threads to create partial trees
      stack = futures.front().get();
      for (size_t i = 1; i < thread_num; ++i) {
        auto &&next_stack = futures.at(i).get();
        for (size_t j = 0; j < max_level_; ++j) {
          auto *next = next_stack.at(j).first;
          if (next == nullptr) break;
          auto *prev = stack.at(j).second;
          if (prev == nullptr) {
            stack.at(j).first = next;
          } else {
            prev->StoreNext(j, next);
          }
          stack.at(j).second = next_stack.at(j).second;
        }
      }
    }

    // link the constructed lists from the head
    for (size_t i = 0; i < max_level_; ++i) {
      head_->StoreNext(i, stack.at(i).first);
    }

    return ReturnCode::kSuccess;
  }

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  void
  Clear()
  {
    auto *oid = gc_->template GetTmpField<NodeTarget>(kOldPos);
    auto *cur = head_->GetNext(0);
    while (cur != nullptr) {
      auto *prev = cur;
      cur = cur->GetNext(0);
      *oid = pmemobj_oid(prev);
      prev->~Node_t();
      pmemobj_free(oid);
    }

    head_->RemoveAllNextPointers();
  }

 private:
  /*####################################################################################
   * Internal constants
   *##################################################################################*/

  /// @brief The most significant bit represents a deleted value.
  static constexpr uint64_t kDelBit = Node_t::kDelBit;

  /// @brief The unsigned long of nullptr.
  static constexpr uintptr_t kNullPtr = 0;

  /// @brief The position of the old nodes/payloads in the temporary fields.
  static constexpr size_t kOldPos = 0;

  /// @brief The position of the new nodes in the temporary fields.
  static constexpr size_t kNewPos = 1;

  /*####################################################################################
   * Internal utility functions
   *##################################################################################*/

  /**
   * @brief Allocate a region of memory for a new node.
   *
   * @param level The maximum level of a new node.
   * @retval 1st: The allocated memory address.
   * @retval 2nd: The allocated OID for persistent memory.
   */
  auto
  AllocateNode(const size_t level)  //
      -> std::pair<Node_t *, PMEMoid *>
  {
    auto *oid = gc_->template GetTmpField<NodeTarget>(kNewPos);
    component::AllocatePmem(pop_, oid, sizeof(Node_t) + level * kWordSize);
    return {reinterpret_cast<Node_t *>(pmemobj_direct(*oid)), oid};
  }

  /**
   * @return The level of a new node.
   */
  [[nodiscard]] auto
  GetLevel() const  //
      -> size_t
  {
    thread_local std::uniform_real_distribution<double> dist{0.0, 1.0};
    thread_local std::mt19937_64 rng{std::random_device{}()};

    size_t level = 1;
    while (dist(rng) < p_ && level < max_level_) {
      ++level;
    }

    return level;
  }

  /**
   * @brief Search and construct a stack of nodes based on a given key.
   *
   * @param key A search key.
   * @retval 1st: true if the search key was found. false otherwise.
   * @retval 2nd: The stack of previous/next nodes of each level.
   */
  [[nodiscard]] auto
  SearchNode(const Key &key) const  //
      -> std::pair<bool, Stack_t>
  {
    Node_t *next{nullptr};
    Stack_t stack{max_level_, std::make_pair(nullptr, nullptr)};
    stack.emplace_back(head_, nullptr);

    // search and retain nodes at each level
    auto *cur = stack.back().first;
    for (int64_t i = max_level_ - 1; i >= 0; --i) {
      // move forward while the next node has the smaller key
      next = cur->GetNext(i);
      while (next != nullptr && next->LT(key)) {
        cur = next;
        next = cur->GetNext(i);
      }

      if (cur->IsDeleted()) {
        // the current node has been deleted, so retry
        for (cur = stack.at(++i).first; cur->IsDeleted(); cur = stack.at(++i).first) {
          // remove all the deleted nodes
        }
      } else {
        // go down to the next level
        stack.at(i) = std::make_pair(cur, next);
      }
    }

    next = stack.front().second;
    return {next != nullptr && !next->GT(key) && !next->IsDeleted(), stack};
  }

  /**
   * @brief Search and construct a stack of nodes based on a given key.
   *
   * @param level The bottom level during the search.
   * @param key A search key.
   * @param stack The stack of previous/next nodes of each level.
   * @retval true if the search key was found.
   * @retval false otherwise.
   */
  auto
  SearchNodeAt(  //
      const size_t level,
      const Key &key,
      Stack_t &stack) const  //
      -> bool
  {
    Node_t *next{nullptr};

    // search and retain nodes at each level
    auto *cur = stack.at(level).first;
    for (int64_t i = level; i >= static_cast<int64_t>(level); --i) {
      // move forward while the next node has the smaller key
      next = cur->GetNext(i);
      while (next != nullptr && next->LT(key)) {
        cur = next;
        next = cur->GetNext(i);
      }

      if (cur->IsDeleted()) {
        // the current node has been deleted, so retry
        for (cur = stack.at(++i).first; cur->IsDeleted(); cur = stack.at(++i).first) {
          // remove all the deleted nodes
        }
      } else {
        // go down to the next level
        stack.at(i) = std::make_pair(cur, next);
      }
    }

    next = stack.at(level).second;
    return next != nullptr && !next->GT(key) && !next->IsDeleted();
  }

  /**
   * @brief Insert a given node at each level.
   *
   * @param max_level The top level of target nodes.
   * @param key A search key.
   * @param node A node to insert.
   * @param stack The stack of previous/next nodes of each level.
   * @param oid A temporary OID to avoid memory leak.
   */
  void
  InsertNodeAtAllLevels(  //
      const size_t max_level,
      const Key &key,
      Node_t *node,
      Stack_t &stack,
      PMEMoid *oid)
  {
    for (size_t i = 1; i < max_level; ++i) {
      while (true) {
        auto [prev, next] = stack.at(i);
        node->StoreNext(i, next);
        if (prev->CASNext(i, next, node, desc_pool_.get())) break;

        // the previous node has been modified, so retry
        SearchNodeAt(i, key, stack);
      }
    }

    oid->off = kNullPtr;
    pmem_persist(&(oid->off), kWordSize);
  }

  /**
   * @brief Perform a recovery procedure if needed.
   *
   */
  void
  RecoveryIfNeeded()
  {
  }

  /*####################################################################################
   * Internal utilities for bulkloading
   *##################################################################################*/

  /**
   * @brief Bulkload specified kay/payload pairs with a single thread.
   *
   * Note that this function does not create a root node. The main process must create a
   * root node by using the nodes constructed by this function.
   *
   * @tparam Entry a container of a key/payload pair.
   * @param iter the begin position of target records.
   * @param n the number of entries to be bulkloaded.
   * @retval 1st: the height of a constructed tree.
   * @retval 2nd: constructed nodes in the top layer.
   */
  template <class Entry>
  auto
  BulkloadWithSingleThread(  //
      BulkIter<Entry> iter,
      const size_t n)  //
      -> Stack_t
  {
    auto level = GetLevel();
    auto &&[key, payload, key_len, pay_len] = ParseEntry(*iter);
    auto [node, oid] = AllocateNode(level);
    new (node) Node_t{pop_id_, level, key, key_len, payload, pay_len, pop_};

    Stack_t stack{max_level_, std::make_pair(nullptr, nullptr)};
    std::fill(stack.begin(), std::next(stack.begin(), level), std::make_pair(node, node));

    const auto &iter_end = iter + n;
    for (++iter; iter < iter_end; ++iter) {
      level = GetLevel();
      std::tie(key, payload, key_len, pay_len) = ParseEntry(*iter);
      std::tie(node, oid) = AllocateNode(level);
      new (node) Node_t{pop_id_, level, key, key_len, payload, pay_len, pop_};

      for (size_t i = 0; i < level; ++i) {
        node->StoreNext(i, nullptr);
        auto *prev = stack.at(i).second;
        if (prev == nullptr) {
          stack.at(i) = std::make_pair(node, node);
        } else {
          prev->StoreNext(i, node);
          stack.at(i).second = node;
        }
      }
    }

    return stack;
  }

  /**
   * @brief Parse an entry of bulkload according to key's type.
   *
   * @tparam Entry std::pair or std::tuple for containing entries.
   * @param entry a bulkload entry.
   * @retval 1st: a target key.
   * @retval 2nd: a target payload.
   * @retval 3rd: the length of a target key.
   * @retval 4th: the length of a target payload.
   */
  template <class Entry>
  constexpr auto
  ParseEntry(const Entry &entry)  //
      -> std::tuple<Key, Payload, size_t, size_t>
  {
    constexpr auto kTupleSize = std::tuple_size_v<Entry>;
    static_assert(2 <= kTupleSize && kTupleSize <= 4);

    if constexpr (kTupleSize == 4) {
      return entry;
    } else if constexpr (kTupleSize == 3) {
      const auto &[key, payload, key_len] = entry;
      return {key, payload, key_len, sizeof(Payload)};
    } else {
      const auto &[key, payload] = entry;
      return {key, payload, sizeof(Key), sizeof(Payload)};
    }
  }

  /*####################################################################################
   * Static assertions
   *##################################################################################*/

  /**
   * @tparam T A target class.
   * @retval true if a given class is trivially copyable and destructible.
   * @retval false otherwise.
   */
  template <class T>
  static constexpr auto
  TypeCheck()  //
      -> bool
  {
    if constexpr (IsVarLenData<T>()) {
      using WOPtr = std::remove_pointer_t<T>;
      return std::is_trivially_copyable_v<WOPtr> && std::is_trivially_destructible_v<WOPtr>;
    } else {
      return std::is_trivially_copyable_v<T> && std::is_trivially_destructible_v<T>;
    }
  }

  static_assert(TypeCheck<Key>());

  static_assert(TypeCheck<Payload>());

  static_assert(!CanCAS<Payload>()
                || (std::is_copy_constructible_v<Payload> && std::is_move_constructible_v<Payload>
                    && std::is_copy_assignable_v<Payload> && std::is_move_assignable_v<Payload>));

  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// @brief The maximum level in this skip list.
  size_t max_level_{kDefaultMaxHeight};

  /// @brief The probability of determining the levels of each node.
  double p_{kDefaultProb};

  /// @brief A garbage collector.
  std::unique_ptr<GC_t> gc_{nullptr};

  /// @brief The dummy head node for a search start position.
  Node_t *head_{nullptr};

  /// @brief The pool for persistent memory.
  PMEMobjpool *pop_{nullptr};

  /// @brief The UUID of PMEMobjpool.
  uint64_t pop_id_{0};

  /// @brief The pool of PMwCAS descriptors.
  std::unique_ptr<DescriptorPool> desc_pool_{nullptr};

  /// @brief The path to a BzTree instance.
  std::filesystem::path index_path_{};

  /// @brief The path to a garbage collection instance.
  std::filesystem::path gc_path_{};

  /// @brief The path to a PMwCAS descriptor instance.
  std::filesystem::path pmwcas_path_{};

  /// @brief The name of the layout for identification purposes.
  std::string layout_name_{};

  /// @brief The amount of memory available for garbage collection.
  size_t gc_size_{};

  /// @brief The garbage collection interval.
  size_t gc_interval_{};

  /// @brief The number of worker threads used for garbage collection.
  size_t gc_num_{};
};

}  // namespace dbgroup::index::skip_list

#endif  // SKIP_LIST_SKIP_LIST_ON_PMEM_HPP
