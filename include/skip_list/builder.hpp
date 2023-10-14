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

#ifndef SKIP_LIST_BUILDER_HPP
#define SKIP_LIST_BUILDER_HPP

// local sources
#include "skip_list/skip_list.hpp"

#ifdef SKIP_LIST_USE_ON_PMEM
#include "skip_list/skip_list_on_pmem.hpp"
#endif

namespace dbgroup::index::skip_list
{
/**
 * @brief A class for creating an index and managing its parameters.
 *
 * @tparam Key A class of stored keys.
 * @tparam Payload A class of stored payloads.
 * @tparam Comp A class for ordering keys.
 */
template <class Key, class Payload, class Comp = std::less<Key>>
class Builder
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using SkipList_t = SkipList<Key, Payload, Comp>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Prepare a builder for an index.
   *
   */
  constexpr Builder() = default;

  constexpr Builder(const Builder &) = default;
  constexpr Builder(Builder &&) noexcept = default;

  constexpr Builder &operator=(const Builder &) = default;
  constexpr Builder &operator=(Builder &&) noexcept = default;

  /*####################################################################################
   * Public destructor
   *##################################################################################*/

  ~Builder() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @brief Construct an index using the specified parameters.
   *
   * @return The @c std::unique_ptr of an index.
   */
  [[nodiscard]] auto
  Build() const
  {
    return std::make_unique<SkipList_t>(max_level_, p_, gc_interval_, gc_num_);
  }

  /**
   * @brief Set the maxmum level of nodes
   *
   * @param max_level The maxmum level of nodes.
   * @return Oneself.
   */
  void
  SetMaxLevel(const size_t max_level)
  {
    max_level_ = max_level;
  }

  /**
   * @brief Set a probability to determine node heights.
   *
   * @param p The probability to determine node heights.
   * @return Oneself.
   */
  void
  SetProbability(const double p)
  {
    p_ = p;
  }

  /**
   * @brief Set the GC interval.
   *
   * @param gc_interval The GC interval in microseconds.
   * @return Oneself.
   */
  void
  SetGCInterval(const size_t gc_interval)
  {
    gc_interval_ = gc_interval;
  }

  /**
   * @brief Set the number of worker threads for GC.
   *
   * @param gc_thread_num The number of worker threads for GC.
   * @return Oneself.
   */
  void
  SetGCThreadNum(const size_t gc_thread_num)
  {
    gc_num_ = gc_thread_num;
  }

 protected:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// @brief The maximum level in this skip list.
  size_t max_level_{kDefaultMaxHeight};

  /// @brief The probability of determining the levels of each node.
  double p_{kDefaultProb};

  /// @brief The garbage collection interval.
  size_t gc_interval_{kDefaultGCTime};

  /// @brief The number of worker threads used for garbage collection.
  size_t gc_num_{kDefaultGCThreadNum};
};

#ifdef SKIP_LIST_USE_ON_PMEM
/**
 * @brief A class for creating an index and managing its parameters.
 *
 * @tparam Key A class of stored keys.
 * @tparam Payload A class of stored payloads.
 * @tparam Comp A class for ordering keys.
 */
template <class Key, class Payload, class Comp = std::less<Key>>
class BuilderOnPMEM : public Builder<Key, Payload, Comp>
{
 public:
  /*####################################################################################
   * Type aliases
   *##################################################################################*/

  using SkipListOnPMEM_t = SkipListOnPMEM<Key, Payload, Comp>;

  /*####################################################################################
   * Public constructors and assignment operators
   *##################################################################################*/

  /**
   * @brief Prepare a builder for an index.
   *
   * @param pmem_dir The path to a directory on persistent memory to store an index.
   */
  explicit BuilderOnPMEM(const std::string &pmem_dir) : pmem_dir_{pmem_dir} {}

  constexpr BuilderOnPMEM(const BuilderOnPMEM &) = default;
  constexpr BuilderOnPMEM(BuilderOnPMEM &&) noexcept = default;

  constexpr BuilderOnPMEM &operator=(const BuilderOnPMEM &) = default;
  constexpr BuilderOnPMEM &operator=(BuilderOnPMEM &&) noexcept = default;

  /*####################################################################################
   * Public destructor
   *##################################################################################*/

  ~BuilderOnPMEM() = default;

  /*####################################################################################
   * Public utilities
   *##################################################################################*/

  /**
   * @brief Construct an index using the specified parameters.
   *
   * @return The @c std::unique_ptr of an index.
   */
  [[nodiscard]] auto
  Build() const
  {
    return std::make_unique<SkipListOnPMEM_t>(pmem_dir_, index_size_,      //
                                              this->max_level_, this->p_,  //
                                              layout_name_,                //
                                              gc_size_, this->gc_interval_, this->gc_num_);
  }

  /**
   * @brief Set the storage capacity for an index.
   *
   * @param index_size The maximum capacity in bytes for an index.
   * @return Oneself.
   */
  void
  SetIndexSize(const size_t index_size)
  {
    index_size_ = index_size;
  }

  /**
   * @brief Set the PMDK layout name for an index.
   *
   * @param layout_name The layout name.
   * @return Oneself.
   */
  void
  SetLayout(const std::string &layout_name)
  {
    layout_name_ = layout_name;
  }

  /**
   * @brief Set the storage capacity for GC.
   *
   * @param gc_size The maximum capacity in bytes for GC.
   * @return Oneself.
   */
  void
  SetGCSize(const size_t gc_size)
  {
    gc_size_ = gc_size;
  }

 private:
  /*####################################################################################
   * Internal member variables
   *##################################################################################*/

  /// @brief The path to persistent memory.
  std::filesystem::path pmem_dir_{};

  /// @brief The maximum index size on persistent memory.
  size_t index_size_{kDefaultIndexSize};

  /// @brief The name of the layout for identification purposes.
  std::string layout_name_{"skip_list"};

  /// @brief The amount of memory available for garbage collection.
  size_t gc_size_{PMEMOBJ_MIN_POOL * 2};
};
#endif

}  // namespace dbgroup::index::skip_list

#endif  // SKIP_LIST_BUILDER_HPP
