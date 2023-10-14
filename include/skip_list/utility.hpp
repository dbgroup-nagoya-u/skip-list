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

#ifndef SKIP_LIST_UTILITY_HPP
#define SKIP_LIST_UTILITY_HPP

// C++ standard libraries
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace dbgroup::index::skip_list
{
/*######################################################################################
 * Global constants
 *####################################################################################*/

/// @brief Assumes that one word is represented by 8 bytes (64bits).
constexpr size_t kWordSize = 8;

/// @brief Assumes that one cache line is represented by 64 bytes.
constexpr size_t kCacheLineSize = 64;

/// @brief The page size of virtual memory address.
constexpr size_t kVMPageSize = 4096;

/// @brief Napier's constant.
constexpr double kE = 2.71828182845904523536;

/// @brief The default maximum height of nodes in a skip list.
constexpr size_t kDefaultMaxHeight = 20;

/// @brief The default probability to determine node heights.
constexpr double kDefaultProb = 1.0 / kE;

/// @brief The default time interval for garbage collection [us].
constexpr size_t kDefaultGCTime = 10000;

/// @brief The default number of worker threads for garbage collection.
constexpr size_t kDefaultGCThreadNum = 1;

/// @brief A flag for indicating closed intervals
constexpr bool kClosed = true;

/// @brief A flag for indicating closed intervals
constexpr bool kOpen = false;

#ifdef SKIP_LIST_USE_ON_PMEM
/// @brief The default maximum index size [bytes].
constexpr size_t kDefaultIndexSize = 100 * 1024 * 1024;

/// @brief Do not use type checks in PMDK.
constexpr uint64_t kDefaultPMDKType = 0;

/// @brief Divide a node type from others.
constexpr uint64_t kNodePMDKType = 1;
#endif

/*######################################################################################
 * Utility enum and classes
 *####################################################################################*/

/**
 * @brief Return codes for APIs of a Skiplist.
 *
 */
enum ReturnCode {
  kKeyNotExist = -2,
  kKeyExist,
  kSuccess = 0,
};

/**
 * @brief Compare binary keys as CString. The end of every key must be '\\0'.
 *
 */
struct CompareAsCString {
  constexpr auto
  operator()(const void *a, const void *b) const noexcept  //
      -> bool
  {
    if (a == nullptr) return false;
    if (b == nullptr) return true;
    return strcmp(static_cast<const char *>(a), static_cast<const char *>(b)) < 0;
  }
};

/**
 * @tparam T a target class.
 * @retval true if a target class is variable-length data.
 * @retval false if a target class is static-length data.
 */
template <class T>
constexpr auto
IsVarLenData()  //
    -> bool
{
  if constexpr (std::is_same_v<T, char *> || std::is_same_v<T, std::byte *>) {
    return true;
  } else {
    return false;
  }
}

/**
 * @tparam T a PMwCAS target class.
 * @retval true if a target class can be updated by PMwCAS.
 * @retval false otherwise.
 */
template <class T>
constexpr auto
CanCAS()  //
    -> bool
{
  if constexpr (IsVarLenData<T>()) {
    return false;
  } else if constexpr (std::is_same_v<T, uint64_t> || std::is_pointer_v<T>) {
    return true;
  } else {
    return false;
  }
}

/*######################################################################################
 * Tuning parameters
 *####################################################################################*/

/// @brief The expected maximum size of a variable-length data.
constexpr size_t kMaxVarDataSize = SKIP_LIST_MAX_VARIABLE_DATA_SIZE;

}  // namespace dbgroup::index::skip_list

#endif  // SKIP_LIST_UTILITY_HPP
