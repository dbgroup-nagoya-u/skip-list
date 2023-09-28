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

#ifndef SKIP_LIST_COMPONENT_COMMON_HPP
#define SKIP_LIST_COMPONENT_COMMON_HPP

// C++ standard libraries
#include <cstring>
#include <functional>
#include <memory>

// external system libraries
#ifdef SKIP_LIST_HAS_SPINLOCK_HINT
#include <xmmintrin.h>
#endif

// local sources
#include "skip_list/utility.hpp"

// macro definitions
#ifdef SKIP_LIST_HAS_SPINLOCK_HINT
#define SKIP_LIST_SPINLOCK_HINT _mm_pause();  // NOLINT
#else
#define SKIP_LIST_SPINLOCK_HINT /* do nothing */
#endif

namespace dbgroup::index::skip_list::component
{
/*######################################################################################
 * Internal constants
 *####################################################################################*/

/// @brief The most significant bit represents a deleted value.
constexpr uint64_t kDelBit = 1UL << 63UL;

/*######################################################################################
 * Internal utility functions
 *####################################################################################*/

/**
 * @tparam Comp a comparator class.
 * @tparam T a target class.
 * @param obj_1 an object to be compared.
 * @param obj_2 another object to be compared.
 * @retval true if given objects are equivalent.
 * @retval false if given objects are different.
 */
template <class Comp, class T>
constexpr auto
IsEqual(  //
    const T &obj_1,
    const T &obj_2)  //
    -> bool
{
  return !Comp{}(obj_1, obj_2) && !Comp{}(obj_2, obj_1);
}

/**
 * @brief Shift a memory address by byte offsets.
 *
 * @param addr an original address.
 * @param offset an offset to shift.
 * @return a shifted address.
 */
constexpr auto
ShiftAddr(  //
    const void *addr,
    const int64_t offset)  //
    -> void *
{
  return static_cast<std::byte *>(const_cast<void *>(addr)) + offset;
}

/**
 * @brief Allocate a memory region with alignments.
 *
 * @tparam T a target class.
 * @param size the size of target class.
 * @return the address of an allocated one.
 */
template <class T>
inline auto
Allocate(size_t size = sizeof(T))  //
    -> T *
{
  constexpr auto kAlign = static_cast<std::align_val_t>(alignof(T));
  return reinterpret_cast<T *>(::operator new(size, kAlign));
}

/**
 * @brief A deleter function to release aligned pages.
 *
 * @tparam T a target class.
 * @param ptr the address of allocations to be released.
 */
template <class T>
inline void
Release(T *ptr)
{
  constexpr auto kAlign = static_cast<std::align_val_t>(alignof(T));
  ::operator delete(ptr, kAlign);
}

}  // namespace dbgroup::index::skip_list::component

#endif  // SKIP_LIST_COMPONENT_COMMON_HPP
