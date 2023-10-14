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
#include <iostream>
#include <memory>

// external system libraries
#ifdef SKIP_LIST_HAS_SPINLOCK_HINT
#include <xmmintrin.h>
#endif

#ifdef SKIP_LIST_USE_ON_PMEM
#include <libpmem.h>
#include <libpmemobj.h>
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

#ifdef SKIP_LIST_USE_ON_PMEM
/**
 * @brief Allocate a region of persistent memory by using a given pool.
 *
 * Internally, this function uses pmemobj_zalloc, and so an allocated region is filled
 * with zeros.
 *
 * @param pop the pointer to a pmemobj pool instance.
 * @param oid the address of a PMEMoid instance to store an allocated region.
 * @param size the desired size of allocation.
 */
inline void
AllocatePmem(  //
    PMEMobjpool *pop,
    PMEMoid *oid,
    const size_t size)
{
  const auto rc = pmemobj_alloc(pop, oid, size, kDefaultPMDKType, nullptr, nullptr);
  if (rc != 0) {
    throw std::runtime_error{pmemobj_errormsg()};
  }
}
#endif

}  // namespace dbgroup::index::skip_list::component

#endif  // SKIP_LIST_COMPONENT_COMMON_HPP
