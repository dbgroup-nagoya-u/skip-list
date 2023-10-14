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

#ifndef TEST_SKIP_LIST_COMMON_PMEM_HPP
#define TEST_SKIP_LIST_COMMON_PMEM_HPP

// system headers
#include <sys/stat.h>

// C++ standard libraries
#include <filesystem>
#include <string>

// external system libraries
#include <libpmem.h>
#include <libpmemobj.h>

// external sources
#include "gtest/gtest.h"

namespace dbgroup
{
// utility macros for expanding compile definitions as std::string
#define DBGROUP_ADD_QUOTES_INNER(x) #x                     // NOLINT
#define DBGROUP_ADD_QUOTES(x) DBGROUP_ADD_QUOTES_INNER(x)  // NOLINT

constexpr auto kModeRW = S_IRUSR | S_IWUSR;  // NOLINT

constexpr char kTestName[] = "tmp_pmem_test";

constexpr std::string_view kTmpPMEMPath = DBGROUP_ADD_QUOTES(DBGROUP_TEST_TMP_PMEM_PATH);

const std::string_view user_name = std::getenv("USER");

inline auto
GetTmpPoolPath()  //
    -> std::filesystem::path
{
  std::filesystem::path pool_path{kTmpPMEMPath};
  pool_path /= user_name;
  pool_path /= kTestName;

  return pool_path;
}

class TmpDirManager : public ::testing::Environment
{
 public:
  // constructor/destructor
  TmpDirManager() = default;
  TmpDirManager(const TmpDirManager &) = default;
  TmpDirManager(TmpDirManager &&) = default;
  TmpDirManager &operator=(const TmpDirManager &) = default;
  TmpDirManager &operator=(TmpDirManager &&) = default;
  ~TmpDirManager() override = default;

  // Override this to define how to set up the environment.
  void
  SetUp() override
  {
    // check the specified path
    if (kTmpPMEMPath.empty() || !std::filesystem::exists(kTmpPMEMPath)) {
      std::cerr << "WARN: The correct path to persistent memory is not set." << std::endl;
      GTEST_SKIP();
    }

    // prepare a temporary directory for testing
    const auto &pool_path = GetTmpPoolPath();
    std::filesystem::remove_all(pool_path);
    std::filesystem::create_directories(pool_path);
  }

  // Override this to define how to tear down the environment.
  void
  TearDown() override
  {
    const auto &pool_path = GetTmpPoolPath();
    std::filesystem::remove_all(pool_path);
  }
};

}  // namespace dbgroup

#endif  // TEST_SKIP_LIST_COMMON_PMEM_HPP
