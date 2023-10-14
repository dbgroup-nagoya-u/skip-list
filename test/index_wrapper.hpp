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

#ifndef TEST_SKIP_LIST_INDEX_WRAPPER_HPP
#define TEST_SKIP_LIST_INDEX_WRAPPER_HPP

// external sources
#include "gtest/gtest.h"

// local sources
#include "common_pmem.hpp"
#include "skip_list/builder.hpp"

namespace dbgroup::index::test
{

template <class Key, class Payload, class Comp>
class IndexWrapper
{
 public:
  using Index = ::dbgroup::index::skip_list::SkipListOnPMEM<Key, Payload, Comp>;
  using Builder_t = ::dbgroup::index::skip_list::BuilderOnPMEM<Key, Payload, Comp>;
  using ScanKey = std::optional<std::tuple<const Key &, size_t, bool>>;

  IndexWrapper()
  {
    constexpr size_t kIndexSize = 8UL * 1024 * 1024 * 1024;
    constexpr size_t kGCSize = PMEMOBJ_MIN_POOL * DBGROUP_TEST_THREAD_NUM;
    constexpr size_t kGCThreadNum = 2;
    const auto &pool_path = GetTmpPoolPath();

    Builder_t builder{pool_path};
    builder.SetIndexSize(kIndexSize);
    builder.SetGCSize(kGCSize);
    builder.SetGCThreadNum(kGCThreadNum);
    index_ = builder.Build();
  }

  IndexWrapper(const IndexWrapper &) = delete;
  IndexWrapper(IndexWrapper &&) = delete;

  IndexWrapper &operator=(const IndexWrapper &) = delete;
  IndexWrapper &operator=(IndexWrapper &&) = delete;

  ~IndexWrapper()
  {  //
    index_->Clear();
  }

  auto
  Read(  //
      const Key &key,
      const size_t key_len = sizeof(Key))
  {
    return index_->Read(key, key_len);
  }

  auto
  Scan(  //
      const ScanKey &begin_key = std::nullopt,
      const ScanKey &end_key = std::nullopt)
  {
    return index_->Scan(begin_key, end_key);
  }

  auto
  Write(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key),
      const size_t pay_len = sizeof(Payload))
  {
    return index_->Write(key, payload, key_len, pay_len);
  }

  auto
  Insert(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key),
      const size_t pay_len = sizeof(Payload))
  {
    return index_->Insert(key, payload, key_len, pay_len);
  }

  auto
  Update(  //
      const Key &key,
      const Payload &payload,
      const size_t key_len = sizeof(Key),
      const size_t pay_len = sizeof(Payload))
  {
    return index_->Update(key, payload, key_len, pay_len);
  }

  auto
  Delete(  //
      const Key &key,
      const size_t key_len = sizeof(Key))
  {
    return index_->Delete(key, key_len);
  }

  template <class Entry>
  auto
  Bulkload(  //
      const std::vector<Entry> &entries,
      const size_t thread_num = 1)
  {
    return index_->Bulkload(entries, thread_num);
  }

 private:
  std::unique_ptr<Index> index_{nullptr};
};

}  // namespace dbgroup::index::test

#endif  // TEST_SKIP_LIST_INDEX_WRAPPER_HPP
