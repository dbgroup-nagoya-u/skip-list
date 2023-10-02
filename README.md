# Skip Lists

[![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/skip-list/actions/workflows/unit_tests.yaml/badge.svg)](https://github.com/dbgroup-nagoya-u/skip-list/actions/workflows/unit_tests.yaml)

## Build

**Note**: This is a header only library. You can use it without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Tuning Parameters

- `SKIP_LIST_MAX_VARIABLE_DATA_SIZE`: The expected maximum size of a variable-length data (default `32`).

#### Build Options for Unit Testing

- `SKIP_LIST_BUILD_TESTS`: Build unit tests for this library if `ON` (default `OFF`).
- `DBGROUP_TEST_THREAD_NUM`: The maximum number of threads to test for concurrency (default `8`).
- `DBGROUP_TEST_RANDOM_SEED`: A fixed seed value to reproduce the results of unit tests (default `0`).
- `DBGROUP_TEST_EXEC_NUM`: The number of operations performed per thread (default `1E5`).
- `DBGROUP_TEST_OVERRIDE_MIMALLOC`: Override entire memory allocation with mimalloc (default `OFF`).
    - NOTE: we use `find_package(mimalloc 1.7 REQUIRED)` to link mimalloc.

### Build and Run Unit Tests

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DSKIP_LIST_BUILD_TESTS=ON ..
make -j
ctest -C Release
```

## Usage

### Linking by CMake

1. Download this library any way you like (e.g., `git submodule`).

    ```bash
    cd <your_project_workspace>
    mkdir external
    git submodule add https://github.com/dbgroup-nagoya-u/skip-list.git external/skip-list
    ```

1. Add this library to your build in `CMakeLists.txt`.

    ```cmake
    add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/skip-list")

    add_executable(
      <target_bin_name>
      [<source> ...]
    )
    target_link_libraries(
      <target_bin_name> PRIVATE
      dbgroup::skip_list
    )
    ```

### Read/Write APIs

We provide the same read/write APIs for our index implementations. See [here](https://github.com/dbgroup-nagoya-u/index-benchmark/wiki/Common-APIs-for-Index-Implementations) for common APIs and usage examples.

### Using Inline Payloads

We dynamically allocate a region of memory for a payload and update/delete it using CAS instructions. However, if your payloads meet the following conditions, you can embed your payloads directly into skip list nodes as inline (note that we consider any pointers and `uint64_t` to be inline payloads by default):

1. the length of a class is `8` (i.e., `static_assert(sizeof(<payload_class>) == 8)`),
2. the last bit is reserved for a delete flag and initialized with zeros, and
3. a specialized `CanCAS` function is implemented in the `dbgroup::index::skip_list` namespace.

The following snippet is an example implementation.

```cpp
/**
 * @brief An example class to represent inline payloads.
 *
 */
struct MyClass {
  /// an actual payload
  uint64_t data : 63;

  /// reserve at least one bit for a delete flag
  uint64_t control_bits : 1;

  // control bits must be initialzed by zeros
  constexpr MyClass() : data{}, control_bits{0} {}

  ~MyClass() = default;

  // target class must be trivially copyable
  constexpr MyClass(const MyClass &) = default;
  constexpr MyClass &operator=(const MyClass &) = default;
  constexpr MyClass(MyClass &&) = default;
  constexpr MyClass &operator=(MyClass &&) = default;

  // enable std::less to compare this class
  constexpr bool
  operator<(const MyClass &comp) const
  {
    return data < comp.data;
  }
};

namespace dbgroup::index::skip_list
{
/**
 * @brief An example specialization to enable in-place updating.
 *
 */
template <>
constexpr bool
CanCAS<MyClass>()
{
  return true;
}

}  // namespace dbgroup::index::skip_list
```
