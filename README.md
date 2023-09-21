# Skip Lists

![Ubuntu-20.04](https://github.com/dbgroup-nagoya-u/skip-list/workflows/Ubuntu-20.04/badge.svg?branch=main)

## Build

**Note**: This is a header only library. You can use it without pre-build.

### Prerequisites

```bash
sudo apt update && sudo apt install -y build-essential cmake
```

### Build Options

#### Tuning Parameters

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
      SKIP_LIST
    )
    ```

### Read/Write APIs

We provide the same read/write APIs for our index implementations. See [here](https://github.com/dbgroup-nagoya-u/index-benchmark/wiki/Common-APIs-for-Index-Implementations) for common APIs and usage examples.
