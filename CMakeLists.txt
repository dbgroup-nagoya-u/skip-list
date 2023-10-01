cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  skip_list
  VERSION "0.1.0"
  DESCRIPTION "An open-source skip list library."
  LANGUAGES "CXX"
)

# check SSE support for spinlock hints
include(CheckIncludeFileCXX)
CHECK_INCLUDE_FILE_CXX("xmmintrin.h" SKIP_LIST_HAS_SPINLOCK_HINT)

#--------------------------------------------------------------------------------------#
# Configurations
#--------------------------------------------------------------------------------------#

set(SKIP_LIST_MAX_VARIABLE_DATA_SIZE "32" CACHE STRING "The expected maximum size of a variable-length data.")

#--------------------------------------------------------------------------------------#
# Configure external libraries
#--------------------------------------------------------------------------------------#

# configure GC
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/memory-manager")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET dbgroup::${PROJECT_NAME})
  add_library(${PROJECT_NAME} INTERFACE)
  add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
  target_compile_features(${PROJECT_NAME} INTERFACE
    "cxx_std_17"
  )
  target_compile_definitions(${PROJECT_NAME} INTERFACE
    SKIP_LIST_MAX_VARIABLE_DATA_SIZE=${SKIP_LIST_MAX_VARIABLE_DATA_SIZE}
    $<$<BOOL:${SKIP_LIST_HAS_SPINLOCK_HINT}>:SKIP_LIST_HAS_SPINLOCK_HINT>
  )
  target_include_directories(${PROJECT_NAME} INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(${PROJECT_NAME} INTERFACE
    dbgroup::memory_manager
  )
endif()

#--------------------------------------------------------------------------------------#
# Build unit tests
#--------------------------------------------------------------------------------------#

option(SKIP_LIST_BUILD_TESTS "Build unit tests for Bc-tree" OFF)
if(${SKIP_LIST_BUILD_TESTS})
  enable_testing()
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
