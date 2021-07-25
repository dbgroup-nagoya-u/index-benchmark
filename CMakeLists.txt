cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  index_bench
  VERSION "0.1.0"
  DESCRIPTION "A benchmark for thread-safe index implementations."
  LANGUAGES "CXX"
)

# define source directories
set(INDEX_BENCH_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(OPEN_BWTREE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/open_bwtree/src")

#--------------------------------------------------------------------------------------#
# Configure BzTree
#--------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/bztree")

#--------------------------------------------------------------------------------------#
# Configure gflags
#--------------------------------------------------------------------------------------#

# prevent building unit tests
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/gflags")

#--------------------------------------------------------------------------------------#
# Configure cpp-utility
#--------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/cpp-utility")

#--------------------------------------------------------------------------------------#
# Build Benchmark
#--------------------------------------------------------------------------------------#

# build executable
add_executable(
  index_bench
  "${INDEX_BENCH_SOURCE_DIR}/index_bench.cpp"
)
target_compile_features(index_bench PRIVATE "cxx_std_17")
target_compile_options(
  index_bench PRIVATE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_link_libraries(
  index_bench PRIVATE
  pthread
  gflags
  cpp_utility
  bztree
)

#--------------------------------------------------------------------------------------#
# Build OpenBw-Tree for benchmarking
#--------------------------------------------------------------------------------------#
option(INDEX_BENCH_BUILD_OPEN_BWTREE, "Build OpenBw-Tree as a benchmarking target" OFF)
if(${INDEX_BENCH_BUILD_OPEN_BWTREE})
  target_compile_options(
    index_bench PRIVATE
    -mcx16
  )
  target_compile_definitions(
    index_bench PRIVATE
    INDEX_BENCH_BUILD_OPEN_BWTREE
    BWTREE_NODEBUG
  )
  target_link_libraries(
    index_bench PRIVATE
    atomic
  )
endif()

#--------------------------------------------------------------------------------------#
# Build PTree for benchmarking
#--------------------------------------------------------------------------------------#
option(INDEX_BENCH_BUILD_PTREE, "Build PTree as a benchmarking target" OFF)
if(${INDEX_BENCH_BUILD_PTREE})
  target_compile_options(
    index_bench PRIVATE
    -mcx16
  )
  target_include_directories(
    index_bench PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/external/PAM/c++/
  )
  target_compile_definitions(
    index_bench PRIVATE
    INDEX_BENCH_BUILD_PTREE
    HOMEGROWN
  )
endif()

#--------------------------------------------------------------------------------------#
# Build unit tests if required
#--------------------------------------------------------------------------------------#
option(INDEX_BENCH_BUILD_TESTS, "Build unit tests" OFF)
if(${INDEX_BENCH_BUILD_TESTS})
  # enable CTest
  enable_testing()
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
