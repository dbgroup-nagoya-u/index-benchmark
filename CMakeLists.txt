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
set(INDEX_BENCH_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
set(OPEN_BWTREE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/open_bwtree/src")
set(MASSTREE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/masstree")

#--------------------------------------------------------------------------------------#
# Use gflags to manage CLI options
#--------------------------------------------------------------------------------------#

find_package(gflags REQUIRED)

#--------------------------------------------------------------------------------------#
# Build option for efficient memory allocators
#--------------------------------------------------------------------------------------#
option(INDEX_BENCH_OVERRIDE_JEMALLOC "Override entire memory allocation with jemalloc" OFF)

#--------------------------------------------------------------------------------------#
# Build option for target indexes
#--------------------------------------------------------------------------------------#
option(INDEX_BENCH_BUILD_BTREE_OLC "Build optimistic-locking based B-tree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_OPEN_BWTREE "Build OpenBw-Tree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_MASSTREE "Build Masstree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_PTREE "Build PTree as a benchmarking target" OFF)

#--------------------------------------------------------------------------------------#
# Configure Bw-tree
#--------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/bw-tree")

#--------------------------------------------------------------------------------------#
# Configure BzTree
#--------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/bztree")

#--------------------------------------------------------------------------------------#
# Configure Masstree
#--------------------------------------------------------------------------------------#

if(${INDEX_BENCH_BUILD_MASSTREE} AND NOT EXISTS "${MASSTREE_SOURCE_DIR}/config.h")
  execute_process(
    COMMAND ./bootstrap.sh
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${MASSTREE_SOURCE_DIR}
  )
  if(result)
    message(FATAL_ERROR "Bootstrap step for Masstree failed: ${result}")
  endif()
  execute_process(
    COMMAND ./configure --disable-assertions
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${MASSTREE_SOURCE_DIR}
  )
  if(result)
    message(FATAL_ERROR "Configure step for Masstree failed: ${result}")
  endif()
  execute_process(
    COMMAND make -j
    RESULT_VARIABLE result
    WORKING_DIRECTORY ${MASSTREE_SOURCE_DIR}
  )
  if(result)
    message(FATAL_ERROR "Build step for Masstree failed: ${result}")
  endif()
endif()

#--------------------------------------------------------------------------------------#
# Configure nlohmann_json
#--------------------------------------------------------------------------------------#

set(JSON_BuildTests OFF CACHE INTERNAL "")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/json")

#--------------------------------------------------------------------------------------#
# Configure cpp-utility
#--------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/cpp-utility")

#--------------------------------------------------------------------------------------#
# Configure benchmarker
#--------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/cpp-benchmark")

#--------------------------------------------------------------------------------------#
# Build Benchmark
#--------------------------------------------------------------------------------------#

# build executable
add_executable(index_bench
  "${INDEX_BENCH_SOURCE_DIR}/src/index_bench.cpp"
)
target_compile_features(index_bench PRIVATE
  "cxx_std_17"
)
target_compile_options(index_bench PRIVATE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_include_directories(index_bench PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/external"
)
target_link_libraries(index_bench PRIVATE
  gflags
  nlohmann_json::nlohmann_json
  cpp_utility::cpp_utility
  cpp_bench::cpp_bench
  bw_tree::bw_tree
  bztree::bztree
)
target_compile_definitions(index_bench PRIVATE
  $<$<BOOL:${INDEX_BENCH_BUILD_BTREE_OLC}>:INDEX_BENCH_BUILD_BTREE_OLC>
)

if(${INDEX_BENCH_OVERRIDE_JEMALLOC})
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC REQUIRED IMPORTED_TARGET jemalloc)

  target_include_directories(index_bench PRIVATE
    ${JEMALLOC_INCLUDE_DIRS}
  )
  target_link_libraries(index_bench PRIVATE
    PkgConfig::JEMALLOC
  )
endif()

if(${INDEX_BENCH_OVERRIDE_MIMALLOC})
  find_package(mimalloc 1.7 REQUIRED)

  target_link_libraries(index_bench PRIVATE
    mimalloc-static
  )
  target_compile_definitions(index_bench PRIVATE
    INDEX_BENCH_OVERRIDE_MIMALLOC
  )
endif()

#--------------------------------------------------------------------------------------#
# Build OpenBw-Tree for benchmarking
#--------------------------------------------------------------------------------------#
if(${INDEX_BENCH_BUILD_OPEN_BWTREE})
  target_compile_options(index_bench PRIVATE
    -mcx16
  )
  target_compile_definitions(index_bench PRIVATE
    INDEX_BENCH_BUILD_OPEN_BWTREE
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:BWTREE_NODEBUG>
  )
  target_link_libraries(index_bench PRIVATE
    atomic
  )
endif()

#--------------------------------------------------------------------------------------#
# Build Masstree for benchmarking
#--------------------------------------------------------------------------------------#
if(${INDEX_BENCH_BUILD_MASSTREE})
  target_compile_definitions(index_bench PRIVATE
    INDEX_BENCH_BUILD_MASSTREE
  )
  target_link_libraries(index_bench PRIVATE
    ${MASSTREE_SOURCE_DIR}/misc.o
    ${MASSTREE_SOURCE_DIR}/checkpoint.o
    ${MASSTREE_SOURCE_DIR}/query_masstree.o
    ${MASSTREE_SOURCE_DIR}/value_string.o
    ${MASSTREE_SOURCE_DIR}/value_array.o
    ${MASSTREE_SOURCE_DIR}/value_versioned_array.o
    ${MASSTREE_SOURCE_DIR}/string_slice.o
    ${MASSTREE_SOURCE_DIR}/kvio.o
    ${MASSTREE_SOURCE_DIR}/libjson.a
  )
endif()

#--------------------------------------------------------------------------------------#
# Build PTree for benchmarking
#--------------------------------------------------------------------------------------#
if(${INDEX_BENCH_BUILD_PTREE})
  target_compile_options(index_bench PRIVATE
    -mcx16
  )
  target_include_directories(index_bench PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/external/PAM/c++/
  )
  target_compile_definitions(index_bench PRIVATE
    INDEX_BENCH_BUILD_PTREE
    HOMEGROWN
  )
endif()

#--------------------------------------------------------------------------------------#
# Build unit tests if required
#--------------------------------------------------------------------------------------#
option(INDEX_BENCH_BUILD_TESTS "Build unit tests" OFF)
if(${INDEX_BENCH_BUILD_TESTS})
  # enable CTest
  enable_testing()
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
