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

# --------------------------------------------------------------------------------------#
# Use gflags to manage CLI options
# --------------------------------------------------------------------------------------#
find_package(gflags REQUIRED)

find_package(TBB REQUIRED)

# --------------------------------------------------------------------------------------#
# Build option for efficient memory allocators
# --------------------------------------------------------------------------------------#
option(INDEX_BENCH_OVERRIDE_JEMALLOC "Override entire memory allocation with jemalloc" OFF)
option(INDEX_BENCH_OVERRIDE_MIMALLOC "Override entire memory allocation with mimalloc" OFF)

if(${INDEX_BENCH_OVERRIDE_JEMALLOC} AND ${INDEX_BENCH_OVERRIDE_MIMALLOC})
  message(FATAL_ERROR "The benchmarks cannot use multiple memory allocators.")
endif()

if(${INDEX_BENCH_OVERRIDE_JEMALLOC})
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC REQUIRED IMPORTED_TARGET jemalloc)
endif()

if(${INDEX_BENCH_OVERRIDE_MIMALLOC})
  find_package(mimalloc 1.7 REQUIRED)
endif()

# --------------------------------------------------------------------------------------#
# Build option for target indexes
# --------------------------------------------------------------------------------------#
option(INDEX_BENCH_BUILD_YAKUSHIMA "Build yakushima as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_BTREE_OLC "Build optimistic-locking based B-tree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_OPEN_BWTREE "Build OpenBw-Tree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_MASSTREE "Build Masstree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_PTREE "Build PTree as a benchmarking target" OFF)

# --------------------------------------------------------------------------------------#
# Configurations for target indexes
# --------------------------------------------------------------------------------------#
set(INDEX_BENCH_PAGE_SIZE "" CACHE STRING "The default page size in bytes.")

if(NOT "${INDEX_BENCH_PAGE_SIZE}" STREQUAL "")
  set(BW_TREE_PAGE_SIZE "${INDEX_BENCH_PAGE_SIZE}" CACHE STRING "" FORCE)
  set(BZTREE_PAGE_SIZE "${INDEX_BENCH_PAGE_SIZE}" CACHE STRING "" FORCE)
endif()

set(INDEX_BENCH_MAX_DELTA_RECORD_NUM "" CACHE STRING "The maximum number of delta records if used.")

if(NOT "${INDEX_BENCH_MAX_DELTA_RECORD_NUM}" STREQUAL "")
  set(BW_TREE_MAX_DELTA_RECORD_NUM "${INDEX_BENCH_MAX_DELTA_RECORD_NUM}" CACHE STRING "" FORCE)
  set(BZTREE_MAX_DELTA_RECORD_NUM "${INDEX_BENCH_MAX_DELTA_RECORD_NUM}" CACHE STRING "" FORCE)
endif()

# --------------------------------------------------------------------------------------#
# Configure B-tree
# --------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/b-tree")

# --------------------------------------------------------------------------------------#
# Configure Bw-tree
# --------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/bw-tree")

# --------------------------------------------------------------------------------------#
# Configure BzTree
# --------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/bztree")

# --------------------------------------------------------------------------------------#
# Configure Masstree
# --------------------------------------------------------------------------------------#
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

set(INDEX_BENCH_MASSTREE_LIBS
  "${MASSTREE_SOURCE_DIR}/misc.o"
  "${MASSTREE_SOURCE_DIR}/checkpoint.o"
  "${MASSTREE_SOURCE_DIR}/query_masstree.o"
  "${MASSTREE_SOURCE_DIR}/value_string.o"
  "${MASSTREE_SOURCE_DIR}/value_array.o"
  "${MASSTREE_SOURCE_DIR}/value_versioned_array.o"
  "${MASSTREE_SOURCE_DIR}/string_slice.o"
  "${MASSTREE_SOURCE_DIR}/kvio.o"
  "${MASSTREE_SOURCE_DIR}/libjson.a"
)

# --------------------------------------------------------------------------------------#
# Configure nlohmann_json
# --------------------------------------------------------------------------------------#
set(JSON_BuildTests OFF CACHE INTERNAL "")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/json")

# --------------------------------------------------------------------------------------#
# Configure cpp-utility
# --------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/cpp-utility")

# --------------------------------------------------------------------------------------#
# Configure benchmarker
# --------------------------------------------------------------------------------------#
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/cpp-benchmark")

# --------------------------------------------------------------------------------------#
# Build Benchmark
# --------------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(ADD_BENCHMARK BENCHMARK_TARGET)
  add_executable(${BENCHMARK_TARGET}
    "${INDEX_BENCH_SOURCE_DIR}/src/${BENCHMARK_TARGET}.cpp"
  )
  target_compile_features(${BENCHMARK_TARGET} PRIVATE
    "cxx_std_17"
  )
  target_compile_options(${BENCHMARK_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
    $<$<OR:$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>,$<BOOL:${INDEX_BENCH_BUILD_PTREE}>>:-mcx16>
  )
  target_include_directories(${BENCHMARK_TARGET} PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/src"
    "${CMAKE_CURRENT_SOURCE_DIR}/external"
    $<$<BOOL:${INDEX_BENCH_OVERRIDE_JEMALLOC}>:${JEMALLOC_INCLUDE_DIRS}>
    $<$<BOOL:${INDEX_BENCH_BUILD_PTREE}>:${CMAKE_CURRENT_SOURCE_DIR}/external/PAM/c++/>
  )
  target_link_libraries(${BENCHMARK_TARGET} PRIVATE
    gflags
    TBB::tbb
    nlohmann_json::nlohmann_json
    cpp_utility::cpp_utility
    cpp_bench::cpp_bench
    b_tree::b_tree
    bw_tree::bw_tree
    bztree::bztree
    $<$<BOOL:${INDEX_BENCH_OVERRIDE_JEMALLOC}>:PkgConfig::JEMALLOC>
    $<$<BOOL:${INDEX_BENCH_OVERRIDE_MIMALLOC}>:mimalloc-static>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:atomic>
  )
  target_compile_definitions(${BENCHMARK_TARGET} PRIVATE
    $<$<BOOL:${INDEX_BENCH_BUILD_BTREE_PCL}>:INDEX_BENCH_BUILD_BTREE_PCL>
    $<$<BOOL:${INDEX_BENCH_BUILD_YAKUSHIMA}>:INDEX_BENCH_BUILD_YAKUSHIMA>
    $<$<BOOL:${INDEX_BENCH_BUILD_BTREE_OLC}>:INDEX_BENCH_BUILD_BTREE_OLC>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:INDEX_BENCH_BUILD_OPEN_BWTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:INDEX_BENCH_BUILD_OPEN_BWTREE>
    $<$<AND:$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>,$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">>:BWTREE_NODEBUG>
    $<$<BOOL:${INDEX_BENCH_BUILD_MASSTREE}>:INDEX_BENCH_BUILD_MASSTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_PTREE}>:INDEX_BENCH_BUILD_PTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_PTREE}>:HOMEGROWN>
  )

  if(${INDEX_BENCH_BUILD_MASSTREE})
    target_link_libraries(${BENCHMARK_TARGET} PRIVATE
      ${INDEX_BENCH_MASSTREE_LIBS}
    )
  endif()
endfunction()

# add executables
ADD_BENCHMARK("index_bench")
ADD_BENCHMARK("bulkload_bench")

# --------------------------------------------------------------------------------------#
# Build unit tests if required
# --------------------------------------------------------------------------------------#
option(INDEX_BENCH_BUILD_TESTS "Build unit tests" OFF)

if(${INDEX_BENCH_BUILD_TESTS})
  # enable CTest
  enable_testing()

  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
