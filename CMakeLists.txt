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
set(MASSTREE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/masstree")

#--------------------------------------------------------------------------------------#
# Build option for efficient memory allocators
#--------------------------------------------------------------------------------------#
option(INDEX_BENCH_USE_MIMALLOC "Use mimalloc as a memory allocator" OFF)
option(INDEX_BENCH_USE_JEMALLOC "Use jemalloc as a memory allocator" OFF)
option(INDEX_BENCH_OVERRIDE_JEMALLOC "Override entire memory allocation with jemalloc" OFF)

if(${INDEX_BENCH_USE_MIMALLOC} AND ${INDEX_BENCH_USE_JEMALLOC})
  message(FATAL_ERROR "Only one memory allocator can be specified.")
endif()
if(${INDEX_BENCH_OVERRIDE_JEMALLOC} AND (${INDEX_BENCH_USE_MIMALLOC} OR ${INDEX_BENCH_USE_JEMALLOC}))
  message(FATAL_ERROR "Only one memory allocator can be specified.")
endif()

#--------------------------------------------------------------------------------------#
# Build option for target indexes
#--------------------------------------------------------------------------------------#
option(INDEX_BENCH_BUILD_OPEN_BWTREE, "Build OpenBw-Tree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_MASSTREE, "Build Masstree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_PTREE, "Build PTree as a benchmarking target" OFF)

#--------------------------------------------------------------------------------------#
# Configure BzTree
#--------------------------------------------------------------------------------------#
if(${INDEX_BENCH_USE_MIMALLOC})
  message("Use mimalloc for running BzTree")
  set(BZTREE_USE_MIMALLOC "ON" CACHE BOOL "" FORCE)
  set(BZTREE_USE_JEMALLOC "OFF" CACHE BOOL "" FORCE)
elseif(${INDEX_BENCH_USE_JEMALLOC})
  message("Use jemalloc for running BzTree")
  set(BZTREE_USE_MIMALLOC "OFF" CACHE BOOL "" FORCE)
  set(BZTREE_USE_JEMALLOC "ON" CACHE BOOL "" FORCE)
else()
  set(BZTREE_USE_MIMALLOC "OFF" CACHE BOOL "" FORCE)
  set(BZTREE_USE_JEMALLOC "OFF" CACHE BOOL "" FORCE)
endif()

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
# Configure gflags
#--------------------------------------------------------------------------------------#

# prevent building unit tests
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/gflags")

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
  nlohmann_json::nlohmann_json
  cpp_utility
  bztree
)

if(${INDEX_BENCH_OVERRIDE_JEMALLOC})
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC REQUIRED IMPORTED_TARGET jemalloc)

  target_include_directories(
    index_bench PRIVATE
    ${JEMALLOC_INCLUDE_DIRS}
  )
  target_link_libraries(
    index_bench PRIVATE
    PkgConfig::JEMALLOC
  )
endif()

#--------------------------------------------------------------------------------------#
# Build OpenBw-Tree for benchmarking
#--------------------------------------------------------------------------------------#
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
# Build Masstree for benchmarking
#--------------------------------------------------------------------------------------#
if(${INDEX_BENCH_BUILD_MASSTREE})
  target_compile_definitions(
    index_bench PRIVATE
    INDEX_BENCH_BUILD_MASSTREE
  )
  target_link_libraries(
    index_bench PRIVATE
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
