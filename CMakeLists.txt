cmake_minimum_required(VERSION "3.11")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  index_bench
  VERSION "0.1.0"
  DESCRIPTION "A benchmark for thread-safe index implementations."
  LANGUAGES "CXX" "C"
)

# enable features
include(FetchContent)
include(ProcessorCount)

#--------------------------------------------------------------------------------------#
# Build option for target indexes
#--------------------------------------------------------------------------------------#

option(INDEX_BENCH_BUILD_LONG_KEYS "Build keys with sizes of 16/32/64/128 bytes." OFF)
option(INDEX_BENCH_BUILD_OPTIMIZED_B_TREES "Build the optimized B+trees for fixed-length keys." OFF)

#--------------------------------------------------------------------------------------#
# Build option for optional indexes
#--------------------------------------------------------------------------------------#

option(INDEX_BENCH_BUILD_SKIP_LIST "Build skip list as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_B_TREE_OLC "Build B+tree with OLC as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_B_TREE_OPTIQL "Build B+tree with OptiQL as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_OPEN_BWTREE "Build OpenBw-Tree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_MASSTREE "Build Masstree as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_YAKUSHIMA "Build Yakushima as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_ART_OLC "Build ART with OLC as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_HYDRALIST "Build HydraList as a benchmarking target" OFF)
option(INDEX_BENCH_BUILD_ALEX_OLC "Build Alex with OLC as a benchmarking target" OFF)

set(INDEX_BENCH_COMPARE_WITH_SOTA OFF)
if(${INDEX_BENCH_BUILD_B_TREE_OLC}
  OR ${INDEX_BENCH_BUILD_B_TREE_OPTIQL}
  OR ${INDEX_BENCH_BUILD_OPEN_BWTREE}
  OR ${INDEX_BENCH_BUILD_MASSTREE}
  OR ${INDEX_BENCH_BUILD_YAKUSHIMA}
  OR ${INDEX_BENCH_BUILD_ART_OLC}
  OR ${INDEX_BENCH_BUILD_HYDRALIST}
  OR ${INDEX_BENCH_BUILD_ALEX_OLC}
)
  message("[${PROJECT_NAME}] Use 8-bytes unsigned integer keys for comparison among the state-of-the-art indexes.")
  set(INDEX_BENCH_COMPARE_WITH_SOTA ON)
endif()

#--------------------------------------------------------------------------------------#
# Configurations for target indexes
#--------------------------------------------------------------------------------------#

ProcessorCount(TMP_PROC_COUNT)
if(NOT TMP_PROC_COUNT EQUAL 0)
  set(INDEX_BENCH_MAX_CORES "${TMP_PROC_COUNT}" CACHE STRING "The number of logical cores.")
  message("[${PROJECT_NAME}] The detected number of cores: ${INDEX_BENCH_MAX_CORES}")
else()
  set(INDEX_BENCH_MAX_CORES "128" CACHE STRING "The number of logical cores.")
  message(WARNING "[${PROJECT_NAME}] The number of cores could not be detected. Please set INDEX_BENCH_MAX_CORES explicitly.")
endif()

#--------------------------------------------------------------------------------------#
# Use gflags to manage CLI options
#--------------------------------------------------------------------------------------#

find_package(gflags REQUIRED)

#--------------------------------------------------------------------------------------#
# Build option for efficient memory allocators
#--------------------------------------------------------------------------------------#

option(INDEX_BENCH_OVERRIDE_MIMALLOC "Override entire memory allocation with mimalloc" OFF)
if(${INDEX_BENCH_OVERRIDE_MIMALLOC})
  set(MI_BUILD_SHARED OFF CACHE BOOL "Disable mimalloc's unit tests." FORCE)
  set(MI_BUILD_OBJECT OFF CACHE BOOL "Disable mimalloc's unit tests." FORCE)
  set(MI_BUILD_TESTS OFF CACHE BOOL "Disable mimalloc's unit tests." FORCE)
  FetchContent_Declare(
    mimalloc
    GIT_REPOSITORY "https://github.com/microsoft/mimalloc"
    GIT_TAG "8c532c32c3c96e5ba1f2283e032f69ead8add00f" # v2.1.7
  )
  FetchContent_MakeAvailable(mimalloc)
endif()

#--------------------------------------------------------------------------------------#
# Configure our libraries
#--------------------------------------------------------------------------------------#

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/cpp-utility")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/cpp-benchmark")

#--------------------------------------------------------------------------------------#
# Configure nlohmann_json
#--------------------------------------------------------------------------------------#

set(JSON_BuildTests OFF CACHE INTERNAL "")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/json")

#--------------------------------------------------------------------------------------#
# Configure our B+trees
#--------------------------------------------------------------------------------------#

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/b-tree")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/bw-tree")
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/bztree")

#--------------------------------------------------------------------------------------#
# Configure the state-of-the-art indexes
#--------------------------------------------------------------------------------------#

if(${INDEX_BENCH_BUILD_SKIP_LIST})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/skip_list.cmake")
endif()

if(${INDEX_BENCH_BUILD_B_TREE_OLC})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/b_tree_olc.cmake")
endif()

if(${INDEX_BENCH_BUILD_B_TREE_OPTIQL})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/b_tree_optiql.cmake")
endif()

if(${INDEX_BENCH_BUILD_OPEN_BWTREE})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/open_bw_tree.cmake")
endif()

if(${INDEX_BENCH_BUILD_MASSTREE})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/masstree.cmake")
endif()

if(${INDEX_BENCH_BUILD_YAKUSHIMA})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/yakushima.cmake")
endif()

if(${INDEX_BENCH_BUILD_ART_OLC})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/art_olc.cmake")
endif()

if(${INDEX_BENCH_BUILD_HYDRALIST})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/hydralist.cmake")
endif()

if(${INDEX_BENCH_BUILD_ALEX_OLC})
  include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/alex_olc.cmake")
endif()

#--------------------------------------------------------------------------------------#
# Build Benchmark
#--------------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(ADD_BENCHMARK BENCHMARK_TARGET)
  add_executable(${BENCHMARK_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/src/${BENCHMARK_TARGET}.cpp"
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
  )
  target_compile_definitions(${BENCHMARK_TARGET} PRIVATE
    INDEX_BENCH_MAX_CORES=${INDEX_BENCH_MAX_CORES}
    $<$<BOOL:${INDEX_BENCH_BUILD_LONG_KEYS}>:INDEX_BENCH_BUILD_LONG_KEYS>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPTIMIZED_B_TREES}>:INDEX_BENCH_BUILD_OPTIMIZED_B_TREES>
    $<$<BOOL:${INDEX_BENCH_BUILD_SKIP_LIST}>:INDEX_BENCH_BUILD_SKIP_LIST>
    $<$<BOOL:${INDEX_BENCH_BUILD_B_TREE_OLC}>:INDEX_BENCH_BUILD_B_TREE_OLC>
    $<$<BOOL:${INDEX_BENCH_BUILD_B_TREE_OPTIQL}>:INDEX_BENCH_BUILD_B_TREE_OPTIQL>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:INDEX_BENCH_BUILD_OPEN_BWTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_MASSTREE}>:INDEX_BENCH_BUILD_MASSTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_YAKUSHIMA}>:INDEX_BENCH_BUILD_YAKUSHIMA>
    $<$<BOOL:${INDEX_BENCH_BUILD_ART_OLC}>:INDEX_BENCH_BUILD_ART_OLC>
    $<$<BOOL:${INDEX_BENCH_BUILD_HYDRALIST}>:INDEX_BENCH_BUILD_HYDRALIST>
    $<$<BOOL:${INDEX_BENCH_BUILD_ALEX_OLC}>:INDEX_BENCH_BUILD_ALEX_OLC>
    $<$<BOOL:${INDEX_BENCH_COMPARE_WITH_SOTA}>:INDEX_BENCH_COMPARE_WITH_SOTA>
  )
  target_include_directories(${BENCHMARK_TARGET} PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/src"
  )
  target_link_libraries(${BENCHMARK_TARGET} PRIVATE
    dbgroup::cpp_utility
    dbgroup::cpp_bench
    dbgroup::b_tree
    dbgroup::bw_tree
    dbgroup::bztree
    gflags
    nlohmann_json::nlohmann_json
    $<$<BOOL:${INDEX_BENCH_OVERRIDE_MIMALLOC}>:mimalloc-static>
    $<$<BOOL:${INDEX_BENCH_BUILD_SKIP_LIST}>:dbgroup::skip_list>
    $<$<BOOL:${INDEX_BENCH_BUILD_B_TREE_OLC}>:open_bw::b_tree_olc>
    $<$<BOOL:${INDEX_BENCH_BUILD_B_TREE_OPTIQL}>:b_tree_optiql::b_tree_optiql>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:open_bw::bw_tree>
    $<$<BOOL:${INDEX_BENCH_BUILD_MASSTREE}>:masstree::masstree>
    $<$<BOOL:${INDEX_BENCH_BUILD_YAKUSHIMA}>:tsurugi::yakushima>
    $<$<BOOL:${INDEX_BENCH_BUILD_ART_OLC}>:open_bw::art_olc>
    $<$<BOOL:${INDEX_BENCH_BUILD_HYDRALIST}>:hydralist::hydralist>
    $<$<BOOL:${INDEX_BENCH_BUILD_ALEX_OLC}>:GRE::alex_olc>
  )
endfunction()

# add executables
ADD_BENCHMARK("index_bench")

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
