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

#------------------------------------------------------------------------------#
# Build options for benchmark
#------------------------------------------------------------------------------#

set(
  INDEX_BENCH_PAGE_SIZE
  "1024" CACHE STRING
  "A page size"
)

set(
  INDEX_BENCH_MAX_VARLEN_DATA_SIZE
  "32" CACHE STRING
  "The expected maximum size of a variable-length data."
)

option(
  INDEX_BENCH_OVERRIDE_MIMALLOC
  "Override entire memory allocation with mimalloc."
  OFF
)

ProcessorCount(TMP_PROC_COUNT)
if(NOT TMP_PROC_COUNT EQUAL 0)
  set(
    INDEX_BENCH_MAX_CORES
    "${TMP_PROC_COUNT}" CACHE STRING
    "The number of logical cores."
  )
  message(STATUS
    "[${PROJECT_NAME}] The detected number of cores: ${INDEX_BENCH_MAX_CORES}"
  )
else()
  set(
    INDEX_BENCH_MAX_CORES
    "128" CACHE STRING
    "The number of logical cores."
  )
  message(WARNING
    "[${PROJECT_NAME}] The number of cores could not be detected. Please set INDEX_BENCH_MAX_CORES explicitly."
  )
endif()

#------------------------------------------------------------------------------#
# Configurations
#------------------------------------------------------------------------------#

# use gflags to manage CLI options
find_package(gflags REQUIRED)

# use our utility
FetchContent_Declare(
  cpp-utility
  GIT_REPOSITORY "https://github.com/dbgroup-nagoya-u/cpp-utility"
  GIT_TAG "e00883c3d14da4a0f6e68691c45f7e9a558e4bec"
)
FetchContent_MakeAvailable(cpp-utility)

# use our benchmark utility
FetchContent_Declare(
  cpp-benchmark
  GIT_REPOSITORY "https://github.com/dbgroup-nagoya-u/cpp-benchmark"
  GIT_TAG "ab0e87838d620e1e086baaf512230b0428ef9f13"
)
FetchContent_MakeAvailable(cpp-benchmark)

# use YAML parser
set(YAML_CPP_BUILD_TOOLS OFF CACHE INTERNAL "Disable parse tools")
FetchContent_Declare(
  yaml-cpp
  GIT_REPOSITORY "https://github.com/jbeder/yaml-cpp"
  GIT_TAG        "56e3bb550c91fd7005566f19c079cb7a503223cf" # v0.9.0
)
FetchContent_MakeAvailable(yaml-cpp)

# use mimalloc as efficient memory allocator
if(${INDEX_BENCH_OVERRIDE_MIMALLOC})
  set(MI_BUILD_SHARED OFF CACHE BOOL "Disable mimalloc's unit tests." FORCE)
  set(MI_BUILD_OBJECT OFF CACHE BOOL "Disable mimalloc's unit tests." FORCE)
  set(MI_BUILD_TESTS OFF CACHE BOOL "Disable mimalloc's unit tests." FORCE)
  FetchContent_Declare(
    mimalloc
    GIT_REPOSITORY "https://github.com/microsoft/mimalloc"
    GIT_TAG "30b2d9d89099bee08e9f67a1ffb3e12e7ba45227" # v3.3.2
  )
  FetchContent_MakeAvailable(mimalloc)
endif()

#------------------------------------------------------------------------------#
# Build benchmark library
#------------------------------------------------------------------------------#

# define as library
add_library(${PROJECT_NAME} STATIC
  "${CMAKE_CURRENT_SOURCE_DIR}/src/workload/var_len_data.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/workload/key_space.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/workload/operation_selector.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/workload/zipf_workload.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/workload/timestamp_workload.cpp"
)
add_library(dbgroup::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
target_compile_features(${PROJECT_NAME} PUBLIC
  "cxx_std_20"
)
target_compile_options(${PROJECT_NAME} PRIVATE
  $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Release">:-march=native>
  $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Debug">:-g3>
  -Wall
  -Wextra
)
target_compile_definitions(${PROJECT_NAME} PUBLIC
  INDEX_BENCH_MAX_VARLEN_DATA_SIZE=${INDEX_BENCH_MAX_VARLEN_DATA_SIZE}
  INDEX_BENCH_MAX_CORES=${INDEX_BENCH_MAX_CORES}
  $<$<BOOL:${INDEX_BENCH_BUILD_LONG_KEYS}>:INDEX_BENCH_BUILD_LONG_KEYS>
)
target_include_directories(${PROJECT_NAME} PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
target_link_libraries(${PROJECT_NAME} PUBLIC
  dbgroup::cpp_utility
  dbgroup::cpp_bench
  gflags
  yaml-cpp::yaml-cpp
  $<$<BOOL:${INDEX_BENCH_OVERRIDE_MIMALLOC}>:mimalloc-static>
)

#------------------------------------------------------------------------------#
# Configure competitors
#------------------------------------------------------------------------------#

file(GLOB INDEX_BENCH_COMPETITORS CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/*.cmake"
)
foreach(INDEX_BENCH_COMPETITOR ${INDEX_BENCH_COMPETITORS})
  include(${INDEX_BENCH_COMPETITOR})
endforeach()

#------------------------------------------------------------------------------#
# Build each benchmark
#------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(ADD_BENCHMARK BENCHMARK_TARGET)
  add_executable(${BENCHMARK_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/src/${BENCHMARK_TARGET}.cpp"
  )
  target_compile_features(${BENCHMARK_TARGET} PRIVATE
    "cxx_std_20"
  )
  target_compile_options(${BENCHMARK_TARGET} PRIVATE
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Release">:-march=native>
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Debug">:-g3>
    -Wall
    -Wextra
  )
  target_link_libraries(${BENCHMARK_TARGET} PRIVATE
    dbgroup::${PROJECT_NAME}
  )
endfunction()

# add executables
ADD_BENCHMARK("run_bench")

#------------------------------------------------------------------------------#
# Build unit tests if required
#------------------------------------------------------------------------------#
option(INDEX_BENCH_BUILD_TESTS "Build unit tests" OFF)
if(${INDEX_BENCH_BUILD_TESTS})
  # enable CTest
  enable_testing()
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
