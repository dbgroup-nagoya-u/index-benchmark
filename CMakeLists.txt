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

#--------------------------------------------------------------------------------------#
# Configure BzTree
#--------------------------------------------------------------------------------------#
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/bztree)

#--------------------------------------------------------------------------------------#
# Configure gflags
#--------------------------------------------------------------------------------------#
configure_file(external/CMakeLists-gflags.txt.in gflags-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/gflags-download )
if(result)
  message(FATAL_ERROR "CMake step for gflags failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/gflags-download )
if(result)
  message(FATAL_ERROR "Build step for gflags failed: ${result}")
endif()

# prevent building unit tests
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)

# add gflags directly to our build
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/gflags-src
                 ${CMAKE_CURRENT_BINARY_DIR}/gflags-build
                 EXCLUDE_FROM_ALL)

#--------------------------------------------------------------------------------------#
# Configure cpp-utility
#--------------------------------------------------------------------------------------#
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/cpp-utility)

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
add_dependencies(
  cpp_utility
  bztree
)

# build unit tests if required
option(INDEX_BENCH_BUILD_TESTS, "Build unit tests" OFF)
if(${INDEX_BENCH_BUILD_TESTS})
  # enable CTest
  enable_testing()
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
