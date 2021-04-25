cmake_minimum_required(VERSION "3.8")

# decline in-source build
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.")
endif()

# project information
project(
  mwcas_bench
  VERSION "0.1.0"
  DESCRIPTION "A template project for C++ language."
  LANGUAGES "CXX"
)

# define source directories
set(PROJECT_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")

# set compile options
set(
  MWCAS_BENCH_MAX_FIELD_NUM
  "8"
  CACHE STRING
  "The maximum number of target words of MwCAS."
)

#--------------------------------------------------------------------------------------#
# Configure MwCAS
#--------------------------------------------------------------------------------------#

set(MWCAS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/external/mwcas")

# set MwCAS capacity
set(MWCAS_CAPACITY "${MWCAS_BENCH_MAX_FIELD_NUM}" CACHE STRING "" FORCE)

add_subdirectory("${MWCAS_SOURCE_DIR}")

#--------------------------------------------------------------------------------------#
# Configure PMwCAS
#--------------------------------------------------------------------------------------#
configure_file(external/CMakeLists-pmwcas.txt.in pmwcas-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/pmwcas-download )
if(result)
  message(FATAL_ERROR "CMake step for pmwcas failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/pmwcas-download )
if(result)
  message(FATAL_ERROR "Build step for pmwcas failed: ${result}")
endif()

# build volatile PMwCAS
set(PMEM_BACKEND "Volatile" CACHE STRING "" FORCE)

# set MwCAS capacity
set(DESC_CAP "${MWCAS_BENCH_MAX_FIELD_NUM}" CACHE STRING "" FORCE)

# prevent building Google libraries
set(GOOGLE_FRAMEWORK OFF CACHE BOOL "" FORCE)

# prevent building unit tests
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_APPS OFF CACHE BOOL "" FORCE)

# add PMwCAS directly to our build
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/pmwcas-src
                 ${CMAKE_CURRENT_BINARY_DIR}/pmwcas-build
                 EXCLUDE_FROM_ALL)
set(PMWCAS_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/pmwcas-src")

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
# Build Benchmark
#--------------------------------------------------------------------------------------#

# build executable
add_executable(
  mwcas_bench
  "${PROJECT_SOURCE_DIR}/main.cpp"
)
target_compile_features(mwcas_bench PRIVATE "cxx_std_17")
target_compile_options(
  mwcas_bench PRIVATE
  -Wall
  -Wextra
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
  $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
)
target_compile_definitions(
  mwcas_bench PRIVATE
  MWCAS_BENCH_MAX_FIELD_NUM=${MWCAS_BENCH_MAX_FIELD_NUM}
)
target_include_directories(
  mwcas_bench PRIVATE
  "${PMWCAS_SOURCE_DIR}/"
  "${PMWCAS_SOURCE_DIR}/src"
  "${PMWCAS_SOURCE_DIR}/include"
)
target_link_libraries(
  mwcas_bench PRIVATE
  mwcas
  pmwcas_static
  gflags
  rt
)
add_dependencies(
  mwcas_bench
  mwcas
  pmwcas_static
)

# build unit tests if required
option(MWCAS_BENCH_BUILD_TESTS, "Build unit tests" OFF)
if(${MWCAS_BENCH_BUILD_TESTS})
  # enable CTest
  enable_testing()
  # include unit tests
  add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/test")
endif()
