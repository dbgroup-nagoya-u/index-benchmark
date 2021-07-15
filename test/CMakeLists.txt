#--------------------------------------------------------------------------------------#
# Configure GoogleTest
#--------------------------------------------------------------------------------------#

# Prevent overriding the parent INDEX_BENCH's compiler/linker settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/external/googletest")

#--------------------------------------------------------------------------------------#
# Build Unit Tests
#--------------------------------------------------------------------------------------#

# define function to add unit tests in the same format
function(ADD_INDEX_BENCH_TEST INDEX_BENCH_TEST_TARGET)
  add_executable(
    ${INDEX_BENCH_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${INDEX_BENCH_TEST_TARGET}.cpp"
  )
  target_compile_features(
    ${INDEX_BENCH_TEST_TARGET} PRIVATE
    "cxx_std_17"
  )
  target_compile_options(
    ${INDEX_BENCH_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_compile_definitions(
    ${INDEX_BENCH_TEST_TARGET} PRIVATE
    INDEX_BENCH_MAX_FIELD_NUM=${INDEX_BENCH_MAX_FIELD_NUM}
  )
  target_include_directories(
    ${INDEX_BENCH_TEST_TARGET} PRIVATE
    "${INDEX_BENCH_SOURCE_DIR}"
  )
  target_link_libraries(
    ${INDEX_BENCH_TEST_TARGET} PRIVATE
    gflags
    cpp_utility
    bztree
    gtest_main
  )

  #--------------------------------------------------------------------------------------#
  # Build OpenBw-Tree for benchmarking
  #--------------------------------------------------------------------------------------#
  if(${INDEX_BENCH_BUILD_OPEN_BWTREE})
    target_compile_definitions(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      INDEX_BENCH_BUILD_OPEN_BWTREE
    )
  endif()

  #--------------------------------------------------------------------------------------#
  # Build PTree for benchmarking
  #--------------------------------------------------------------------------------------#
  if(${INDEX_BENCH_BUILD_PTREE})
    target_compile_options(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      -mcx16
      -DHOMEGROWN
    )
    target_include_directories(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      ${CMAKE_CURRENT_SOURCE_DIR}/external/PAM/c++/
    )
    target_compile_definitions(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      INDEX_BENCH_BUILD_PTREE
    )
  endif()

  add_test(NAME ${INDEX_BENCH_TEST_TARGET} COMMAND $<TARGET_FILE:${INDEX_BENCH_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_INDEX_BENCH_TEST("operation_generator_test")
ADD_INDEX_BENCH_TEST("worker_test")
