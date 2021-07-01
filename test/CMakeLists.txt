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
  add_executable(${INDEX_BENCH_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${INDEX_BENCH_TEST_TARGET}.cpp"
  )
  target_compile_features(${INDEX_BENCH_TEST_TARGET} PRIVATE "cxx_std_17")
  target_compile_options(${INDEX_BENCH_TEST_TARGET} PRIVATE
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
    gtest_main
    cpp_utility
    bztree
    atomic
  )
  add_test(NAME ${INDEX_BENCH_TEST_TARGET} COMMAND $<TARGET_FILE:${INDEX_BENCH_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_INDEX_BENCH_TEST("operation_generator_test")
ADD_INDEX_BENCH_TEST("worker_open_bwtree_test")
