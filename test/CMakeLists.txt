#--------------------------------------------------------------------------------------#
# Configure GoogleTest
#--------------------------------------------------------------------------------------#

configure_file(CMakeLists-googletest.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)

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
    nlohmann_json::nlohmann_json
    cpp_utility
    bztree
    gtest_main
  )

  if(DEFINED INDEX_BENCH_TEST_THREAD_NUM)
    target_compile_definitions(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      INDEX_BENCH_TEST_THREAD_NUM=${INDEX_BENCH_TEST_THREAD_NUM}
    )
  endif()

  #--------------------------------------------------------------------------------------#
  # Build OpenBw-Tree for benchmarking
  #--------------------------------------------------------------------------------------#
  if(${INDEX_BENCH_BUILD_OPEN_BWTREE})
    target_compile_options(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      -mcx16
    )
    target_compile_definitions(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      INDEX_BENCH_BUILD_OPEN_BWTREE
      BWTREE_NODEBUG
    )
    target_link_libraries(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      atomic
    )
  endif()

  #--------------------------------------------------------------------------------------#
  # Build Masstree for benchmarking
  #--------------------------------------------------------------------------------------#
  if(${INDEX_BENCH_BUILD_MASSTREE})
    target_compile_definitions(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      INDEX_BENCH_BUILD_MASSTREE
    )
    if(${INDEX_BENCH_TEST_TARGET} STREQUAL "index_wrapper_test"
       OR ${INDEX_BENCH_TEST_TARGET} STREQUAL "index_wrapper_multi_thread_test")
      target_link_libraries(
        ${INDEX_BENCH_TEST_TARGET} PRIVATE
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
  endif()

  #--------------------------------------------------------------------------------------#
  # Build PTree for benchmarking
  #--------------------------------------------------------------------------------------#
  if(${INDEX_BENCH_BUILD_PTREE})
    target_compile_options(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      -mcx16
    )
    target_compile_definitions(
      ${INDEX_BENCH_TEST_TARGET} PRIVATE
      INDEX_BENCH_BUILD_PTREE
      HOMEGROWN
    )
  endif()

  add_test(NAME ${INDEX_BENCH_TEST_TARGET} COMMAND $<TARGET_FILE:${INDEX_BENCH_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
# ADD_INDEX_BENCH_TEST("operation_generator_test")
# ADD_INDEX_BENCH_TEST("index_wrapper_test")
# ADD_INDEX_BENCH_TEST("index_wrapper_multi_thread_test")
