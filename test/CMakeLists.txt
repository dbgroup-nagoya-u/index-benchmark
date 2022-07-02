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
  add_executable(${INDEX_BENCH_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${INDEX_BENCH_TEST_TARGET}.cpp"
  )
  target_compile_features(${INDEX_BENCH_TEST_TARGET} PRIVATE
    "cxx_std_17"
  )
  target_compile_options(${INDEX_BENCH_TEST_TARGET} PRIVATE
    -Wall
    -Wextra
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
    $<$<OR:$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>,$<BOOL:${INDEX_BENCH_BUILD_PTREE}>>:-mcx16>
  )
  target_include_directories(${INDEX_BENCH_TEST_TARGET} PRIVATE
    "${INDEX_BENCH_SOURCE_DIR}/src"
    "${INDEX_BENCH_SOURCE_DIR}/external"
    $<$<BOOL:${INDEX_BENCH_OVERRIDE_JEMALLOC}>:${JEMALLOC_INCLUDE_DIRS}>
    $<$<BOOL:${INDEX_BENCH_BUILD_PTREE}>:${INDEX_BENCH_SOURCE_DIR}/external/PAM/c++/>
  )
  target_link_libraries(${INDEX_BENCH_TEST_TARGET} PRIVATE
    gtest_main
    gflags
    TBB::tbb
    nlohmann_json::nlohmann_json
    cpp_utility::cpp_utility
    cpp_bench::cpp_bench
    bw_tree::bw_tree
    bztree::bztree
    $<$<BOOL:${INDEX_BENCH_OVERRIDE_JEMALLOC}>:PkgConfig::JEMALLOC>
    $<$<BOOL:${INDEX_BENCH_OVERRIDE_MIMALLOC}>:mimalloc-static>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:atomic>
  )
  target_compile_definitions(${INDEX_BENCH_TEST_TARGET} PRIVATE
    $<$<BOOL:${INDEX_BENCH_BUILD_BTREE_OLC}>:INDEX_BENCH_BUILD_BTREE_OLC>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:INDEX_BENCH_BUILD_OPEN_BWTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:INDEX_BENCH_BUILD_OPEN_BWTREE>
    $<$<AND:$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>,$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">>:BWTREE_NODEBUG>
    $<$<BOOL:${INDEX_BENCH_BUILD_MASSTREE}>:INDEX_BENCH_BUILD_MASSTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_PTREE}>:INDEX_BENCH_BUILD_PTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_PTREE}>:HOMEGROWN>
  )

  if(${INDEX_BENCH_BUILD_MASSTREE})
    # target_link_libraries(${INDEX_BENCH_TEST_TARGET} PRIVATE
    #   ${INDEX_BENCH_MASSTREE_LIBS}
    # )
  endif()

  add_test(NAME ${INDEX_BENCH_TEST_TARGET} COMMAND $<TARGET_FILE:${INDEX_BENCH_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_INDEX_BENCH_TEST("key_test")
ADD_INDEX_BENCH_TEST("workload_test")
ADD_INDEX_BENCH_TEST("operation_engine_test")
# ADD_INDEX_BENCH_TEST("index_wrapper_test")
# ADD_INDEX_BENCH_TEST("index_wrapper_multi_thread_test")
