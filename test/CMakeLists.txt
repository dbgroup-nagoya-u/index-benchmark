#--------------------------------------------------------------------------------------#
# Configure GoogleTest
#--------------------------------------------------------------------------------------#

include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY "https://github.com/google/googletest.git"
  GIT_TAG "58d77fa8070e8cec2dc1ed015d66b454c8d78850" # v1.12.1
)
FetchContent_MakeAvailable(googletest)

#--------------------------------------------------------------------------------------#
# Build Unit Tests
#--------------------------------------------------------------------------------------#

# tuning parameters for testing
set(INDEX_BENCH_TEST_THREAD_NUM "8" CACHE STRING "the maximum number of threads to perform unit tests.")

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
  )
  target_compile_definitions(${INDEX_BENCH_TEST_TARGET} PRIVATE
    INDEX_BENCH_MAX_CORES=${INDEX_BENCH_MAX_CORES}
    INDEX_BENCH_TEST_THREAD_NUM=${INDEX_BENCH_TEST_THREAD_NUM}
    $<$<BOOL:${INDEX_BENCH_BUILD_B_TREE_OLC}>:INDEX_BENCH_BUILD_B_TREE_OLC>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:INDEX_BENCH_BUILD_OPEN_BWTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_MASSTREE}>:INDEX_BENCH_BUILD_MASSTREE>
    $<$<BOOL:${INDEX_BENCH_BUILD_YAKUSHIMA}>:INDEX_BENCH_BUILD_YAKUSHIMA>
    $<$<BOOL:${INDEX_BENCH_BUILD_ART_OLC}>:INDEX_BENCH_BUILD_ART_OLC>
    $<$<BOOL:${INDEX_BENCH_BUILD_HYDRALIST}>:INDEX_BENCH_BUILD_HYDRALIST>
    $<$<BOOL:${INDEX_BENCH_BUILD_ALEX_OLC}>:INDEX_BENCH_BUILD_ALEX_OLC>
    $<$<BOOL:${INDEX_BENCH_COMPARE_WITH_SOTA}>:INDEX_BENCH_COMPARE_WITH_SOTA>
  )
  target_include_directories(${INDEX_BENCH_TEST_TARGET} PRIVATE
    "${PROJECT_SOURCE_DIR}/src"
  )
  target_link_libraries(${INDEX_BENCH_TEST_TARGET} PRIVATE
    GTest::gtest_main
    gflags
    nlohmann_json::nlohmann_json
    cpp_utility::cpp_utility
    cpp_bench::cpp_bench
    b_tree::b_tree
    bw_tree::bw_tree
    bztree::bztree
    $<$<BOOL:${INDEX_BENCH_OVERRIDE_MIMALLOC}>:mimalloc-static>
    $<$<BOOL:${INDEX_BENCH_BUILD_B_TREE_OLC}>:open_bw::b_tree_olc>
    $<$<BOOL:${INDEX_BENCH_BUILD_OPEN_BWTREE}>:open_bw::bw_tree>
    $<$<BOOL:${INDEX_BENCH_BUILD_MASSTREE}>:masstree::masstree>
    $<$<BOOL:${INDEX_BENCH_BUILD_YAKUSHIMA}>:tsurugi::yakushima>
    $<$<BOOL:${INDEX_BENCH_BUILD_ART_OLC}>:open_bw::art_olc>
    $<$<BOOL:${INDEX_BENCH_BUILD_HYDRALIST}>:hydralist::hydralist>
    $<$<BOOL:${INDEX_BENCH_BUILD_ALEX_OLC}>:GRE::alex_olc>
  )

  add_test(NAME ${INDEX_BENCH_TEST_TARGET} COMMAND $<TARGET_FILE:${INDEX_BENCH_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_INDEX_BENCH_TEST("key_test")
ADD_INDEX_BENCH_TEST("workload_test")
ADD_INDEX_BENCH_TEST("operation_engine_test")
# ADD_INDEX_BENCH_TEST("index_wrapper_test")
# ADD_INDEX_BENCH_TEST("index_wrapper_multi_thread_test")
