#------------------------------------------------------------------------------#
# Configure GoogleTest
#------------------------------------------------------------------------------#

include(FetchContent)

# disable tests for not implemented APIs
set(DBGROUP_TEST_DISABLE_SCAN_TEST ON CACHE BOOL "" FORCE)
set(DBGROUP_TEST_DISABLE_SCAN_BACKWARD_TEST ON CACHE BOOL "" FORCE)
set(DBGROUP_TEST_DISABLE_UPSERT_TEST ON CACHE BOOL "" FORCE)
set(DBGROUP_TEST_DISABLE_INSERT_TEST ON CACHE BOOL "" FORCE)
set(DBGROUP_TEST_DISABLE_UPDATE_TEST ON CACHE BOOL "" FORCE)
set(DBGROUP_TEST_DISABLE_DELETE_TEST ON CACHE BOOL "" FORCE)
set(DBGROUP_TEST_DISABLE_BULKLOAD_TEST ON CACHE BOOL "" FORCE)
set(DBGROUP_TEST_DISABLE_SCAN_VERIFIER_TEST ON CACHE BOOL "" FORCE)
set(DBGROUP_TEST_DISABLE_RECORD_MERGER ON CACHE BOOL "" FORCE)

FetchContent_Declare(
  index_fixtures
  GIT_REPOSITORY "https://github.com/dbgroup-nagoya-u/index-fixtures"
  GIT_TAG "c090e574caa034c65e8b4e53374501b61a7c0066"
)
FetchContent_MakeAvailable(index_fixtures)

#------------------------------------------------------------------------------#
# Build Unit Tests
#------------------------------------------------------------------------------#

# tuning parameters for testing
set(INDEX_BENCH_TEST_THREAD_NUM "8" CACHE STRING "the maximum number of threads to perform unit tests.")

# define function to add unit tests in the same format
function(ADD_INDEX_BENCH_TEST INDEX_BENCH_TEST_TARGET)
  add_executable(${INDEX_BENCH_TEST_TARGET}
    "${CMAKE_CURRENT_SOURCE_DIR}/${INDEX_BENCH_TEST_TARGET}.cpp"
  )
  target_compile_features(${INDEX_BENCH_TEST_TARGET} PRIVATE
    "cxx_std_20"
  )
  target_compile_options(${INDEX_BENCH_TEST_TARGET} PRIVATE
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Release">:-march=native>
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Debug">:-g3>
    -Wall
    -Wextra
  )
  target_include_directories(${INDEX_BENCH_TEST_TARGET} PRIVATE
    "${PROJECT_SOURCE_DIR}/src"
  )
  target_link_libraries(${INDEX_BENCH_TEST_TARGET} PRIVATE
    dbgroup::${PROJECT_NAME}
    dbgroup::index_fixtures
  )

  add_test(NAME ${INDEX_BENCH_TEST_TARGET} COMMAND $<TARGET_FILE:${INDEX_BENCH_TEST_TARGET}>)
endfunction()

# add unit tests to build targets
ADD_INDEX_BENCH_TEST("var_len_data_test")
# ADD_INDEX_BENCH_TEST("workload_test")
# ADD_INDEX_BENCH_TEST("operation_engine_test")

# add unit tests for wrappers
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/wrappers)
