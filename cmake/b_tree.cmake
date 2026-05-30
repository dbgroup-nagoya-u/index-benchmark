set(ORGANIZATION "dbgroup")
set(COMPETITOR "b_tree")
string(TOUPPER ${COMPETITOR} COMPETITOR_FLAG)

option(
  INDEX_BENCH_BUILD_${COMPETITOR_FLAG}
  "Build ${ORGANIZATION}::${COMPETITOR}"
  ON
)
if(NOT ${INDEX_BENCH_BUILD_${COMPETITOR_FLAG}})
  message(STATUS "[${COMPETITOR}] Ignore ${ORGANIZATION}::${COMPETITOR}.")
  return()
endif()

#------------------------------------------------------------------------------#
# Configuration
#------------------------------------------------------------------------------#
message(STATUS "[${COMPETITOR}] Prepare ${ORGANIZATION}::${COMPETITOR}.")

set(
  B_TREE_DEFAULT_PAGE_SIZE
  ${INDEX_BENCH_PAGE_SIZE}
  CACHE STRING "" FORCE
)
set(
  B_TREE_MAX_VARLEN_DATA_SIZE
  ${INDEX_BENCH_MAX_VARLEN_DATA_SIZE}
  CACHE STRING "" FORCE
)

include(FetchContent)
FetchContent_Declare(
  ${COMPETITOR}
  GIT_REPOSITORY "https://github.com/dbgroup-nagoya-u/b-tree"
  GIT_TAG "659018f4c07c922d66aeedf894dc19e282e717f6"
)
FetchContent_MakeAvailable(${COMPETITOR})

#------------------------------------------------------------------------------#
# Add competitor to benchmark
#------------------------------------------------------------------------------#

target_compile_definitions(${PROJECT_NAME} PUBLIC
  INDEX_BENCH_BUILD_${COMPETITOR_FLAG}
)
target_link_libraries(${PROJECT_NAME} PUBLIC
  ${ORGANIZATION}::${COMPETITOR}
)

message(STATUS "[${COMPETITOR}] Preparation completed.")
