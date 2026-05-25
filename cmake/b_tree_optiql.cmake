set(ORGANIZATION "sfu_dis")
set(COMPETITOR "b_tree_optiql")
string(TOUPPER ${COMPETITOR} COMPETITOR_FLAG)

option(
  INDEX_BENCH_BUILD_${COMPETITOR_FLAG}
  "Build ${ORGANIZATION}::${COMPETITOR}"
  OFF
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
  BTREE_PAGE_SIZE
  ${INDEX_BENCH_PAGE_SIZE}
  CACHE STRING "" FORCE
)

include(FetchContent)
FetchContent_GetProperties(${COMPETITOR})
if(NOT ${COMPETITOR}_POPULATED)
  FetchContent_Declare(
    ${COMPETITOR}
    GIT_REPOSITORY "https://github.com/sfu-dis/optiql"
    GIT_TAG "4b9e562378bb9c6ff0384eab9e629c4159849231" # latest at May 25, 2026
  )
  FetchContent_Populate(${COMPETITOR})
endif()
set(SOURCE_DIR "${${COMPETITOR}_SOURCE_DIR}/index-benchmarks")

set(
  ${COMPETITOR_FLAG}_DUMMY_FILE
  "${SOURCE_DIR}/dbgroup_dummy.txt"
)
if(NOT EXISTS "${${COMPETITOR_FLAG}_DUMMY_FILE}")
  execute_process(
    COMMAND bash "-c" "sed -i '23,34d' ${SOURCE_DIR}/indexes/BTreeOLC/BTreeOMCS.h"
  )
  message(STATUS "[${COMPETITOR}] The noisy log messages have been removed.")

  execute_process(
    COMMAND bash "-c" "touch ${${COMPETITOR_FLAG}_DUMMY_FILE}"
  )
  message(STATUS "[${COMPETITOR}] Create a dummy file to avoid multiple initializations.")
endif()

#------------------------------------------------------------------------------#
# Build targets
#------------------------------------------------------------------------------#

if(NOT TARGET ${ORGANIZATION}::${COMPETITOR})
  add_library(${COMPETITOR} INTERFACE)
  add_library(${ORGANIZATION}::${COMPETITOR} ALIAS ${COMPETITOR})
  target_include_directories(${COMPETITOR} SYSTEM INTERFACE
    "${SOURCE_DIR}"
  )
  target_compile_definitions(${COMPETITOR} INTERFACE
    OMCS_LOCK
    BTREE_PAGE_SIZE=${BTREE_PAGE_SIZE}
  )
endif()

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
