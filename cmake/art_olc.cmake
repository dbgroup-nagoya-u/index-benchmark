set(ORGANIZATION "microbench")
set(COMPETITOR "art_olc")
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

include(FetchContent)
FetchContent_GetProperties(${ORGANIZATION})
if(NOT ${ORGANIZATION}_POPULATED)
  FetchContent_Declare(
    ${ORGANIZATION}
    GIT_REPOSITORY "https://github.com/wangziqi2016/index-microbench.git"
    GIT_TAG "74cafa57d74798f209d8fcbce8c4f317ce066eae" # latest at May 25, 2026
  )
  FetchContent_Populate(${ORGANIZATION})
endif()
set(SOURCE_DIR "${${ORGANIZATION}_SOURCE_DIR}/ARTOLC")

execute_process(
  COMMAND bash "-c" "grep 'functional' ${SOURCE_DIR}/Tree.cpp &> /dev/null"
  RESULT_VARIABLE ${COMPETITOR_FLAG}_NEED_FIX
)
if(${${COMPETITOR_FLAG}_NEED_FIX})
  execute_process(
    COMMAND bash "-c" "sed -i '3i #include <functional>' ${SOURCE_DIR}/Tree.cpp"
  )
  message(STATUS "[${COMPETITOR}] The <functional> library has been added.")
endif()

#------------------------------------------------------------------------------#
# Build targets
#------------------------------------------------------------------------------#

if(NOT TARGET ${ORGANIZATION}::${COMPETITOR})
  find_package(TBB REQUIRED)

  add_library(${COMPETITOR} STATIC
    "${SOURCE_DIR}/Epoche.cpp"
    "${SOURCE_DIR}/Tree.cpp"
  )
  add_library(${ORGANIZATION}::${COMPETITOR} ALIAS ${COMPETITOR})
  target_compile_features(${COMPETITOR} PRIVATE
    "cxx_std_14"
  )
  target_compile_options(${COMPETITOR} PRIVATE
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Release">:-march=native>
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Debug">:-g3>
  )
  target_include_directories(${COMPETITOR} PUBLIC
    "${SOURCE_DIR}"
  )
  target_link_libraries(${COMPETITOR} PUBLIC
    TBB::tbb
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
