set(ORGANIZATION "tsurugi")
set(COMPETITOR "yakushima")
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
FetchContent_Declare(
  ${COMPETITOR}
  GIT_REPOSITORY "https://github.com/project-tsurugi/yakushima"
  GIT_TAG "c584ca806f9d440ff895887916438c00064dc6e5" # latest at Mar 25, 2026
)
FetchContent_Populate(${COMPETITOR})

#------------------------------------------------------------------------------#
# Build targets
#------------------------------------------------------------------------------#

if(NOT TARGET ${ORGANIZATION}::${COMPETITOR})
  find_package(TBB REQUIRED)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(glog REQUIRED libglog)

  add_library(${COMPETITOR} INTERFACE)
  add_library(${ORGANIZATION}::${COMPETITOR} ALIAS ${COMPETITOR})
  target_compile_features(${COMPETITOR} INTERFACE
    "cxx_std_20"
  )
  target_compile_definitions(${COMPETITOR} INTERFACE
    YAKUSHIMA_MAX_PARALLEL_SESSIONS=${INDEX_BENCH_MAX_WORKER_NUM}
  )
  target_compile_options(${COMPETITOR} INTERFACE
    -fsized-deallocation
  )
  target_include_directories(${COMPETITOR} INTERFACE
    "${yakushima_SOURCE_DIR}/include"
    ${glog_INCLUDE_DIRS}
  )
  target_link_libraries(${COMPETITOR} INTERFACE
    TBB::tbb
    ${glog_LIBRARIES}
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
