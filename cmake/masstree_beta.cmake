set(ORGANIZATION "kohler")
set(COMPETITOR "masstree_beta")
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
  GIT_REPOSITORY "https://github.com/kohler/masstree-beta"
  GIT_TAG "11198427a1170654ca646dd20d96c8f349bca2bd" # latest at May 25, 2026
)
FetchContent_Populate(${COMPETITOR})
set(SOURCE_DIR "${${COMPETITOR}_SOURCE_DIR}")

if(NOT EXISTS "${SOURCE_DIR}/config.h")
  execute_process(
    COMMAND ./bootstrap.sh
    WORKING_DIRECTORY ${SOURCE_DIR}
  )
  message(STATUS "[${COMPETITOR}] Bootstrap has finished.")

  execute_process(
    COMMAND ./configure --disable-assertions
    WORKING_DIRECTORY ${SOURCE_DIR}
  )
  message(STATUS "[${COMPETITOR}] Configuration has finished.")

  execute_process(
    COMMAND bash "-c" "sed -i 's/#define CACHE_LINE_SIZE \\([0-9]*\\)/constexpr unsigned long CACHE_LINE_SIZE = \\1;/' ${SOURCE_DIR}/config.h"
  )
  message(STATUS "[${COMPETITOR}] Use `constexpr` for `CACHE_LINE_SIZE`.")

  execute_process(
    COMMAND bash "-c" "sed -i '18i #include \"config.h\"' ${SOURCE_DIR}/compiler.hh"
  )
  message(STATUS "[${COMPETITOR}] The header config.h has been added.")
endif()

#------------------------------------------------------------------------------#
# Build targets
#------------------------------------------------------------------------------#

if(NOT TARGET ${ORGANIZATION}::${COMPETITOR})
  add_library(${COMPETITOR} STATIC # cSpell:disable
    "${SOURCE_DIR}/checkpoint.cc"
    "${SOURCE_DIR}/clp.c"
    "${SOURCE_DIR}/compiler.cc"
    "${SOURCE_DIR}/file.cc"
    "${SOURCE_DIR}/json.cc"
    "${SOURCE_DIR}/kvio.cc"
    "${SOURCE_DIR}/kvrandom.cc"
    "${SOURCE_DIR}/kvthread.cc"
    "${SOURCE_DIR}/log.cc"
    "${SOURCE_DIR}/memdebug.cc"
    "${SOURCE_DIR}/misc.cc"
    "${SOURCE_DIR}/msgpack.cc"
    "${SOURCE_DIR}/str.cc"
    "${SOURCE_DIR}/straccum.cc"
    "${SOURCE_DIR}/string.cc"
    "${SOURCE_DIR}/string_slice.cc"
    "${SOURCE_DIR}/query_masstree.cc"
    "${SOURCE_DIR}/value_array.cc"
    "${SOURCE_DIR}/value_string.cc"
    "${SOURCE_DIR}/value_versioned_array.cc"
  ) # cSpell:enable
  add_library(${ORGANIZATION}::${COMPETITOR} ALIAS ${COMPETITOR})
  target_compile_options(${COMPETITOR} PRIVATE
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Release">:-march=native>
    $<$<STREQUAL:"${CMAKE_BUILD_TYPE}","Debug">:-g3>
    -w # disable warnings
  )
  target_include_directories(${COMPETITOR} SYSTEM PUBLIC
    "${SOURCE_DIR}"
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
