set(MASSTREE_SOURCE_DIR "${PROJECT_SOURCE_DIR}/external/masstree")

#--------------------------------------------------------------------------------------#
# Configurations
#--------------------------------------------------------------------------------------#

if(${INDEX_BENCH_BUILD_MASSTREE} AND NOT EXISTS "${MASSTREE_SOURCE_DIR}/config.h")
  execute_process(
    COMMAND ./bootstrap.sh
    WORKING_DIRECTORY ${MASSTREE_SOURCE_DIR}
  )
  message(NOTICE "[masstree] Bootstrap has finished.")

  execute_process(
    COMMAND ./configure --disable-assertions
    WORKING_DIRECTORY ${MASSTREE_SOURCE_DIR}
  )
  message(NOTICE "[masstree] Configuration has finished.")

  execute_process(
    COMMAND bash "-c" "sed -i '18i #include \"config.h\"' ${MASSTREE_SOURCE_DIR}/compiler.hh"
  )
  message(NOTICE "[masstree] The header config.h has been added.")
endif()

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET masstree::masstree)
  add_library(masstree STATIC
    "${MASSTREE_SOURCE_DIR}/checkpoint.cc"
    "${MASSTREE_SOURCE_DIR}/clp.c"
    "${MASSTREE_SOURCE_DIR}/compiler.cc"
    "${MASSTREE_SOURCE_DIR}/file.cc"
    "${MASSTREE_SOURCE_DIR}/json.cc"
    "${MASSTREE_SOURCE_DIR}/kvio.cc"
    "${MASSTREE_SOURCE_DIR}/kvrandom.cc"
    "${MASSTREE_SOURCE_DIR}/kvthread.cc"
    "${MASSTREE_SOURCE_DIR}/log.cc"
    "${MASSTREE_SOURCE_DIR}/memdebug.cc"
    "${MASSTREE_SOURCE_DIR}/misc.cc"
    "${MASSTREE_SOURCE_DIR}/msgpack.cc"
    "${MASSTREE_SOURCE_DIR}/str.cc"
    "${MASSTREE_SOURCE_DIR}/straccum.cc"
    "${MASSTREE_SOURCE_DIR}/string.cc"
    "${MASSTREE_SOURCE_DIR}/string_slice.cc"
    "${MASSTREE_SOURCE_DIR}/query_masstree.cc"
    "${MASSTREE_SOURCE_DIR}/value_array.cc"
    "${MASSTREE_SOURCE_DIR}/value_string.cc"
    "${MASSTREE_SOURCE_DIR}/value_versioned_array.cc"
  )
  add_library(masstree::masstree ALIAS masstree)
  target_compile_options(masstree PRIVATE
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_include_directories(masstree PUBLIC
    "${MASSTREE_SOURCE_DIR}"
  )
endif()
