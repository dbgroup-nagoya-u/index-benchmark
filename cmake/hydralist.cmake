message(NOTICE "[hydralist] Prepare HydraList.")
#--------------------------------------------------------------------------------------#
# Configure HydraList
#--------------------------------------------------------------------------------------#

include(FetchContent)
FetchContent_Declare(
  hydralist
  GIT_REPOSITORY "https://github.com/cosmoss-jigu/hydralist.git"
  GIT_TAG "989b1dd73d225e51c7cde4bfd83f4f1631e3700b" # latest at May 31, 2023
)
FetchContent_Populate(hydralist)
set(HYDRALIST_SOURCE_DIR "${hydralist_SOURCE_DIR}/hydralist")
set(HYDRALIST_ART_SOURCE_DIR "${HYDRALIST_SOURCE_DIR}/lib/ARTROWEX")

set(
  HYDRALIST_NUMA_CONFIG_PATH
  "${HYDRALIST_SOURCE_DIR}/include/numa-config.h"
)
if(NOT EXISTS "${HYDRALIST_NUMA_CONFIG_PATH}")
  execute_process(
    COMMAND python3 "${HYDRALIST_SOURCE_DIR}/tools/cpu-topology.py"
    OUTPUT_FILE "${HYDRALIST_NUMA_CONFIG_PATH}"
  )
  message(NOTICE "[hydralist] The CPUs' NUMA settings have been collected.")

  execute_process(
    COMMAND bash "-c" "sed -i '134 s/d/d, c/' ${HYDRALIST_SOURCE_DIR}/include/arch.h"
    COMMAND bash "-c" "sed -i '135 s/(d)/(d), \"=c\"(c)/' ${HYDRALIST_SOURCE_DIR}/include/arch.h"
  )
  message(NOTICE "[hydralist] The codes for the rdtscp instruction have been fixed.")

  execute_process(
    COMMAND bash "-c" "sed -i '/printf/d' ${HYDRALIST_SOURCE_DIR}/src/HydraList.cpp"
    COMMAND bash "-c" "sed -i '/std::cout/d' ${HYDRALIST_SOURCE_DIR}/src/HydraList.cpp"
  )
  message(NOTICE "[hydralist] The noisy log messages have been removed.")
endif()

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET hydralist::hydralist)
  find_package(TBB REQUIRED)

  add_library(hydralist STATIC
    "${HYDRALIST_SOURCE_DIR}/src/HydraList.cpp"
    "${HYDRALIST_SOURCE_DIR}/src/Oplog.cpp"
    "${HYDRALIST_SOURCE_DIR}/src/WorkerThread.cpp"
    "${HYDRALIST_SOURCE_DIR}/src/linkedList.cpp"
    "${HYDRALIST_SOURCE_DIR}/src/listNode.cpp"
    "${HYDRALIST_ART_SOURCE_DIR}/Tree.cpp"
  )
  add_library(hydralist::hydralist ALIAS hydralist)
  target_compile_features(hydralist PRIVATE
    "cxx_std_14"
  )
  target_compile_options(hydralist PRIVATE
    -mavx512f
    -mavx512bw
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_include_directories(hydralist PUBLIC
    "${HYDRALIST_SOURCE_DIR}/src"
    "${HYDRALIST_SOURCE_DIR}/include"
  )
  target_include_directories(hydralist PRIVATE
    "${HYDRALIST_ART_SOURCE_DIR}"
  )
  target_link_libraries(hydralist PUBLIC
    numa
    TBB::tbb
  )
endif()

message(NOTICE "[hydralist] Preparation completed.")
