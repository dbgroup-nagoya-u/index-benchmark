message(NOTICE "[art_olc] Prepare ART with optimistic lock coupling.")
#--------------------------------------------------------------------------------------#
# Configure ART with OLC
#--------------------------------------------------------------------------------------#

include(FetchContent)
FetchContent_GetProperties(open_bw)
if(NOT open_bw_POPULATED)
  FetchContent_Declare(
    open_bw
    GIT_REPOSITORY "https://github.com/wangziqi2016/index-microbench.git"
    GIT_TAG "74cafa57d74798f209d8fcbce8c4f317ce066eae" # latest at May 31, 2023
  )
  FetchContent_Populate(open_bw)
endif()
set(ART_OLC_SOURCE_DIR "${open_bw_SOURCE_DIR}/ARTOLC")

execute_process(
  COMMAND bash "-c" "grep 'functional' ${ART_OLC_SOURCE_DIR}/Tree.cpp &> /dev/null"
  RESULT_VARIABLE ART_OLC_NEED_FIX
)
if(${ART_OLC_NEED_FIX})
  execute_process(
    COMMAND bash "-c" "sed -i '3i #include <functional>' ${ART_OLC_SOURCE_DIR}/Tree.cpp"
  )
  message(NOTICE "[art_olc] The <functional> library has been added.")
endif()

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET open_bw::art_olc)
  find_package(TBB REQUIRED)

  add_library(art_olc STATIC
    "${ART_OLC_SOURCE_DIR}/Epoche.cpp"
    "${ART_OLC_SOURCE_DIR}/Tree.cpp"
  )
  add_library(open_bw::art_olc ALIAS art_olc)
  target_compile_features(art_olc PRIVATE
    "cxx_std_14"
  )
  target_compile_options(art_olc PRIVATE
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_include_directories(art_olc PUBLIC
    "${ART_OLC_SOURCE_DIR}"
  )
  target_link_libraries(art_olc PUBLIC
    TBB::tbb
  )
endif()

message(NOTICE "[art_olc] Preparation completed.")
