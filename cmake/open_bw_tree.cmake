message(NOTICE "[open_bw] Prepare OpenBw-tree.")
#--------------------------------------------------------------------------------------#
# Configure OpenBw-tree
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
set(OPEN_BW_TREE_SOURCE_DIR "${open_bw_SOURCE_DIR}")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET open_bw::bw_tree)
  find_package(Threads)
  add_library(open_bw_tree STATIC
    "${OPEN_BW_TREE_SOURCE_DIR}/BwTree/bwtree.cpp"
  )
  add_library(open_bw::bw_tree ALIAS open_bw_tree)
  target_compile_features(open_bw_tree PRIVATE
    "cxx_std_14"
  )
  target_compile_options(open_bw_tree PRIVATE
    -mcx16
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:"-O2 -march=native">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"RelWithDebInfo">:"-g3 -Og -pg">
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Debug">:"-g3 -O0 -pg">
  )
  target_compile_definitions(open_bw_tree PUBLIC
    $<$<STREQUAL:${CMAKE_BUILD_TYPE},"Release">:BWTREE_NODEBUG>
  )
  target_include_directories(open_bw_tree PUBLIC
    "${OPEN_BW_TREE_SOURCE_DIR}"
  )
  target_link_libraries(open_bw_tree PUBLIC
    Threads::Threads
    atomic
  )
endif()

message(NOTICE "[open_bw] Preparation completed.")
