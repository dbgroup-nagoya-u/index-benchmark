set(OPEN_BW_TREE_SOURCE_DIR "${PROJECT_SOURCE_DIR}/external/open_bwtree/BwTree")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET open_bw::bw_tree)
  add_library(open_bw_tree STATIC
    "${OPEN_BW_TREE_SOURCE_DIR}/bwtree.cpp"
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
  target_compile_definitions(open_bw_tree PRIVATE
    BWTREE_NODEBUG
  )
  target_include_directories(open_bw_tree PUBLIC
    "${OPEN_BW_TREE_SOURCE_DIR}"
  )
  target_link_libraries(open_bw_tree PRIVATE
    pthread
    atomic
  )
endif()
