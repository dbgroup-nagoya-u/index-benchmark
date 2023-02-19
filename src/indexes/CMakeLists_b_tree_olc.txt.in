set(B_TREE_OLC_SOURCE_DIR "${PROJECT_SOURCE_DIR}/external/open_bwtree/BTreeOLC")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET open_bw::b_tree_olc)
  add_library(b_tree_olc INTERFACE)
  add_library(open_bw::b_tree_olc ALIAS b_tree_olc)
  target_include_directories(b_tree_olc INTERFACE
    "${B_TREE_OLC_SOURCE_DIR}"
  )
endif()
