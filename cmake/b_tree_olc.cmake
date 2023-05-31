message(NOTICE "[b_olc] Prepare B+tree with optimistic lock coupling.")
#--------------------------------------------------------------------------------------#
# Configure B+tree with OLC
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
set(B_TREE_OLC_SOURCE_DIR "${open_bw_SOURCE_DIR}/BTreeOLC")

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

message(NOTICE "[b_olc] Preparation completed.")
