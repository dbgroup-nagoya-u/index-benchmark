message(NOTICE "[b_optiql] Prepare B+tree with optimistic lock coupling.")
#--------------------------------------------------------------------------------------#
# Configure B+tree with OLC
#--------------------------------------------------------------------------------------#

include(FetchContent)
FetchContent_GetProperties(b_optiql)
if(NOT b_optiql_POPULATED)
  FetchContent_Declare(
    b_optiql
    GIT_REPOSITORY "https://github.com/sfu-dis/optiql.git"
    GIT_TAG "2d9a846a1537b9b00b1e66f4cade37083c1753d5" # latest at Sept. 26, 2023
  )
  FetchContent_Populate(b_optiql)
endif()
set(B_TREE_OPTIQL_SOURCE_DIR "${b_optiql_SOURCE_DIR}/index-benchmarks")

set(
  B_TREE_OPTIQL_DUMMY_FILE
  "${B_TREE_OPTIQL_SOURCE_DIR}/dbgroup_dummy.txt"
)
if(NOT EXISTS "${B_TREE_OPTIQL_DUMMY_FILE}")
  execute_process(
    COMMAND bash "-c" "sed -i '23,34d' ${B_TREE_OPTIQL_SOURCE_DIR}/indexes/BTreeOLC/BTreeOMCS.h"
  )
  message(NOTICE "[b_optiql] The noisy log messages have been removed.")

  execute_process(
    COMMAND bash "-c" "touch ${B_TREE_OPTIQL_DUMMY_FILE}"
  )
  message(NOTICE "[b_optiql] Create a dummy file to avoid multiple initializations.")
endif()

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET b_tree_optiql::b_tree_optiql)
  add_library(b_tree_optiql INTERFACE)
  add_library(b_tree_optiql::b_tree_optiql ALIAS b_tree_optiql)
  target_include_directories(b_tree_optiql INTERFACE
    "${B_TREE_OPTIQL_SOURCE_DIR}"
  )
  target_compile_definitions(b_tree_optiql INTERFACE
    OMCS_LOCK
    BTREE_PAGE_SIZE=256
  )
endif()

message(NOTICE "[b_optiql] Preparation completed.")
