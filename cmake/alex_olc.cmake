message(NOTICE "[alex_olc] Prepare ALEX+.")
#--------------------------------------------------------------------------------------#
# Configure ALEX+
#--------------------------------------------------------------------------------------#

include(FetchContent)
FetchContent_Declare(
  alex
  GIT_REPOSITORY "https://github.com/gre4index/GRE.git"
  GIT_TAG "e807edcef51df6732f07f94d4c797fb3897519ba" # latest at May 31, 2023
)
FetchContent_Populate(alex)
set(ALEX_OLC_SOURCE_DIR "${alex_SOURCE_DIR}/src/competitor/alexol")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET GRE::alex_olc)
  find_package(TBB REQUIRED)

  add_library(alex_olc INTERFACE)
  add_library(GRE::alex_olc ALIAS alex_olc)
  target_compile_options(alex_olc INTERFACE
    -march=native
  )
  target_include_directories(alex_olc INTERFACE
    "${ALEX_OLC_SOURCE_DIR}"
  )
  target_link_libraries(alex_olc INTERFACE
    TBB::tbb
  )
endif()

message(NOTICE "[alex_olc] Preparation completed.")
