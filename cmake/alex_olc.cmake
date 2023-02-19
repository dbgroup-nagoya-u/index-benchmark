set(ALEX_OLC_SOURCE_DIR "${PROJECT_SOURCE_DIR}/external/GRE/src/competitor/alexol")

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
