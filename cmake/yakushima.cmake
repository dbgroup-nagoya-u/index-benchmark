set(YAKUSHIMA_SOURCE_DIR "${PROJECT_SOURCE_DIR}/external/yakushima")

#--------------------------------------------------------------------------------------#
# Build targets
#--------------------------------------------------------------------------------------#

if(NOT TARGET tsurugi::yakushima)
  find_package(TBB REQUIRED)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(glog REQUIRED libglog)

  add_library(yakushima INTERFACE)
  add_library(tsurugi::yakushima ALIAS yakushima)
  target_compile_features(yakushima INTERFACE
    "cxx_std_17"
  )
  target_compile_definitions(yakushima INTERFACE
    YAKUSHIMA_MAX_PARALLEL_SESSIONS=${INDEX_BENCH_MAX_CORES}
  )
  target_include_directories(yakushima INTERFACE
    "${YAKUSHIMA_SOURCE_DIR}/include"
    ${glog_INCLUDE_DIRS}
  )
  target_link_libraries(yakushima INTERFACE
    TBB::tbb
    ${glog_LIBRARIES}
  )
endif()
