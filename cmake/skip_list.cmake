message(NOTICE "[skip_list] Prepare skip list.")

include(FetchContent)
FetchContent_Declare(
  skip_list
  GIT_REPOSITORY "https://github.com/dbgroup-nagoya-u/skip-list.git"
  GIT_TAG "97981304b07dbbc56660923e06848675c34d8816" # latest at Oct. 9, 2023
)
FetchContent_MakeAvailable(skip_list)

message(NOTICE "[skip_list] Preparation completed.")
