#include "../external/PAM/c++/pbbslib/utilities.h"
#include "../external/PAM/c++/pam.h"
#include "../external/PAM/tpch/readCSV.h"
#include "../external/PAM/tpch/lineitem.h"
#include "../external/PAM/tpch/utils.h"

template <class Val>
struct ptree_entry {
  using key_t = uint64_t;
  using val_t = Val;
  static bool comp(const key_t& a, const key_t& b) { return a < b;}
};