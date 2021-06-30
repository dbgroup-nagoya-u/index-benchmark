#include "../external/PAM/c++/pbbslib/utilities.h"
#include "../external/PAM/c++/pam.h"
#include "../external/PAM/tpch/readCSV.h"
#include "../external/PAM/tpch/lineitem.h"
#include "../external/PAM/tpch/utils.h"

template <class Key, class Value>
struct ptree_entry {
  using key_t = Key;
  using val_t = Value;
  static_assert(!std::is_pointer<Key>::value);
  static_assert(std::is_scalar<Key>::value);

  static bool comp(const Key& a, const Key& b) { return a < b;}
};
