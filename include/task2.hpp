
#pragma once

#include <tables.hpp>

namespace task1{

  void aggregation_query_linitem(const rowstore::lineitem& l);

  void aggregation_query_linitem(const columnstore::lineitem& l);

  void tpch_query6_linitem(const rowstore::lineitem& l);

  void tpch_query6_linitem(const columnstore::lineitem& l);

}
