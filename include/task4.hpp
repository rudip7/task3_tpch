
#pragma once

#include <tables.hpp>
#include <query_tests.hpp>

namespace task4{

  void tpch_query1(const columnstore::lineitem& l);

  void tpch_query5(const columnstore::lineitem& l,
                   const columnstore::orders& o,
                   const columnstore::partsupplier& ps,
                   const columnstore::part p,
                   const columnstore::supplier s,
                   const columnstore::customer c,
                   const columnstore::nation n,
                   const columnstore::region r);

  void tpch_query6(const columnstore::lineitem& l);

}
