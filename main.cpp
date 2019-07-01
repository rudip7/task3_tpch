/*
Copyright 2017 Sebastian Breß, German Research Center for Artificial
Intelligence (DFKI GmbH), Technical University of Berlin

MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>

#include "queries.hpp"
#include "tables.hpp"
#include <query_tests.hpp>
#include <task2.hpp>
#include <task4.hpp>

//#define TASK2
#define TASK4

namespace rowstore {

  int perform_queries(const std::string& path) {
    rowstore::lineitem l;
    rowstore::orders o;
    rowstore::part p;
    rowstore::supplier s;
    rowstore::customer c;
    rowstore::nation n;
    rowstore::region r;
    rowstore::partsupplier ps;

    if (!rowstore::loadDataFromFile(path + "/lineitem.tbl", l)) {
      return -1;
    }
    if (!rowstore::loadDataFromFile(path + "/orders.tbl", o)) {
      return -1;
    }
    if (!rowstore::loadDataFromFile(path + "/part.tbl", p)) {
      return -1;
    }
    if (!rowstore::loadDataFromFile(path + "/supplier.tbl", s)) {
      return -1;
    }
    if (!rowstore::loadDataFromFile(path + "/customer.tbl", c)) {
      return -1;
    }
    if (!rowstore::loadDataFromFile(path + "/nation.tbl", n)) {
      return -1;
    }
    if (!rowstore::loadDataFromFile(path + "/region.tbl", r)) {
      return -1;
    }
    if (!rowstore::loadDataFromFile(path + "/partsupp.tbl", ps)) {
      return -1;
    }

    size_t num_repetitions = 10;
#ifdef TASK2
    for (size_t i = 0; i < num_repetitions; ++i) task1::aggregation_query_linitem(l);

    for (size_t i = 0; i < num_repetitions; ++i) task1::tpch_query6_linitem(l);
#endif
  }
}

namespace columnstore {

  int perform_queries(const std::string& path) {
    columnstore::lineitem l;
    columnstore::orders o;
    columnstore::part p;
    columnstore::supplier s;
    columnstore::customer c;
    columnstore::nation n;
    columnstore::region r;
    columnstore::partsupplier ps;

    Timestamp begin = getTimestamp();
    if (!columnstore::loadDataFromFile(path + "/lineitem.tbl", l)) {
      return -1;
    }
    if (!columnstore::loadDataFromFile(path + "/orders.tbl", o)) {
      return -1;
    }
    if (!columnstore::loadDataFromFile(path + "/part.tbl", p)) {
      return -1;
    }
    if (!columnstore::loadDataFromFile(path + "/supplier.tbl", s)) {
      return -1;
    }
    if (!columnstore::loadDataFromFile(path + "/customer.tbl", c)) {
      return -1;
    }
    if (!columnstore::loadDataFromFile(path + "/nation.tbl", n)) {
      return -1;
    }
    if (!columnstore::loadDataFromFile(path + "/region.tbl", r)) {
      return -1;
    }
    if (!columnstore::loadDataFromFile(path + "/partsupp.tbl", ps)) {
      return -1;
    }

    Timestamp end = getTimestamp();
    //printTable(l);
    std::cout << "Import Time: " << double(end - begin) / (1000 * 1000 * 1000)
              << "s" << std::endl;
    std::cout << "Number of Rows: " << l.size() << std::endl;
    std::cout << "Size of Table: "
              << double(sizeof(rowstore::lineitem::record) * l.size()) /
                     (1024 * 1024 * 1024)
              << "GB" << std::endl;
    size_t num_repetitions = 10;

/*#ifdef TASK2
    for (size_t i = 0; i < num_repetitions; ++i) task1::aggregation_query_linitem(l);

    for (size_t i = 0; i < num_repetitions; ++i) task1::tpch_query6_linitem(l);
#endif*/

#ifdef TASK4
    for (size_t i = 0; i < num_repetitions; ++i) task4::tpch_query1(l);

    //for (size_t i = 0; i < num_repetitions; ++i) task4::tpch_query5(l, o, ps, p, s, c, n, r);

    for (size_t i = 0; i < num_repetitions; ++i) task4::tpch_query6(l);
#endif

  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Missing paramter!" << std::endl;
    std::cerr << "Usage: " << argv[0]
              << " <path to directory containing .tbl files>" << std::endl;
    return -1;
  }
  if (!fs::exists(argv[1])) {
    std::cerr << "Did not find file '" << argv[1] << "'" << std::endl;
    return -1;
  }
  /*std::cout << std::string(80, '#') << std::endl;
  std::cout << "Using Row Store..." << std::endl;
  if (rowstore::perform_queries(argv[1])) return -1;*/

  std::cout << std::string(80, '#') << std::endl;
  std::cout << "Using Column Store..." << std::endl;
  if (columnstore::perform_queries(argv[1])) return -1;

  return 0;
}
