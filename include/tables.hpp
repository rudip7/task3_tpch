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

#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/align/aligned_allocator.hpp>
#include <boost/lexical_cast.hpp>
#include <chrono>
#include <chrono>
#include <cstdint>
#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>

using NanoSeconds = std::chrono::nanoseconds;
using Clock = std::chrono::high_resolution_clock;
typedef uint64_t Timestamp;
Timestamp getTimestamp();
namespace fs = boost::filesystem;

template <typename T, typename Allocator = std::allocator<T>>
bool readDataFromFile(const std::string& binary_file_path,
                      std::vector<T, Allocator>& v) {
  std::fstream cache_file;
  cache_file.open(binary_file_path.c_str(), std::ios::binary | std::ios::in);
  if (!cache_file.is_open()) {
    std::cout << "Error: could not open file '" << binary_file_path << "'"
              << std::endl;
    return false;
  }
  // get size of file
  cache_file.seekg(0, cache_file.end);
  size_t size = cache_file.tellg();
  cache_file.seekg(0);
  v.resize(size / sizeof(T));
  // read content of cache_file and fill vector of records with data
  cache_file.read(reinterpret_cast<char*>(v.data()), size);
  cache_file.close();
  return true;
}

template <typename T, typename Allocator = std::allocator<T>>
bool writeDataToFile(const std::string& binary_file_path,
                     const std::vector<T, Allocator>& v) {
  std::fstream cache_file;
  cache_file.open(binary_file_path.c_str(),
                  std::ios::binary | std::ios::out | std::ios::trunc);
  if (!cache_file.is_open()) {
    std::cout << "Error: could not open file '" << binary_file_path << "'"
              << std::endl;
    return false;
  }
  cache_file.write(reinterpret_cast<const char*>(v.data()),
                   v.size() * sizeof(T));
  cache_file.close();
  return true;
}

bool convertStringToInternalDateType(const std::string& value,
                                     uint32_t& result);

namespace rowstore {

  template <typename TableType>
  void printTable(const TableType& table) {
    for (size_t i = 0; i < table.records.size(); ++i) {
      std::cout << table.records[i].toString() << std::endl;
    }
  }

  struct lineitem {
    static const int AVG_ROW_LENGTH = 126;
    struct record {
      uint64_t orderkey;
      uint64_t partkey;
      uint64_t suppkey;
      uint8_t lineitem;
      uint16_t quantity;
      float extendedprice;    // decimal(8,2), a real system would not use float
                              // here!
      float discount;         // decimal(3,2)
      float tax;              // decimal(3,2)
      char returnflag;        // char(1)
      char linestatus;        // char(1)
      uint32_t shipdate;      // date, we encode dates as uint32_t type
      uint32_t commitdate;    // date
      uint32_t receiptdate;   // date
      char shipinstruct[26];  // char(25), encode string directly as fixed size
      // struct array, reserve one byte more for null termination character
      char shipmode[11];  // char(10)
      char comment[45];   // varchar(44)

      const std::string toString() const;
      bool parseString(const char* line, size_t row_id);
    };
    static size_t getAverageRowSize();
    std::vector<lineitem::record> records;
  };

  struct part {
    struct record {
      uint64_t p_partkey;
      char p_name[56];       // variable text, size 55
      char p_mfgr[26];       // fixed text, size 25
      char p_brand[11];      // fixed text, size 10
      char p_type[26];       // fixed text, size 25
      int32_t p_size;        // integer
      char p_container[11];  // fixed text, size 11
      float p_retailprice;   /* decimal */
      char p_comment[24];    // fixed text, size 23
      const std::string toString() const;
      bool parseString(const char* line, size_t row_id);
    };
    static size_t getAverageRowSize();
    std::vector<part::record> records;
  };

  struct supplier {
    struct record {
      uint64_t s_suppkey;
      uint64_t s_nationkey; /* Foreign Key to N_NATIONKEY */
      char s_name[26];      // variable text, size 25
      char s_address[41];   // fixed text, size 40
      char s_phone[16];     // fixed text, size 15
      float s_acctbal;      // decimal
      char s_comment[102];  // fixed text, size 101
      const std::string toString() const;
      bool parseString(const char* line, size_t row_id);
    };
    static size_t getAverageRowSize();
    std::vector<supplier::record> records;
  };

  struct partsupplier {
    struct record {
      uint64_t ps_partkey; /*Foreign Key to P_PARTKEY */
      uint64_t ps_suppkey; /*Foreign Key to S_SUPPKEY */
      int32_t ps_availqty;
      float ps_supplycost;   /* decimal */
      char ps_comment[200];  // variable text, size 199
      const std::string toString() const;
      bool parseString(const char* line, size_t row_id);
    };
    static size_t getAverageRowSize();
    std::vector<partsupplier::record> records;
  };

  struct customer {
    struct record {
      uint64_t c_custkey;
      char c_name[26];     // variable text, size 25
      char c_address[41];  // fixed text, size 40
      uint64_t c_nationkey;
      char c_phone[16];       // fixed text, size 15
      float c_acctbal;        // decimal
      char c_mktsegment[11];  // fixed text, size 10
      char c_comment[1];      // variable text, size 1
      const std::string toString() const;
      bool parseString(const char* line, size_t row_id);
    };
    static size_t getAverageRowSize();
    std::vector<customer::record> records;
  };

  struct orders {
    struct record {
      uint64_t o_orderkey;
      uint64_t o_custkey; /* Foreign Key to C_CUSTKEY */
      char o_orderstatus;
      float o_total_price;       /* Decimal */
      uint32_t o_orderdate;      // date, we encode dates as uint32_t type
      char o_orderpriority[16];  // fixed text, size 15
      char o_clerk[16];          // fixed text, size 15
      int32_t o_shippriority;
      char o_comment[45];  // varchar(44)
      const std::string toString() const;
      bool parseString(const char* line, size_t row_id);
    };
    static size_t getAverageRowSize();
    std::vector<orders::record> records;
  };

  struct nation {
    struct record {
      uint64_t n_nationkey;
      uint64_t n_regionkey; /* Foreign Key to R_REGIONKEY */
      char n_name[26];      // variable text, size 25
      char n_comment[153];  // variable text, size 152
      const std::string toString() const;
      bool parseString(const char* line, size_t row_id);
    };
    static size_t getAverageRowSize();
    std::vector<nation::record> records;
  };

  struct region {
    struct record {
      uint64_t r_regionkey;
      char r_name[26];      // variable text, size 25
      char r_comment[153];  // variable text, size 152
      const std::string toString() const;
      bool parseString(const char* line, size_t row_id);
    };
    static size_t getAverageRowSize();
    std::vector<region::record> records;
  };

  template <typename TableType>
  bool loadDataFromFile_impl(const std::string& filepath, TableType& table) {
    /* import file and store in table format */
    std::ifstream fin(filepath.c_str());
    if (!fin.is_open()) {
      std::cout << "Error: could not open file '" << filepath << "'"
                << std::endl;
      return false;
    }
    fs::path p{filepath.c_str()};
    /* estimate number of rows */
    size_t est_num_rows = fs::file_size(p) / TableType::getAverageRowSize();
    table.records.reserve(est_num_rows * 1.3);
    std::cout << "Loading " << typeid(TableType).name() << " table from file: '"
              << filepath << "'" << std::endl;
    size_t row_id = 0;
    std::string line;
    while (fin.good()) {
      std::getline(fin, line, '\n');
      row_id++;
      if (line.empty()) continue;
      typename TableType::record r;
      if (!r.parseString(line.c_str(), row_id)) {
        return false;
      }
      table.records.push_back(r);
    }
    fin.close();

    return true;
  }

  template <typename TableType>
  bool loadDataFromFile(const std::string& filepath, TableType& table) {
    Timestamp begin = getTimestamp();
    bool ret = true;
    std::string binary_file_path = filepath + ".bin";
    std::fstream cache_file;
    cache_file.open(binary_file_path.c_str(), std::ios::binary | std::ios::in);
    if (!cache_file.is_open()) {
      std::cout << "No cached database dump found: '" << binary_file_path << "'"
                << std::endl;
      ret = loadDataFromFile_impl(filepath, table);
      if (!ret) return ret;
      cache_file.open(binary_file_path.c_str(),
                      std::ios::binary | std::ios::out | std::ios::trunc);
      cache_file.write(
          reinterpret_cast<const char*>(table.records.data()),
          table.records.size() * sizeof(typename TableType::record));
      std::cout << "Create binary dump of " << typeid(TableType).name()
                << " table: '" << binary_file_path << "'" << std::endl;
      // return ret;
    } else {
      std::cout << "Cached database dump found: '" << binary_file_path << "'"
                << std::endl;
      // get size of file
      cache_file.seekg(0, cache_file.end);
      size_t size = cache_file.tellg();
      cache_file.seekg(0);
      table.records.resize(size / sizeof(typename TableType::record));
      // read content of cache_file and fill vector of records with data
      cache_file.read(reinterpret_cast<char*>(table.records.data()), size);
      ret = true;
    }
    Timestamp end = getTimestamp();
    std::cout << "Import Time: " << double(end - begin) / (1000 * 1000 * 1000)
              << "s" << std::endl;
    std::cout << "Record Width: " << sizeof(typename TableType::record)
              << "bytes" << std::endl;
    std::cout << "Number of Rows: " << table.records.size() << std::endl;
    std::cout << "Size of Table: "
              << double(sizeof(typename TableType::record) *
                        table.records.size()) /
                     (1024 * 1024 * 1024)
              << "GB" << std::endl;
    return ret;
  }
}

namespace columnstore {

  template <uint32_t T>
  struct FixedStringBuffer {
    FixedStringBuffer() : str() {}
    FixedStringBuffer(const char val[T]) { strncpy(str, val, T); }

    char str[T];
  };
  template <class T>
  using Column = std::vector<T, boost::alignment::aligned_allocator<T, 32>>;

  struct lineitem {
    typedef rowstore::lineitem::record record;

    Column<uint64_t> orderkey;
    Column<uint64_t> partkey;
    Column<uint64_t> suppkey;
    Column<uint8_t> lineitem;
    Column<uint16_t> quantity;
    Column<float>
        extendedprice;  // decimal(8,2), a real system would not use float here!
    Column<float> discount;        // decimal(3,2)
    Column<float> tax;             // decimal(3,2)
    Column<char> returnflag;       // char(1)
    Column<char> linestatus;       // char(1)
    Column<uint32_t> shipdate;     // date, we encode dates as uint32_t type
    Column<uint32_t> commitdate;   // date
    Column<uint32_t> receiptdate;  // date

    Column<FixedStringBuffer<26>>
        shipinstruct;  // char(25), encode string directly as fixed size in
    // struct array, reserve one byte more for null termination character
    Column<FixedStringBuffer<11>> shipmode;  // char(10)
    Column<FixedStringBuffer<45>> comment;   // varchar(44)

    size_t size() const { return orderkey.size(); }

    bool getRecord(record& r, uint64_t tid) const;

    void push_back(const record& r);
  };

  struct part {
    typedef rowstore::part::record record;

    Column<uint64_t> p_partkey;
    Column<FixedStringBuffer<56>> p_name;       // variable text, size 55
    Column<FixedStringBuffer<26>> p_mfgr;       // fixed text, size 25
    Column<FixedStringBuffer<11>> p_brand;      // fixed text, size 10
    Column<FixedStringBuffer<26>> p_type;       // fixed text, size 25
    Column<int32_t> p_size;                     // integer
    Column<FixedStringBuffer<11>> p_container;  // fixed text, size 11
    Column<float> p_retailprice;                /* decimal */
    Column<FixedStringBuffer<45>> p_comment;    // fixed text, size 23

    size_t size() const { return p_partkey.size(); }

    bool getRecord(record& r, uint64_t tid) const;

    void push_back(const record& r);
  };

  struct supplier {
    typedef rowstore::supplier::record record;

    Column<uint64_t> s_suppkey;
    Column<uint64_t> s_nationkey;              /* Foreign Key to N_NATIONKEY */
    Column<FixedStringBuffer<26>> s_name;      // variable text, size 25
    Column<FixedStringBuffer<41>> s_address;   // fixed text, size 40
    Column<FixedStringBuffer<16>> s_phone;     // fixed text, size 15
    Column<float> s_acctbal;                   // decimal
    Column<FixedStringBuffer<102>> s_comment;  // fixed text, size 101

    size_t size() const { return s_suppkey.size(); }

    bool getRecord(record& r, uint64_t tid) const;

    void push_back(const record& r);
  };

  struct partsupplier {
    typedef rowstore::partsupplier::record record;

    Column<uint64_t> ps_partkey; /*Foreign Key to P_PARTKEY */
    Column<uint64_t> ps_suppkey; /*Foreign Key to S_SUPPKEY */
    Column<int32_t> ps_availqty;
    Column<float> ps_supplycost;                /* decimal */
    Column<FixedStringBuffer<200>> ps_comment;  // variable text, size 199

    size_t size() const { return ps_partkey.size(); }

    bool getRecord(record& r, uint64_t tid) const;

    void push_back(const record& r);
  };

  struct customer {
    typedef rowstore::customer::record record;

    Column<uint64_t> c_custkey;
    Column<FixedStringBuffer<26>> c_name;     // variable text, size 25
    Column<FixedStringBuffer<41>> c_address;  // fixed text, size 40
    Column<uint64_t> c_nationkey;
    Column<FixedStringBuffer<16>> c_phone;       // fixed text, size 15
    Column<float> c_acctbal;                     // decimal
    Column<FixedStringBuffer<11>> c_mktsegment;  // fixed text, size 10
    Column<FixedStringBuffer<1>> c_comment;      // variable text, size 1

    size_t size() const { return c_custkey.size(); }

    bool getRecord(record& r, uint64_t tid) const;

    void push_back(const record& r);
  };

  struct orders {
    typedef rowstore::orders::record record;

    Column<uint64_t> o_orderkey;
    Column<uint64_t> o_custkey; /* Foreign Key to C_CUSTKEY */
    Column<char> o_orderstatus;
    Column<float> o_total_price;   /* Decimal */
    Column<uint32_t> o_orderdate;  // date, we encode dates as uint32_t type
    Column<FixedStringBuffer<16>> o_orderpriority;  // fixed text, size 15
    Column<FixedStringBuffer<16>> o_clerk;          // fixed text, size 15
    Column<int32_t> o_shippriority;
    Column<FixedStringBuffer<45>> o_comment;  // varchar(44)

    size_t size() const { return o_orderkey.size(); }

    bool getRecord(record& r, uint64_t tid) const;

    void push_back(const record& r);
  };

  struct nation {
    typedef rowstore::nation::record record;

    Column<uint64_t> n_nationkey;
    Column<uint64_t> n_regionkey;              /* Foreign Key to R_REGIONKEY */
    Column<FixedStringBuffer<26>> n_name;      // variable text, size 25
    Column<FixedStringBuffer<153>> n_comment;  // variable text, size 152

    size_t size() const { return n_nationkey.size(); }

    bool getRecord(record& r, uint64_t tid) const;

    void push_back(const record& r);
  };

  struct region {
    typedef rowstore::region::record record;

    Column<uint64_t> r_regionkey;
    Column<FixedStringBuffer<26>> r_name;      // variable text, size 25
    Column<FixedStringBuffer<153>> r_comment;  // variable text, size 152

    size_t size() const { return r_regionkey.size(); }

    bool getRecord(record& r, uint64_t tid) const;

    void push_back(const record& r);
  };

  template <typename TableType>
  void printTable(const TableType& table) {
    for (size_t i = 0; i < table.size(); ++i) {
      typename TableType::record r;
      table.getRecord(r, i);
      std::cout << r.toString() << std::endl;
    }
  }

  bool loadDataFromFile(const std::string& filepath, lineitem& table);
  bool loadDataFromFile(const std::string& filepath, part& table);
  bool loadDataFromFile(const std::string& filepath, supplier& table);
  bool loadDataFromFile(const std::string& filepath, partsupplier& table);
  bool loadDataFromFile(const std::string& filepath, customer& table);
  bool loadDataFromFile(const std::string& filepath, orders& table);
  bool loadDataFromFile(const std::string& filepath, nation& table);
  bool loadDataFromFile(const std::string& filepath, region& table);
}
