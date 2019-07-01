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

#include "tables.hpp"
#include <boost/algorithm/string.hpp>
#include <boost/align/aligned_allocator.hpp>
#include <boost/lexical_cast.hpp>
#include <chrono>
#include <cstdint>
#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>
namespace fs = boost::filesystem;

bool convertStringToInternalDateType(const std::string& value,
                                     uint32_t& result) {
  std::vector<std::string> strs;
  boost::split(strs, value, boost::is_any_of("-"));
  if (strs.size() != 3) {
    return false;
  }
  // we encode a date as integer of the form <year><month><day>
  // e.g., the date '1998-01-05' will be encoded as integer 19980105
  uint32_t res_value = boost::lexical_cast<uint32_t>(strs[2]);
  res_value += boost::lexical_cast<uint32_t>(strs[1]) * 100;
  res_value += boost::lexical_cast<uint32_t>(strs[0]) * 100 * 100;
  result = res_value;
  return true;
}

namespace rowstore {

  void extractString(char* buffer, size_t size, const std::string& s) {
    // if(size>=s.size()){
    strncpy(buffer, s.c_str(), size);
    //}
  }

  bool lineitem::record::parseString(const char* line, size_t row_id) {
    std::vector<std::string> tokens;
    std::string str(line);
    boost::split(tokens, str, boost::is_any_of("|"));
    if (tokens.size() < 16) {
      std::cerr << "Found insufficient tokens in line: '" << line
                << "', line number " << row_id << std::endl;
      return false;
    }
    this->orderkey = boost::lexical_cast<uint64_t>(tokens[0]);
    this->partkey = boost::lexical_cast<uint64_t>(tokens[1]);
    this->suppkey = boost::lexical_cast<uint64_t>(tokens[2]);
    this->lineitem = boost::lexical_cast<uint8_t>(tokens[3]);
    this->quantity = boost::lexical_cast<uint16_t>(tokens[4]);
    this->extendedprice = boost::lexical_cast<float>(tokens[5]);
    this->discount = boost::lexical_cast<float>(tokens[6]);
    this->tax = boost::lexical_cast<float>(tokens[7]);
    this->returnflag = boost::lexical_cast<char>(tokens[8]);
    this->linestatus = boost::lexical_cast<char>(tokens[9]);
    if (!convertStringToInternalDateType(tokens[10], this->shipdate)) {
      std::cout << "Fatal Error! Invalid Date: " << tokens[10] << std::endl;
      return false;
    }
    if (!convertStringToInternalDateType(tokens[11], commitdate)) {
      std::cout << "Fatal Error! Invalid Date: " << tokens[11] << std::endl;
      return false;
    }
    if (!convertStringToInternalDateType(tokens[12], receiptdate)) {
      std::cout << "Fatal Error! Invalid Date: " << tokens[12] << std::endl;
      return false;
    }
    extractString(this->shipinstruct, sizeof(this->shipinstruct), tokens[13]);
    extractString(this->shipmode, sizeof(this->shipmode), tokens[14]);
    extractString(this->comment, sizeof(this->comment), tokens[15]);

    return true;
  }

  const std::string lineitem::record::toString() const {
    std::stringstream s;
    s << orderkey << "|" << partkey << "|" << suppkey << "|" << lineitem << "|"
      << quantity << "|" << extendedprice << "|" << discount << "|" << tax
      << "|" << returnflag << "|" << linestatus << "|" << shipdate << "|"
      << commitdate << "|" << receiptdate << "|" << shipinstruct << "|"
      << shipmode << "|" << comment;
    return s.str();
  }

  size_t lineitem::getAverageRowSize() { return 144; }

  bool part::record::parseString(const char* line, size_t row_id) {
    std::vector<std::string> tokens;
    std::string str(line);
    boost::split(tokens, str, boost::is_any_of("|"));
    if (tokens.size() < 9) {
      std::cerr << "Found insufficient tokens in line: '" << line
                << "', line number " << row_id << std::endl;
      return false;
    }

    this->p_partkey = boost::lexical_cast<uint64_t>(tokens[0]);
    extractString(this->p_name, sizeof(this->p_name),
                  tokens[1]);  // = boost::lexical_cast<uint64_t>(t);
    extractString(this->p_mfgr, sizeof(this->p_mfgr), tokens[2]);
    extractString(this->p_brand, sizeof(this->p_brand), tokens[3]);
    extractString(this->p_type, sizeof(this->p_type), tokens[4]);
    this->p_size = boost::lexical_cast<int32_t>(tokens[5]);
    extractString(this->p_container, sizeof(this->p_container), tokens[6]);
    this->p_retailprice = boost::lexical_cast<float>(tokens[7]);
    extractString(this->p_comment, sizeof(this->p_comment), tokens[8]);

    return true;
  }

  const std::string part::record::toString() const {
    std::stringstream s;
    s << p_partkey << "|" << p_name << "|" << p_mfgr << "|" << p_brand << "|"
      << p_type << "|" << p_size << "|" << p_container << "|" << p_retailprice
      << "|" << p_comment;
    return s.str();
  }

  size_t part::getAverageRowSize() { return 176; }

  bool supplier::record::parseString(const char* line, size_t row_id) {
    std::vector<std::string> tokens;
    std::string str(line);
    boost::split(tokens, str, boost::is_any_of("|"));
    if (tokens.size() < 7) {
      std::cerr << "Found insufficient tokens in line: '" << line
                << "', line number " << row_id << std::endl;
      return false;
    }
    try {
      this->s_suppkey = boost::lexical_cast<uint64_t>(tokens[0]);
      extractString(this->s_name, sizeof(this->s_name),
                    tokens[1]);  // = boost::lexical_cast<uint64_t>(t);
      extractString(this->s_address, sizeof(this->s_address), tokens[2]);
      this->s_nationkey = boost::lexical_cast<uint64_t>(tokens[3]);
      extractString(this->s_phone, sizeof(this->s_phone), tokens[4]);
      this->s_acctbal = boost::lexical_cast<float>(tokens[5]);
      extractString(this->s_comment, sizeof(this->s_comment), tokens[6]);
    } catch (boost::bad_lexical_cast& e) {
      std::cout << "row ID: " << row_id << " Exception: " << e.what()
                << "Source: " << __FILE__ << ":" << __LINE__ << std::endl;
    }
    return true;
  }

  const std::string supplier::record::toString() const {
    std::stringstream s;
    s << s_suppkey << "|" << s_nationkey << "|" << s_name << "|" << s_address
      << "|" << s_phone << "|" << s_acctbal << "|" << s_comment;
    return s.str();
  }

  size_t supplier::getAverageRowSize() { return 208; }

  bool partsupplier::record::parseString(const char* line, size_t row_id) {
    std::vector<std::string> tokens;
    std::string str(line);
    boost::split(tokens, str, boost::is_any_of("|"));
    if (tokens.size() < 5) {
      std::cerr << "Found insufficient tokens in line: '" << line
                << "', line number " << row_id << std::endl;
      return false;
    }
    try {
      this->ps_partkey = boost::lexical_cast<uint64_t>(tokens[0]);
      this->ps_suppkey = boost::lexical_cast<uint64_t>(tokens[1]);
      this->ps_availqty = boost::lexical_cast<int32_t>(tokens[2]);
      this->ps_supplycost = boost::lexical_cast<float>(tokens[3]);
      extractString(this->ps_comment, sizeof(this->ps_comment), tokens[4]);

    } catch (boost::bad_lexical_cast& e) {
      std::cout << "row ID: " << row_id << " Exception: " << e.what()
                << "Source: " << __FILE__ << ":" << __LINE__ << std::endl;
    }
    return true;
  }

  const std::string partsupplier::record::toString() const {
    std::stringstream s;
    s << ps_partkey << "|" << ps_suppkey << "|" << ps_availqty << "|"
      << ps_supplycost << "|" << ps_comment;
    return s.str();
  }

  size_t partsupplier::getAverageRowSize() { return 224; }

  bool customer::record::parseString(const char* line, size_t row_id) {
    std::vector<std::string> tokens;
    std::string str(line);
    boost::split(tokens, str, boost::is_any_of("|"));
    if (tokens.size() < 8) {
      std::cerr << "Found insufficient tokens in line: '" << line
                << "', line number " << row_id << std::endl;
      return false;
    }
    try {
      this->c_custkey = boost::lexical_cast<uint64_t>(tokens[0]);
      extractString(this->c_name, sizeof(this->c_name),
                    tokens[1]);  // = boost::lexical_cast<uint64_t>(t);
      extractString(this->c_address, sizeof(this->c_address), tokens[2]);
      this->c_nationkey = boost::lexical_cast<uint64_t>(tokens[3]);
      extractString(this->c_phone, sizeof(this->c_phone), tokens[4]);
      this->c_acctbal = boost::lexical_cast<float>(tokens[5]);
      extractString(this->c_mktsegment, sizeof(this->c_mktsegment), tokens[6]);
      extractString(this->c_comment, sizeof(this->c_comment), tokens[7]);
    } catch (boost::bad_lexical_cast& e) {
      std::cout << "row ID: " << row_id << " Exception: " << e.what()
                << "Source: " << __FILE__ << ":" << __LINE__ << std::endl;
    }
    return true;
  }

  const std::string customer::record::toString() const {
    std::stringstream s;
    s << c_custkey << "|" << c_name << "|" << c_address << "|" << c_nationkey
      << "|" << c_phone << "|" << c_acctbal << "|" << c_mktsegment << "|"
      << c_comment;
    return s.str();
  }

  size_t customer::getAverageRowSize() { return 120; }

  bool orders::record::parseString(const char* line, size_t row_id) {
    std::vector<std::string> tokens;
    std::string str(line);
    boost::split(tokens, str, boost::is_any_of("|"));
    if (tokens.size() < 9) {
      std::cerr << "Found insufficient tokens in line: '" << line
                << "', line number " << row_id << std::endl;
      return false;
    }
    try {
      this->o_orderkey = boost::lexical_cast<uint64_t>(tokens[0]);
      this->o_custkey = boost::lexical_cast<uint64_t>(tokens[1]);
      this->o_orderstatus = boost::lexical_cast<char>(tokens[2]);
      this->o_total_price = boost::lexical_cast<float>(tokens[3]);
      uint32_t internal_date = 0;
      if (!convertStringToInternalDateType(tokens[4], internal_date)) {
        std::cout << "Fatal Error! Invalid Date: " << tokens[4] << std::endl;
        return false;
      }
      this->o_orderdate = internal_date;
      extractString(this->o_orderpriority, sizeof(this->o_orderpriority),
                    tokens[5]);  // = boost::lexical_cast<uint64_t>(t);
      extractString(this->o_clerk, sizeof(this->o_clerk), tokens[6]);
      this->o_shippriority = boost::lexical_cast<float>(tokens[7]);
      extractString(this->o_comment, sizeof(this->o_comment), tokens[8]);
    } catch (boost::bad_lexical_cast& e) {
      std::cout << "row ID: " << row_id << " Exception: " << e.what()
                << "Source: " << __FILE__ << ":" << __LINE__ << std::endl;
      return false;
    }
    return true;
  }

  const std::string orders::record::toString() const {
    std::stringstream s;
    s << o_orderkey << "|" << o_custkey << "|" << o_orderstatus << "|"
      << o_total_price << "|" << o_orderdate << "|" << o_orderpriority << "|"
      << o_clerk << "|" << o_shippriority << "|" << o_comment;
    return s.str();
  }

  size_t orders::getAverageRowSize() { return 120; }

  bool nation::record::parseString(const char* line, size_t row_id) {
    std::vector<std::string> tokens;
    std::string str(line);
    boost::split(tokens, str, boost::is_any_of("|"));
    if (tokens.size() < 4) {
      std::cerr << "Found insufficient tokens in line: '" << line
                << "', line number " << row_id << std::endl;
      return false;
    }
    try {
      this->n_nationkey = boost::lexical_cast<uint64_t>(tokens[0]);
      extractString(this->n_name, sizeof(this->n_name), tokens[1]);
      this->n_regionkey = boost::lexical_cast<uint64_t>(tokens[2]);
      extractString(this->n_comment, sizeof(this->n_comment), tokens[3]);

    } catch (boost::bad_lexical_cast& e) {
      std::cout << "row ID: " << row_id << " Exception: " << e.what()
                << "Source: " << __FILE__ << ":" << __LINE__ << std::endl;
      return false;
    }
    return true;
  }

  const std::string nation::record::toString() const {
    std::stringstream s;
    s << n_nationkey << "|" << n_regionkey << "|" << n_name << "|" << n_comment;
    return s.str();
  }

  size_t nation::getAverageRowSize() { return 200; }

  bool region::record::parseString(const char* line, size_t row_id) {
    std::vector<std::string> tokens;
    std::string str(line);
    boost::split(tokens, str, boost::is_any_of("|"));
    if (tokens.size() < 3) {
      std::cerr << "Found insufficient tokens in line: '" << line
                << "', line number " << row_id << std::endl;
      return false;
    }
    try {
      this->r_regionkey = boost::lexical_cast<uint64_t>(tokens[0]);
      extractString(this->r_name, sizeof(this->r_name), tokens[1]);
      extractString(this->r_comment, sizeof(this->r_comment), tokens[2]);

    } catch (boost::bad_lexical_cast& e) {
      std::cout << "row ID: " << row_id << " Exception: " << e.what()
                << "Source: " << __FILE__ << ":" << __LINE__ << std::endl;
      return false;
    }
    return true;
  }

  const std::string region::record::toString() const {
    std::stringstream s;
    s << r_regionkey << "|" << r_name << "|" << r_comment;
    return s.str();
  }

  size_t region::getAverageRowSize() { return 200; }
}

namespace columnstore {

  bool lineitem::getRecord(lineitem::record& r, uint64_t tid) const {
    if (tid > size()) return false;
    r.orderkey = orderkey[tid];
    r.partkey = partkey[tid];
    r.suppkey = suppkey[tid];
    r.lineitem = lineitem[tid];
    r.quantity = quantity[tid];
    r.extendedprice = extendedprice[tid];
    r.discount = discount[tid];
    r.tax = tax[tid];
    r.returnflag = returnflag[tid];
    r.linestatus = linestatus[tid];
    r.shipdate = shipdate[tid];
    r.commitdate = commitdate[tid];
    r.receiptdate = receiptdate[tid];
    strcpy(r.shipinstruct, shipinstruct[tid].str);
    strcpy(r.shipmode, shipmode[tid].str);
    strcpy(r.comment, comment[tid].str);
    return true;
  }

  void lineitem::push_back(const lineitem::record& r) {
    orderkey.push_back(r.orderkey);
    partkey.push_back(r.partkey);
    suppkey.push_back(r.suppkey);
    this->lineitem.push_back(r.lineitem);
    quantity.push_back(r.quantity);
    extendedprice.push_back(r.extendedprice);
    discount.push_back(r.discount);
    tax.push_back(r.tax);
    returnflag.push_back(r.returnflag);
    linestatus.push_back(r.linestatus);
    shipdate.push_back(r.shipdate);
    commitdate.push_back(r.commitdate);
    receiptdate.push_back(r.receiptdate);
    shipinstruct.push_back(r.shipinstruct);
    shipmode.push_back(r.shipmode);
    comment.push_back(r.comment);
  }

  void part::push_back(const part::record& r) {
    p_partkey.push_back(r.p_partkey);
    p_name.push_back(r.p_name);
    p_mfgr.push_back(r.p_mfgr);
    p_brand.push_back(r.p_brand);
    p_type.push_back(r.p_type);
    p_size.push_back(r.p_size);
    p_container.push_back(r.p_container);
    p_retailprice.push_back(r.p_retailprice);
    p_comment.push_back(r.p_comment);
  }
  bool part::getRecord(part::record& r, uint64_t tid) const {
    if (tid > size()) return false;
    r.p_partkey = p_partkey[tid];
    strcpy(r.p_name, p_name[tid].str);
    strcpy(r.p_mfgr, p_mfgr[tid].str);
    strcpy(r.p_brand, p_brand[tid].str);
    strcpy(r.p_type, p_type[tid].str);
    r.p_size = p_size[tid];
    strcpy(r.p_container, p_container[tid].str);
    r.p_retailprice = p_retailprice[tid];
    strcpy(r.p_comment, p_comment[tid].str);
    return true;
  }
  void supplier::push_back(const supplier::record& r) {
    s_suppkey.push_back(r.s_suppkey);
    s_nationkey.push_back(r.s_nationkey);
    s_name.push_back(r.s_name);
    s_address.push_back(r.s_address);
    s_phone.push_back(r.s_phone);
    s_acctbal.push_back(r.s_acctbal);
    s_comment.push_back(r.s_comment);
  }
  bool supplier::getRecord(supplier::record& r, uint64_t tid) const {
    if (tid > size()) return false;
    r.s_suppkey = s_suppkey[tid];
    r.s_nationkey = s_nationkey[tid];
    strcpy(r.s_name, s_name[tid].str);
    strcpy(r.s_address, s_address[tid].str);
    strcpy(r.s_phone, s_phone[tid].str);
    r.s_acctbal = s_acctbal[tid];
    strcpy(r.s_comment, s_comment[tid].str);

    return true;
  }
  void partsupplier::push_back(const partsupplier::record& r) {
    ps_partkey.push_back(r.ps_partkey);
    ps_suppkey.push_back(r.ps_suppkey);
    ps_availqty.push_back(r.ps_availqty);
    ps_supplycost.push_back(r.ps_supplycost);
    ps_comment.push_back(r.ps_comment);
  }
  bool partsupplier::getRecord(partsupplier::record& r, uint64_t tid) const {
    if (tid > size()) return false;
    r.ps_partkey = ps_partkey[tid];
    r.ps_suppkey = ps_suppkey[tid];
    r.ps_availqty = ps_availqty[tid];
    r.ps_supplycost = ps_supplycost[tid];
    strcpy(r.ps_comment, ps_comment[tid].str);

    return true;
  }
  void customer::push_back(const customer::record& r) {
    c_custkey.push_back(r.c_custkey);
    c_name.push_back(r.c_name);
    c_address.push_back(r.c_address);
    c_nationkey.push_back(r.c_nationkey);
    c_phone.push_back(r.c_phone);
    c_acctbal.push_back(r.c_acctbal);
    c_mktsegment.push_back(r.c_mktsegment);
    c_comment.push_back(r.c_comment);
  }
  bool customer::getRecord(customer::record& r, uint64_t tid) const {
    if (tid > size()) return false;
    r.c_custkey = c_custkey[tid];
    strcpy(r.c_name, c_name[tid].str);
    strcpy(r.c_address, c_address[tid].str);
    r.c_nationkey = c_nationkey[tid];
    strcpy(r.c_phone, c_phone[tid].str);
    r.c_acctbal = c_acctbal[tid];
    strcpy(r.c_mktsegment, c_mktsegment[tid].str);
    strcpy(r.c_comment, c_comment[tid].str);

    return true;
  }
  void orders::push_back(const orders::record& r) {
    o_orderkey.push_back(r.o_orderkey);
    o_custkey.push_back(r.o_custkey);
    o_orderstatus.push_back(r.o_orderstatus);
    o_total_price.push_back(r.o_total_price);
    o_orderdate.push_back(r.o_orderdate);
    o_orderpriority.push_back(r.o_orderpriority);
    o_clerk.push_back(r.o_clerk);
    o_shippriority.push_back(r.o_shippriority);
    o_comment.push_back(r.o_comment);
  }
  bool orders::getRecord(orders::record& r, uint64_t tid) const {
    if (tid > size()) return false;
    r.o_orderkey = o_orderkey[tid];
    r.o_custkey = o_custkey[tid];
    r.o_orderstatus = o_orderstatus[tid];
    r.o_total_price = o_total_price[tid];
    r.o_orderdate = o_orderdate[tid];
    strcpy(r.o_orderpriority, o_orderpriority[tid].str);
    strcpy(r.o_clerk, o_clerk[tid].str);
    r.o_shippriority = o_shippriority[tid];
    strcpy(r.o_comment, o_comment[tid].str);

    return true;
  }
  void nation::push_back(const nation::record& r) {
    n_nationkey.push_back(r.n_nationkey);
    n_regionkey.push_back(r.n_regionkey);
    n_name.push_back(r.n_name);
    n_comment.push_back(r.n_comment);
  }
  bool nation::getRecord(nation::record& r, uint64_t tid) const {
    if (tid > size()) return false;
    r.n_nationkey = n_nationkey[tid];
    r.n_regionkey = n_regionkey[tid];
    strcpy(r.n_name, n_name[tid].str);
    strcpy(r.n_comment, n_comment[tid].str);

    return true;
  }
  void region::push_back(const region::record& r) {
    r_regionkey.push_back(r.r_regionkey);
    r_name.push_back(r.r_name);
    r_comment.push_back(r.r_comment);
  }
  bool region::getRecord(region::record& r, uint64_t tid) const {
    if (tid > size()) return false;
    r.r_regionkey = r_regionkey[tid];
    strcpy(r.r_name, r_name[tid].str);
    strcpy(r.r_comment, r_comment[tid].str);
    return true;
  }

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
    size_t est_num_rows =
        fs::file_size(p) / 126; /* average row length is 126 byte */

    std::cout << "Loading lineitem table from file: '" << filepath << "'"
              << std::endl;
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
      table.push_back(r);
    }
    fin.close();

    return true;
  }

  bool loadDataFromFile(const std::string& filepath, lineitem& table) {
    bool ret = true;
    std::string cache_dir = filepath + "_col_store_cache";

    if (fs::exists(cache_dir.c_str())) {
      ret &= readDataFromFile(cache_dir + "/orderkey.bin", table.orderkey);
      ret &= readDataFromFile(cache_dir + "/partkey.bin", table.partkey);
      ret &= readDataFromFile(cache_dir + "/suppkey.bin", table.suppkey);
      ret &= readDataFromFile(cache_dir + "/lineitem.bin", table.lineitem);
      ret &= readDataFromFile(cache_dir + "/quantity.bin", table.quantity);
      ret &= readDataFromFile(cache_dir + "/extendedprice.bin",
                              table.extendedprice);
      ret &= readDataFromFile(cache_dir + "/discount.bin", table.discount);
      ret &= readDataFromFile(cache_dir + "/tax.bin", table.tax);
      ret &= readDataFromFile(cache_dir + "/returnflag.bin", table.returnflag);
      ret &= readDataFromFile(cache_dir + "/linestatus.bin", table.linestatus);
      ret &= readDataFromFile(cache_dir + "/shipdate.bin", table.shipdate);
      ret &= readDataFromFile(cache_dir + "/commitdate.bin", table.commitdate);
      ret &=
          readDataFromFile(cache_dir + "/receiptdate.bin", table.receiptdate);
      ret &=
          readDataFromFile(cache_dir + "/shipinstruct.bin", table.shipinstruct);
      ret &= readDataFromFile(cache_dir + "/shipmode.bin", table.shipmode);
      ret &= readDataFromFile(cache_dir + "/comment.bin", table.comment);

      return ret;
    } else {
      ret = loadDataFromFile_impl(filepath, table);
      if (!ret) return false;

      if (!fs::exists(cache_dir.c_str()))
        fs::create_directory(cache_dir.c_str());

      ret &= writeDataToFile(cache_dir + "/orderkey.bin", table.orderkey);
      ret &= writeDataToFile(cache_dir + "/partkey.bin", table.partkey);
      ret &= writeDataToFile(cache_dir + "/suppkey.bin", table.suppkey);
      ret &= writeDataToFile(cache_dir + "/lineitem.bin", table.lineitem);
      ret &= writeDataToFile(cache_dir + "/quantity.bin", table.quantity);
      ret &= writeDataToFile(cache_dir + "/extendedprice.bin",
                             table.extendedprice);
      ret &= writeDataToFile(cache_dir + "/discount.bin", table.discount);
      ret &= writeDataToFile(cache_dir + "/tax.bin", table.tax);
      ret &= writeDataToFile(cache_dir + "/returnflag.bin", table.returnflag);
      ret &= writeDataToFile(cache_dir + "/linestatus.bin", table.linestatus);
      ret &= writeDataToFile(cache_dir + "/shipdate.bin", table.shipdate);
      ret &= writeDataToFile(cache_dir + "/commitdate.bin", table.commitdate);
      ret &= writeDataToFile(cache_dir + "/receiptdate.bin", table.receiptdate);
      ret &=
          writeDataToFile(cache_dir + "/shipinstruct.bin", table.shipinstruct);
      ret &= writeDataToFile(cache_dir + "/shipmode.bin", table.shipmode);
      ret &= writeDataToFile(cache_dir + "/comment.bin", table.comment);

      return true;
    }
  }

  bool loadDataFromFile(const std::string& filepath, part& table) {
    bool ret = true;
    std::string cache_dir = filepath + "_col_store_cache";

    if (fs::exists(cache_dir.c_str())) {
      ret &= readDataFromFile(cache_dir + "/p_partkey.bin", table.p_partkey);
      ret &= readDataFromFile(cache_dir + "/p_name.bin", table.p_name);
      ret &= readDataFromFile(cache_dir + "/p_mfgr.bin", table.p_mfgr);
      ret &= readDataFromFile(cache_dir + "/p_brand.bin", table.p_brand);
      ret &= readDataFromFile(cache_dir + "/p_type.bin", table.p_type);
      ret &= readDataFromFile(cache_dir + "/p_size.bin", table.p_size);
      ret &=
          readDataFromFile(cache_dir + "/p_container.bin", table.p_container);
      ret &= readDataFromFile(cache_dir + "/p_retailprice.bin",
                              table.p_retailprice);
      ret &= readDataFromFile(cache_dir + "/p_comment.bin", table.p_comment);
      return ret;
    } else {
      ret = loadDataFromFile_impl(filepath, table);
      if (!ret) return false;

      if (!fs::exists(cache_dir.c_str()))
        fs::create_directory(cache_dir.c_str());

      ret &= writeDataToFile(cache_dir + "/p_partkey.bin", table.p_partkey);
      ret &= writeDataToFile(cache_dir + "/p_name.bin", table.p_name);
      ret &= writeDataToFile(cache_dir + "/p_mfgr.bin", table.p_mfgr);
      ret &= writeDataToFile(cache_dir + "/p_brand.bin", table.p_brand);
      ret &= writeDataToFile(cache_dir + "/p_type.bin", table.p_type);
      ret &= writeDataToFile(cache_dir + "/p_size.bin", table.p_size);
      ret &= writeDataToFile(cache_dir + "/p_container.bin", table.p_container);
      ret &= writeDataToFile(cache_dir + "/p_retailprice.bin",
                             table.p_retailprice);
      ret &= writeDataToFile(cache_dir + "/p_comment.bin", table.p_comment);

      return true;
    }
  }

  bool loadDataFromFile(const std::string& filepath, supplier& table) {
    bool ret = true;
    std::string cache_dir = filepath + "_col_store_cache";

    if (fs::exists(cache_dir.c_str())) {
      ret &= readDataFromFile(cache_dir + "/s_suppkey.bin", table.s_suppkey);
      ret &=
          readDataFromFile(cache_dir + "/s_nationkey.bin", table.s_nationkey);
      ret &= readDataFromFile(cache_dir + "/s_name.bin", table.s_name);
      ret &= readDataFromFile(cache_dir + "/s_address.bin", table.s_address);
      ret &= readDataFromFile(cache_dir + "/s_phone.bin", table.s_phone);
      ret &= readDataFromFile(cache_dir + "/s_acctbal.bin", table.s_acctbal);
      ret &= readDataFromFile(cache_dir + "/s_comment.bin", table.s_comment);

      return ret;
    } else {
      ret = loadDataFromFile_impl(filepath, table);
      if (!ret) return false;

      if (!fs::exists(cache_dir.c_str()))
        fs::create_directory(cache_dir.c_str());

      ret &= writeDataToFile(cache_dir + "/s_suppkey.bin", table.s_suppkey);
      ret &= writeDataToFile(cache_dir + "/s_nationkey.bin", table.s_nationkey);
      ret &= writeDataToFile(cache_dir + "/s_name.bin", table.s_name);
      ret &= writeDataToFile(cache_dir + "/s_address.bin", table.s_address);
      ret &= writeDataToFile(cache_dir + "/s_phone.bin", table.s_phone);
      ret &= writeDataToFile(cache_dir + "/s_acctbal.bin", table.s_acctbal);
      ret &= writeDataToFile(cache_dir + "/s_comment.bin", table.s_comment);

      return true;
    }
  }

  bool loadDataFromFile(const std::string& filepath, partsupplier& table) {
    bool ret = true;
    std::string cache_dir = filepath + "_col_store_cache";

    if (fs::exists(cache_dir.c_str())) {
      ret &= readDataFromFile(cache_dir + "/ps_partkey.bin", table.ps_partkey);
      ret &= readDataFromFile(cache_dir + "/ps_suppkey.bin", table.ps_suppkey);
      ret &=
          readDataFromFile(cache_dir + "/ps_availqty.bin", table.ps_availqty);
      ret &= readDataFromFile(cache_dir + "/ps_supplycost.bin",
                              table.ps_supplycost);
      ret &= readDataFromFile(cache_dir + "/ps_comment.bin", table.ps_comment);

      return ret;
    } else {
      ret = loadDataFromFile_impl(filepath, table);
      if (!ret) return false;

      if (!fs::exists(cache_dir.c_str()))
        fs::create_directory(cache_dir.c_str());

      ret &= writeDataToFile(cache_dir + "/ps_partkey.bin", table.ps_partkey);
      ret &= writeDataToFile(cache_dir + "/ps_suppkey.bin", table.ps_suppkey);
      ret &= writeDataToFile(cache_dir + "/ps_availqty.bin", table.ps_availqty);
      ret &= writeDataToFile(cache_dir + "/ps_supplycost.bin",
                             table.ps_supplycost);
      ret &= writeDataToFile(cache_dir + "/ps_comment.bin", table.ps_comment);

      return true;
    }
  }

  bool loadDataFromFile(const std::string& filepath, customer& table) {
    bool ret = true;
    std::string cache_dir = filepath + "_col_store_cache";

    if (fs::exists(cache_dir.c_str())) {
      ret &= readDataFromFile(cache_dir + "/c_custkey.bin", table.c_custkey);
      ret &= readDataFromFile(cache_dir + "/c_name.bin", table.c_name);
      ret &= readDataFromFile(cache_dir + "/c_address.bin", table.c_address);
      ret &=
          readDataFromFile(cache_dir + "/c_nationkey.bin", table.c_nationkey);
      ret &= readDataFromFile(cache_dir + "/c_phone.bin", table.c_phone);
      ret &= readDataFromFile(cache_dir + "/c_acctbal.bin", table.c_acctbal);
      ret &=
          readDataFromFile(cache_dir + "/c_mktsegment.bin", table.c_mktsegment);
      ret &= readDataFromFile(cache_dir + "/c_comment.bin", table.c_comment);

      return ret;
    } else {
      ret = loadDataFromFile_impl(filepath, table);
      if (!ret) return false;

      if (!fs::exists(cache_dir.c_str()))
        fs::create_directory(cache_dir.c_str());

      ret &= writeDataToFile(cache_dir + "/c_custkey.bin", table.c_custkey);
      ret &= writeDataToFile(cache_dir + "/c_name.bin", table.c_name);
      ret &= writeDataToFile(cache_dir + "/c_address.bin", table.c_address);
      ret &= writeDataToFile(cache_dir + "/c_nationkey.bin", table.c_nationkey);
      ret &= writeDataToFile(cache_dir + "/c_phone.bin", table.c_phone);
      ret &= writeDataToFile(cache_dir + "/c_acctbal.bin", table.c_acctbal);
      ret &=
          writeDataToFile(cache_dir + "/c_mktsegment.bin", table.c_mktsegment);
      ret &= writeDataToFile(cache_dir + "/c_comment.bin", table.c_comment);

      return true;
    }
  }

  bool loadDataFromFile(const std::string& filepath, orders& table) {
    bool ret = true;
    std::string cache_dir = filepath + "_col_store_cache";

    if (fs::exists(cache_dir.c_str())) {
      ret &= readDataFromFile(cache_dir + "/o_orderkey.bin", table.o_orderkey);
      ret &= readDataFromFile(cache_dir + "/o_custkey.bin", table.o_custkey);
      ret &= readDataFromFile(cache_dir + "/o_orderstatus.bin",
                              table.o_orderstatus);
      ret &= readDataFromFile(cache_dir + "/o_total_price.bin",
                              table.o_total_price);
      ret &=
          readDataFromFile(cache_dir + "/o_orderdate.bin", table.o_orderdate);
      ret &= readDataFromFile(cache_dir + "/o_orderpriority.bin",
                              table.o_orderpriority);
      ret &= readDataFromFile(cache_dir + "/o_clerk.bin", table.o_clerk);
      ret &= readDataFromFile(cache_dir + "/o_shippriority.bin",
                              table.o_shippriority);
      ret &= readDataFromFile(cache_dir + "/o_comment.bin", table.o_comment);

      return ret;
    } else {
      ret = loadDataFromFile_impl(filepath, table);
      if (!ret) return false;

      if (!fs::exists(cache_dir.c_str()))
        fs::create_directory(cache_dir.c_str());

      ret &= writeDataToFile(cache_dir + "/o_orderkey.bin", table.o_orderkey);
      ret &= writeDataToFile(cache_dir + "/o_custkey.bin", table.o_custkey);
      ret &= writeDataToFile(cache_dir + "/o_orderstatus.bin",
                             table.o_orderstatus);
      ret &= writeDataToFile(cache_dir + "/o_total_price.bin",
                             table.o_total_price);
      ret &= writeDataToFile(cache_dir + "/o_orderdate.bin", table.o_orderdate);
      ret &= writeDataToFile(cache_dir + "/o_orderpriority.bin",
                             table.o_orderpriority);
      ret &= writeDataToFile(cache_dir + "/o_clerk.bin", table.o_clerk);
      ret &= writeDataToFile(cache_dir + "/o_shippriority.bin",
                             table.o_shippriority);
      ret &= writeDataToFile(cache_dir + "/o_comment.bin", table.o_comment);

      return true;
    }
  }

  bool loadDataFromFile(const std::string& filepath, nation& table) {
    bool ret = true;
    std::string cache_dir = filepath + "_col_store_cache";

    if (fs::exists(cache_dir.c_str())) {
      ret &=
          readDataFromFile(cache_dir + "/n_nationkey.bin", table.n_nationkey);
      ret &=
          readDataFromFile(cache_dir + "/n_regionkey.bin", table.n_regionkey);
      ret &= readDataFromFile(cache_dir + "/n_name.bin", table.n_name);
      ret &= readDataFromFile(cache_dir + "/n_comment.bin", table.n_comment);

      return ret;
    } else {
      ret = loadDataFromFile_impl(filepath, table);
      if (!ret) return false;

      if (!fs::exists(cache_dir.c_str()))
        fs::create_directory(cache_dir.c_str());

      ret &= writeDataToFile(cache_dir + "/n_nationkey.bin", table.n_nationkey);
      ret &= writeDataToFile(cache_dir + "/n_regionkey.bin", table.n_regionkey);
      ret &= writeDataToFile(cache_dir + "/n_name.bin", table.n_name);
      ret &= writeDataToFile(cache_dir + "/n_comment.bin", table.n_comment);

      return true;
    }
  }

  bool loadDataFromFile(const std::string& filepath, region& table) {
    bool ret = true;
    std::string cache_dir = filepath + "_col_store_cache";

    if (fs::exists(cache_dir.c_str())) {
      ret &=
          readDataFromFile(cache_dir + "/r_regionkey.bin", table.r_regionkey);
      ret &= readDataFromFile(cache_dir + "/r_name.bin", table.r_name);
      ret &= readDataFromFile(cache_dir + "/r_comment.bin", table.r_comment);

      return ret;
    } else {
      ret = loadDataFromFile_impl(filepath, table);
      if (!ret) return false;

      if (!fs::exists(cache_dir.c_str()))
        fs::create_directory(cache_dir.c_str());

      ret &= writeDataToFile(cache_dir + "/r_regionkey.bin", table.r_regionkey);
      ret &= writeDataToFile(cache_dir + "/r_name.bin", table.r_name);
      ret &= writeDataToFile(cache_dir + "/r_comment.bin", table.r_comment);

      return true;
    }
  }
}
