
#pragma once

#include <cstdint>
#include <cmath>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <iterator>
#include <string>
#include <sstream>
#include <vector>

struct tpch_query_1_result {
  struct record {
    char returnflag;
    char linestatus;
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double avg_qty;
    double avg_price;
    double avg_disc;
    uint64_t count_order;
    const std::string toString() const;
  };
  std::vector<tpch_query_1_result::record> records;
  const std::string toString() const;
};

struct tpch_query_5_result {
  struct record {
    char n_name[26];      // variable text, size 25
    double revenue;
    const std::string toString() const;
  };
  std::vector<tpch_query_5_result::record> records;
  const std::string toString() const;
};

struct tpch_query_6_result {
   double result;
   const std::string toString() const;
};

bool checkCorrectness(const tpch_query_1_result& result);

bool checkCorrectness(const tpch_query_5_result& result);

bool checkCorrectness(const tpch_query_6_result& result);

template <typename Type>
bool approxEqual(Type left, Type right) {
  if (left == right) {
    return true;
  }
  auto rel_error =
      std::abs(left - right) / std::max(std::abs(left), std::abs(right));
  return rel_error <= 0.001;
}

template<typename Type> inline
const std::string printTableEntry(Type& value, const int& width = 15) {
  std::stringstream str;
  str << std::right << std::setw(width) << std::setfill(' ') << value << " |";
  return str.str();
}

inline
const std::string printTableLine(const int& columns, const int& width = 15) {
  std::stringstream str;
  for (size_t i = 0; i < columns; ++i) {
      str << std::string(width, '-') << "-+";
  }
  return str.str();
}

