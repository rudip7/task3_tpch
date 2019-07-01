
#include <query_tests.hpp>
#include <queries.hpp>
#include <tables.hpp>
#include <omp.h>
#include <unordered_map>


const std::string tpch_query_1_result::record::toString() const {
  std::stringstream str;
  str << printTableEntry(returnflag)
      << printTableEntry(linestatus)
      << printTableEntry(sum_qty)
      << printTableEntry(sum_base_price)
      << printTableEntry(sum_disc_price)
      << printTableEntry(sum_charge)
      << printTableEntry(avg_qty)
      << printTableEntry(avg_price)
      << printTableEntry(avg_disc)
      << printTableEntry(count_order);
  return str.str();
}

const std::string tpch_query_1_result::toString() const {
  std::stringstream str;
  str << printTableEntry("returnflag")
      << printTableEntry("linestatus")
      << printTableEntry("sum_qty")
      << printTableEntry("sum_base_price")
      << printTableEntry("sum_disc_price")
      << printTableEntry("sum_charge")
      << printTableEntry("avg_qty")
      << printTableEntry("avg_price")
      << printTableEntry("avg_disc")
      << printTableEntry("count_order")
      << std::endl;
  str << printTableLine(10) << std::endl;
  for (size_t i = 0; i < records.size(); ++i) {
    str << records[i].toString() << std::endl;
  }
  str << printTableLine(10);
  return str.str();
}

const std::string tpch_query_5_result::record::toString() const {
  std::stringstream str;
  str << printTableEntry(n_name)
      << printTableEntry(revenue);
  return str.str();
}

const std::string tpch_query_5_result::toString() const {
  std::stringstream str;
  str << printTableEntry("n_name")
      << printTableEntry("revenue")
      << std::endl;
  str << printTableLine(2) << std::endl;
  for (size_t i = 0; i < records.size(); ++i) {
    str << records[i].toString() << std::endl;
  }
  str << printTableLine(2);
  return str.str();
}

const std::string tpch_query_6_result::toString() const {
  std::stringstream str;
  str << printTableEntry("revenue")
      << std::endl;
  str << printTableLine(1) << std::endl;

  str << result << std::endl;

  str << printTableLine(1);
  return str.str();
}

/* result for TPC-H Query 1 with SF 1 lineitem:
l_returnflag	l_linestatus	sum_qty	sum_base_price
sum_disc_price	sum_charge	avg_qty	avg_price	avg_disc
count_order
A	F	37734107.00	56586554400.73	53758257134.8700
55909065222.827692	25.5220	38273.1297	0.0500	1478493
N	F	991417.00	1487504710.38	1413082168.0541
1469649223.194375	25.5165	38284.4678	0.0501	38854
N	O	74476040.00	111701729697.74	106118230307.6056
110367043872.497010	25.5022	38249.1180	0.0500	2920374
R	F	37719753.00	56568041380.90	53741292684.6040
55889619119.831932	25.5058	38250.8546	0.0500	1478870
*/
bool checkCorrectness(const tpch_query_1_result& result) {
  if (result.records.size() != 4) {
    std::cerr << "Wrong Number of Rows! Expected: 4 Got: "
              << result.records.size() << std::endl;
    return false;
  }
  /* check row 0 */
  if (result.records[0].returnflag != 'A') return false;
  if (result.records[0].linestatus != 'F') return false;
  if (result.records[0].count_order != 1478493) return false;
  if (!approxEqual(result.records[0].sum_qty, 37734107.00)) return false;
  if (!approxEqual(result.records[0].sum_base_price, 56586554400.73))
    return false;
  if (!approxEqual(result.records[0].sum_disc_price, 53758257134.8700))
    return false;
  if (!approxEqual(result.records[0].sum_charge, 55909065222.827692))
    return false;
  if (!approxEqual(result.records[0].avg_qty, 25.5220)) return false;
  if (!approxEqual(result.records[0].avg_price, 38273.1297)) return false;
  if (!approxEqual(result.records[0].avg_disc, 0.0500)) return false;
  std::cout << "Row 0 correct!" << std::endl;
  /* check row 1 */
  if (result.records[1].returnflag != 'N') return false;
  if (result.records[1].linestatus != 'F') return false;
  if (result.records[1].count_order != 38854) return false;
  if (!approxEqual(result.records[1].sum_qty, 991417.00)) return false;
  if (!approxEqual(result.records[1].sum_base_price, 1487504710.38))
    return false;
  if (!approxEqual(result.records[1].sum_disc_price, 1413082168.0541))
    return false;
  if (!approxEqual(result.records[1].sum_charge, 1469649223.194375))
    return false;
  if (!approxEqual(result.records[1].avg_qty, 25.5165)) return false;
  if (!approxEqual(result.records[1].avg_price, 38284.4678)) return false;
  if (!approxEqual(result.records[1].avg_disc, 0.0501)) return false;
  std::cout << "Row 1 correct!" << std::endl;
  /* check row 2 */
  if (result.records[2].returnflag != 'N') return false;
  if (result.records[2].linestatus != 'O') return false;
  if (result.records[2].count_order != 2920374) return false;
  if (!approxEqual(result.records[2].sum_qty, 74476040.00)) return false;
  if (!approxEqual(result.records[2].sum_base_price, 111701729697.74))
    return false;
  if (!approxEqual(result.records[2].sum_disc_price, 106118230307.6056))
    return false;
  if (!approxEqual(result.records[2].sum_charge, 110367043872.497010))
    return false;
  if (!approxEqual(result.records[2].avg_qty, 25.5022)) return false;
  if (!approxEqual(result.records[2].avg_price, 38249.1180)) return false;
  if (!approxEqual(result.records[2].avg_disc, 0.0500)) return false;
  std::cout << "Row 2 correct!" << std::endl;
  /* check row 3 */
  if (result.records[3].returnflag != 'R') return false;
  if (result.records[3].linestatus != 'F') return false;
  if (result.records[3].count_order != 1478870) return false;
  if (!approxEqual(result.records[3].sum_qty, 37719753.00)) return false;
  if (!approxEqual(result.records[3].sum_base_price, 56568041380.90))
    return false;
  if (!approxEqual(result.records[3].sum_disc_price, 53741292684.6040))
    return false;
  if (!approxEqual(result.records[3].sum_charge, 55889619119.831932))
    return false;
  if (!approxEqual(result.records[3].avg_qty, 25.5058)) return false;
  if (!approxEqual(result.records[3].avg_price, 38250.8546)) return false;
  if (!approxEqual(result.records[3].avg_disc, 0.0500)) return false;
  std::cout << "Row 3 correct!" << std::endl;

  return true;
}

/*
 *  result for TPC-H Query 5 with SF 1 database:
n_name	revenue
INDONESIA	55502041.1697
VIETNAM	55295086.9967
CHINA	53724494.2566
INDIA	52035512.0002
JAPAN	45410175.6954
*/
bool checkCorrectness(const tpch_query_5_result& result){

  if (result.records.size() != 5) {
    std::cerr << "Wrong Number of Rows! Expected: 5 Got: "
              << result.records.size() << std::endl;
    return false;
  }

  if (strcmp(result.records[0].n_name,"INDONESIA")!=0) return false;
  if (strcmp(result.records[1].n_name,"VIETNAM")!=0) return false;
  if (strcmp(result.records[2].n_name,"CHINA")!=0) return false;
  if (strcmp(result.records[3].n_name,"INDIA")!=0) return false;
  if (strcmp(result.records[4].n_name,"JAPAN")!=0) return false;

  if(!approxEqual(result.records[0].revenue, 55502041.1697)) return false;
  if(!approxEqual(result.records[1].revenue, 55295086.9967)) return false;
  if(!approxEqual(result.records[2].revenue, 53724494.2566)) return false;
  if(!approxEqual(result.records[3].revenue, 52035512.0002)) return false;
  if(!approxEqual(result.records[4].revenue, 45410175.6954)) return false;

  return true;
}

/*
 *  result for TPC-H Query 6 with SF 1 database:
revenue
123141078.2283
 */

bool checkCorrectness(const tpch_query_6_result& result){
  if (!approxEqual(result.result, 123141078.2283)) return false;
  return true;
}

