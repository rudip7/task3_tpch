
#include <task2.hpp>
#include <tables.hpp>
#include <time_measurement.hpp>

namespace task1{

  /** \brief implement these four functions */

  void aggregation_query_linitem(const rowstore::lineitem& l) {
    const rowstore::lineitem::record* records = l.records.data();
    size_t num_records = l.records.size();


    Timestamp begin = getTimestamp();
    /** \todo implement code */



    Timestamp end = getTimestamp();
    double time_in_ms = double(end - begin) / (1000 * 1000);
    std::cout << "Time: " << time_in_ms << "ms" << std::endl;
  }

  void aggregation_query_linitem(const columnstore::lineitem& l) {
    size_t num_records = l.size();
    const uint16_t* quantity = l.quantity.data();

    Timestamp begin = getTimestamp();
    /** \todo implement code */




    Timestamp end = getTimestamp();
    double time_in_ms = double(end - begin) / (1000 * 1000);
    std::cout << "Time: " << time_in_ms << "ms" << std::endl;
  }


  void tpch_query6_linitem(const rowstore::lineitem& l){

    const rowstore::lineitem::record* records = l.records.data();
    size_t num_records = l.records.size();


    Timestamp begin = getTimestamp();
    /** \todo implement code */





    Timestamp end = getTimestamp();
    double time_in_ms = double(end - begin) / (1000 * 1000);
    std::cout << "Time: " << time_in_ms << "ms" << std::endl;
  }

  void tpch_query6_linitem(const columnstore::lineitem& l){

    size_t num_records = l.size();
    const uint16_t* quantity = l.quantity.data();

    Timestamp begin = getTimestamp();
    /** \todo implement code */



    Timestamp end = getTimestamp();
    double time_in_ms = double(end - begin) / (1000 * 1000);
    std::cout << "Time: " << time_in_ms << "ms" << std::endl;
  }

}
