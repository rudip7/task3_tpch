
#include <task4.hpp>
#include <tables.hpp>
#include <time_measurement.hpp>
#include <thread>
#include <unordered_map>
#include <map>

namespace task4{


    struct part_record {
        char returnflag;
        char linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double sum_disc;
        uint64_t count_order;
    };

  /** \brief implement these three functions */
  void query1_function(const uint16_t* quantity,
                        const char* returnflag,
                       const char* linestatus,
                       const uint32_t* shipdate,
                       const float * discount,
                       const float * extendedprice,
                       const float* tax,
                       std::unordered_map<short, part_record> &hm,
                       unsigned int beginIndex, unsigned int endIndex){
      for (unsigned int j = beginIndex; j < endIndex; ++j)
      {
          if(shipdate[j] <= 19980902) {
              short key = (((short) returnflag[j]) << 8) | ((short) linestatus[j]);
              std::unordered_map<short, part_record>::const_iterator got = hm.find(key);
              if ( got == hm.end() ) {
                 // std::cout << "Key: " << key << " corresponds to " << returnflag[j] << " and " << linestatus[j] << std::endl;
                  part_record* record = static_cast<part_record *>(malloc(sizeof(part_record)));
                  record->sum_qty = quantity[j];
                  record->sum_base_price = extendedprice[j];
                  record->sum_disc_price = record->sum_base_price * (1-discount[j]);
                  record->sum_charge = record->sum_disc_price * (1+tax[j]);
                  record->sum_disc = discount[j];
                  record->count_order = 1.0;
                  record->returnflag = returnflag[j];
                  record->linestatus = linestatus[j];

                  hm[key] = *record;
              }else {
//                  part_record* record = static_cast<part_record *>(malloc(sizeof(part_record)));
                  hm.at(key).sum_qty = got->second.sum_qty + quantity[j];
                  hm.at(key).sum_base_price = got->second.sum_base_price + extendedprice[j];
                  double disc_price = extendedprice[j] *(1-discount[j]);
                  hm.at(key).sum_disc_price = got->second.sum_disc_price + disc_price;
                  hm.at(key).sum_charge = got->second.sum_charge + disc_price * (1+tax[j]);
                  hm.at(key).sum_disc = got->second.sum_disc + discount[j];
                  hm.at(key).count_order = got->second.count_order + 1.0;

              }
          }
      }

  }

  void tpch_query1_impl(const columnstore::lineitem& l, tpch_query_1_result& result){

    /** \todo implement code */

    size_t num_records = l.size();
    const uint16_t* quantity = l.quantity.data();
    const char* returnflag = l.returnflag.data();
    const char* linestatus = l.linestatus.data();
      const uint32_t* shipdate = l.shipdate.data();
      const float * discount = l.discount.data();
      const float * extendedprice = l.extendedprice.data();
      const float* tax = l.tax.data();

      int numThreads = std::thread::hardware_concurrency();
      std::unordered_map<short, part_record> hm[numThreads];
      std::thread threads[numThreads];

      for (int k = 0; k < numThreads; ++k) {
          unsigned int endIndex;
          if (k < numThreads-1){
              endIndex = (k+1)*(num_records+numThreads-1)/numThreads;
          } else{
              endIndex = num_records;
          }

          threads[k] = std::thread(query1_function, std::ref(quantity),
                                   std::ref(returnflag),
                                   std::ref(linestatus),
                                   std::ref(shipdate),
                                   std::ref(discount),
                                   std::ref(extendedprice),
                                   std::ref(tax), std::ref(hm[k]),
                                   k*(num_records+numThreads-1)/numThreads, endIndex);
      }

      for (int m = 0; m < numThreads; ++m) {
          threads[m].join();
      }
      std::map<short, part_record> final_ht;
      for (int j = 0; j < numThreads; ++j) {
          for (auto& x: hm[j]) {
              short key = x.first;
              std::map<short, part_record>::const_iterator got = final_ht.find(key);
              if ( got == final_ht.end() ) {
                  final_ht[x.first] = x.second;
              }else {
//                  part_record* record = static_cast<par
//                  t_record *>(malloc(sizeof(part_record)));
                  final_ht.at(key).sum_qty = got->second.sum_qty + x.second.sum_qty;
                  final_ht.at(key).sum_base_price = got->second.sum_base_price + x.second.sum_base_price;

                  final_ht.at(key).sum_disc_price = got->second.sum_disc_price + x.second.sum_disc_price;
                  final_ht.at(key).sum_charge = got->second.sum_charge + x.second.sum_charge;
                  final_ht.at(key).sum_disc = got->second.sum_disc + x.second.sum_disc;
                  final_ht.at(key).count_order = got->second.count_order + x.second.count_order;

              }
          }
      }

      for (auto& x: final_ht) {
          tpch_query_1_result::record* record = static_cast<tpch_query_1_result::record *>(malloc(sizeof(tpch_query_1_result::record)));
          record->returnflag = x.second.returnflag;
          record->linestatus = x.second.linestatus;
          record->sum_qty = x.second.sum_qty;
          record->sum_base_price = x.second.sum_base_price;
          record->sum_disc_price = x.second.sum_disc_price;
          record->sum_charge = x.second.sum_charge;
          record->avg_qty = x.second.sum_qty / x.second.count_order;
          record->avg_price = x.second.sum_base_price / x.second.count_order;
          record->avg_disc = x.second.sum_disc / x.second.count_order;
          record->count_order = x.second.count_order;

//          std::cout << record->toString() << std::endl;

          result.records.push_back(*record);
      }




  }

  void tpch_query5_impl(const columnstore::lineitem& l,
                   const columnstore::orders& o,
                   const columnstore::partsupplier& ps,
                   const columnstore::part p,
                   const columnstore::supplier s,
                   const columnstore::customer c,
                   const columnstore::nation n,
                   const columnstore::region r,
                   tpch_query_5_result& result){

    /** \todo implement code */

    size_t num_records = l.size();
    const uint16_t* quantity = l.quantity.data();



  }


    void accumulator_function(const uint16_t* quantity, const uint32_t* shipdate, const float * discount,
                              const float * extendedprice, float &agg,
                               unsigned int beginIndex, unsigned int endIndex){
        agg = 0.0;
        for (unsigned int j = beginIndex; j < endIndex; ++j)
        {
            if((shipdate[j] >= 19940101 & shipdate[j] < 19950101) &&
                    (discount[j] >= 0.05f & discount[j] <= 0.07f) &&
                    quantity[j] < 24) {
                agg += (extendedprice[j]*discount[j]);
            }
        }

    }

  void tpch_query6_impl(const columnstore::lineitem& l, tpch_query_6_result& result){
    /** \todo implement code */

    size_t num_records = l.size();
    const uint16_t* quantity = l.quantity.data();
    const uint32_t* shipdate = l.shipdate.data();
    const float * discount = l.discount.data();
    const float * extendedprice = l.extendedprice.data();

    int numThreads = std::thread::hardware_concurrency();
    float agg[numThreads];
    std::thread threads[numThreads];

      for (int k = 0; k < numThreads; ++k) {
          unsigned int endIndex;
          if (k < numThreads-1){
              endIndex = (k+1)*(num_records+numThreads-1)/numThreads;
          } else{
              endIndex = num_records;
          }
          threads[k] = std::thread(accumulator_function, std::ref(quantity), std::ref(shipdate),std::ref(discount),
                                   std::ref(extendedprice), std::ref(agg[k]), (k)*(num_records+numThreads-1)/numThreads, endIndex);
      }

      for (int m = 0; m < numThreads; ++m) {
          threads[m].join();
      }
      result.result = 0.0;
      for (int j = 0; j < numThreads; ++j) {
          result.result += agg[j];
      }
  }



  void tpch_query1(const columnstore::lineitem& l){

    tpch_query_1_result result;
    Timestamp begin = getTimestamp();

    tpch_query1_impl(l, result);

    Timestamp end = getTimestamp();

    if(checkCorrectness(result)){
        double time_in_ms = double(end - begin) / (1000 * 1000);
        std::cout << "Time TPC-H Query 1: " << time_in_ms << "ms" << std::endl;
    }else{
        std::cout << "Incorrect Result of TPC-H Query 1!" << std::endl;
    }

  }

  void tpch_query5(const columnstore::lineitem& l,
                   const columnstore::orders& o,
                   const columnstore::partsupplier& ps,
                   const columnstore::part p,
                   const columnstore::supplier s,
                   const columnstore::customer c,
                   const columnstore::nation n,
                   const columnstore::region r){

    tpch_query_5_result result;
    Timestamp begin = getTimestamp();

    tpch_query5_impl(l, o, ps, p, s, c, n, r, result);

    Timestamp end = getTimestamp();

    if(checkCorrectness(result)){
        double time_in_ms = double(end - begin) / (1000 * 1000);
        std::cout << "Time TPC-H Query 5: " << time_in_ms << "ms" << std::endl;
    }else{
        std::cout << "Incorrect Result of TPC-H Query 5!" << std::endl;
    }
  }

  void tpch_query6(const columnstore::lineitem& l){

    tpch_query_6_result result;
    Timestamp begin = getTimestamp();

    tpch_query6_impl(l, result);

    Timestamp end = getTimestamp();

    if(checkCorrectness(result)){
        double time_in_ms = double(end - begin) / (1000 * 1000);
        std::cout << "Time TPC-H Query 6: " << time_in_ms << "ms" << std::endl;
    }else{
        std::cout << "Incorrect Result of TPC-H Query 6!" << std::endl;
    }

  }


}
