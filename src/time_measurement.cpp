
#include <time_measurement.hpp>

Timestamp getTimestamp() {
  return std::chrono::duration_cast<NanoSeconds>(
             Clock::now().time_since_epoch())
      .count();
}
