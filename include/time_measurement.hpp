
#pragma once

#include <chrono>
#include <cstdint>

using NanoSeconds = std::chrono::nanoseconds;
using Clock = std::chrono::high_resolution_clock;
typedef uint64_t Timestamp;

Timestamp getTimestamp();
