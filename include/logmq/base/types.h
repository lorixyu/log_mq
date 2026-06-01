#pragma once

#include <cstdint>
#include <string>

namespace logmq {

using TopicName = std::string;

using PartitionId = std::int32_t;

using Offset = std::int64_t;

// Broker id - 预留
using BrokerId = std::int32_t;

inline constexpr PartitionId kInvalidPartitionId = -1;
inline constexpr Offset kInvalidOffset = -1;
inline constexpr BrokerId kInvalidBrokerId = -1;

}  // namespace logmq
