#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace logmq {

[[nodiscard]] std::uint32_t Crc32(std::span<const std::byte> data);

}  // namespace logmq
