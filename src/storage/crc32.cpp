#include "logmq/storage/crc32.h"

#include <cstddef>
#include <cstdint>

namespace logmq {

std::uint32_t Crc32(std::span<const std::byte> data) {
    std::uint32_t crc = 0xFFFFFFFFU;

    for (const std::byte byte : data) {
        crc ^= std::to_integer<std::uint8_t>(byte);
        for (int bit = 0; bit < 8; ++bit) {
            const std::uint32_t mask = 0U - (crc & 1U);
            crc = (crc >> 1U) ^ (0xEDB88320U & mask);
        }
    }

    return crc ^ 0xFFFFFFFFU;
}

}  // namespace logmq
