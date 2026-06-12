#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "logmq/base/result.h"
#include "logmq/protocol/types.h"

namespace logmq {

[[nodiscard]] std::string_view ApiKeyName(ApiKey api_key);

[[nodiscard]] std::string_view ProtocolErrorCodeName(ProtocolErrorCode code);

[[nodiscard]] Result<std::vector<std::byte>> EncodeRequest(const RequestEnvelope& request);

[[nodiscard]] Result<RequestEnvelope> DecodeRequest(std::span<const std::byte> frame);

[[nodiscard]] Result<std::vector<std::byte>> EncodeResponse(const ResponseEnvelope& response);

[[nodiscard]] Result<std::vector<std::byte>> EncodeErrorResponse(
    ApiKey api_key, std::uint16_t version, std::uint64_t request_id, ProtocolErrorCode code,
    std::string_view message);

[[nodiscard]] Result<ResponseEnvelope> DecodeResponse(std::span<const std::byte> frame);

class FrameDecoder {
public:
    explicit FrameDecoder(std::uint32_t max_frame_bytes = kMaxFrameBytes);

    [[nodiscard]] Result<std::vector<RequestEnvelope>> Push(std::span<const std::byte> data);

    void Clear();

    [[nodiscard]] std::size_t buffered_bytes() const;

private:
    std::uint32_t max_frame_bytes_{kMaxFrameBytes};
    std::vector<std::byte> buffer_;
};

}  // namespace logmq
