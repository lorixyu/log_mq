#include <arpa/inet.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <span>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <variant>
#include <vector>

#include "logmq/protocol/codec.h"

namespace {

struct CliOptions {
    std::string host{"127.0.0.1"};
    std::uint16_t port{9092};
    std::string command;
    int command_index{1};
};

void PrintUsage() {
    std::cerr << "Usage: logmq_cli [--host HOST] [--port PORT] <command> [options]\n"
              << "Commands:\n"
              << "  create-topic --topic TOPIC --partitions N\n"
              << "  metadata [--topic TOPIC]\n"
              << "  produce --topic TOPIC [--partition N] --key KEY --value VALUE [--timestamp TS]\n"
              << "  fetch --topic TOPIC --partition N --offset OFFSET [--max-bytes N]\n";
}

std::string ReadArg(int& index, int argc, char** argv) {
    if (index + 1 >= argc) {
        throw std::runtime_error(std::string("missing value for ") + argv[index]);
    }
    ++index;
    return argv[index];
}

CliOptions ParseGlobalOptions(int argc, char** argv) {
    CliOptions options;
    int i = 1;
    while (i < argc) {
        const std::string arg = argv[i];
        if (arg == "--host") {
            options.host = ReadArg(i, argc, argv);
        } else if (arg == "--port") {
            options.port = static_cast<std::uint16_t>(std::stoul(ReadArg(i, argc, argv)));
        } else {
            options.command = arg;
            options.command_index = i;
            return options;
        }
        ++i;
    }
    throw std::runtime_error("missing command");
}

std::string OptionValue(int argc, char** argv, int start, std::string_view name,
                        std::string default_value = "") {
    for (int i = start + 1; i < argc; ++i) {
        if (argv[i] == name) {
            if (i + 1 >= argc) {
                throw std::runtime_error("missing value for " + std::string(name));
            }
            return argv[i + 1];
        }
    }
    return default_value;
}

std::string RequiredOption(int argc, char** argv, int start, std::string_view name) {
    std::string value = OptionValue(argc, argv, start, name);
    if (value.empty()) {
        throw std::runtime_error("missing required option " + std::string(name));
    }
    return value;
}

std::uint64_t NowTimestamp() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

int Connect(const CliOptions& options) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error("socket: " + std::string(std::strerror(errno)));
    }
#ifdef SO_NOSIGPIPE
    int enabled = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(options.port);
    if (::inet_pton(AF_INET, options.host.c_str(), &addr.sin_addr) != 1) {
        ::close(fd);
        throw std::runtime_error("host must be an IPv4 address");
    }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        throw std::runtime_error("connect: " + std::string(std::strerror(errno)));
    }
    return fd;
}

void WriteAll(int fd, const std::vector<std::byte>& bytes) {
    std::size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::send(fd, bytes.data() + written, bytes.size() - written, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error("send: " + std::string(std::strerror(errno)));
        }
        written += static_cast<std::size_t>(n);
    }
}

void ReadExact(int fd, std::byte* data, std::size_t size) {
    std::size_t read = 0;
    while (read < size) {
        const ssize_t n = ::recv(fd, data + read, size - read, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw std::runtime_error("recv: " + std::string(std::strerror(errno)));
        }
        if (n == 0) {
            throw std::runtime_error("connection closed while reading response");
        }
        read += static_cast<std::size_t>(n);
    }
}

std::uint32_t ReadFrameLength(const std::array<std::byte, 4>& bytes) {
    std::uint32_t value = 0;
    for (std::byte byte : bytes) {
        value <<= 8U;
        value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(byte));
    }
    return value;
}

logmq::ResponseEnvelope RoundTrip(const CliOptions& options,
                                  const logmq::RequestEnvelope& request) {
    auto encoded = logmq::EncodeRequest(request);
    if (!encoded.ok()) {
        throw std::runtime_error(encoded.status().ToString());
    }

    const int fd = Connect(options);
    WriteAll(fd, encoded.value());

    std::array<std::byte, 4> header{};
    ReadExact(fd, header.data(), header.size());
    const std::uint32_t frame_len = ReadFrameLength(header);
    if (frame_len < logmq::kFrameHeaderBytes || frame_len > logmq::kMaxFrameBytes) {
        ::close(fd);
        throw std::runtime_error("invalid response frame length");
    }

    std::vector<std::byte> frame(frame_len);
    std::copy(header.begin(), header.end(), frame.begin());
    ReadExact(fd, frame.data() + header.size(), frame.size() - header.size());
    ::close(fd);

    auto response = logmq::DecodeResponse(std::span<const std::byte>(frame.data(), frame.size()));
    if (!response.ok()) {
        throw std::runtime_error(response.status().ToString());
    }
    return std::move(response).value();
}

logmq::RequestEnvelope BuildRequest(int argc, char** argv, const CliOptions& options) {
    logmq::RequestEnvelope envelope;
    envelope.version = logmq::kProtocolVersion;
    envelope.request_id = NowTimestamp();

    if (options.command == "create-topic") {
        envelope.api_key = logmq::ApiKey::kCreateTopic;
        logmq::CreateTopicRequest request;
        request.topic = RequiredOption(argc, argv, options.command_index, "--topic");
        request.partition_count = static_cast<std::uint32_t>(
            std::stoul(RequiredOption(argc, argv, options.command_index, "--partitions")));
        envelope.body = logmq::RequestBody{std::in_place_type<logmq::CreateTopicRequest>,
                                           std::move(request)};
    } else if (options.command == "metadata") {
        envelope.api_key = logmq::ApiKey::kMetadata;
        logmq::MetadataRequest request;
        request.topic = OptionValue(argc, argv, options.command_index, "--topic");
        envelope.body = logmq::RequestBody{std::in_place_type<logmq::MetadataRequest>,
                                           std::move(request)};
    } else if (options.command == "produce") {
        envelope.api_key = logmq::ApiKey::kProduce;
        logmq::ProduceRequest request;
        request.topic = RequiredOption(argc, argv, options.command_index, "--topic");
        const std::string partition = OptionValue(argc, argv, options.command_index, "--partition");
        request.partition_id = partition.empty()
                                   ? logmq::kInvalidPartitionId
                                   : static_cast<logmq::PartitionId>(std::stoi(partition));
        const std::string timestamp = OptionValue(argc, argv, options.command_index, "--timestamp");
        request.records.push_back(logmq::ProtocolRecord{
            timestamp.empty() ? NowTimestamp() : std::stoull(timestamp),
            RequiredOption(argc, argv, options.command_index, "--key"),
            RequiredOption(argc, argv, options.command_index, "--value")});
        envelope.body = logmq::RequestBody{std::in_place_type<logmq::ProduceRequest>,
                                           std::move(request)};
    } else if (options.command == "fetch") {
        envelope.api_key = logmq::ApiKey::kFetch;
        logmq::FetchRequest request;
        request.topic = RequiredOption(argc, argv, options.command_index, "--topic");
        request.partition_id = static_cast<logmq::PartitionId>(
            std::stoi(RequiredOption(argc, argv, options.command_index, "--partition")));
        request.offset = static_cast<logmq::Offset>(
            std::stoll(RequiredOption(argc, argv, options.command_index, "--offset")));
        request.max_bytes = static_cast<std::uint32_t>(
            std::stoul(OptionValue(argc, argv, options.command_index, "--max-bytes", "4096")));
        envelope.body = logmq::RequestBody{std::in_place_type<logmq::FetchRequest>,
                                           std::move(request)};
    } else {
        throw std::runtime_error("unknown command: " + options.command);
    }

    return envelope;
}

int PrintResponse(const logmq::ResponseEnvelope& response) {
    if (response.error.error_code != logmq::ProtocolErrorCode::kNone) {
        std::cerr << logmq::ProtocolErrorCodeName(response.error.error_code) << ": "
                  << response.error.message << "\n";
        return 2;
    }

    switch (response.api_key) {
        case logmq::ApiKey::kCreateTopic: {
            const auto& body = std::get<logmq::CreateTopicResponse>(response.body);
            std::cout << "created topic=" << body.topic << " partitions="
                      << body.partition_count << "\n";
            return 0;
        }
        case logmq::ApiKey::kMetadata: {
            const auto& body = std::get<logmq::MetadataResponse>(response.body);
            for (const auto& topic : body.topics) {
                std::cout << "topic=" << topic.topic << " partitions="
                          << topic.partition_count << "\n";
            }
            return 0;
        }
        case logmq::ApiKey::kProduce: {
            const auto& body = std::get<logmq::ProduceResponse>(response.body);
            std::cout << "partition=" << body.partition_id << " base_offset="
                      << body.base_offset << " record_count=" << body.record_count << "\n";
            return 0;
        }
        case logmq::ApiKey::kFetch: {
            const auto& body = std::get<logmq::FetchResponse>(response.body);
            std::cout << "base_offset=" << body.base_offset << " high_watermark="
                      << body.high_watermark << " records=" << body.records.size() << "\n";
            for (std::size_t i = 0; i < body.records.size(); ++i) {
                const auto& record = body.records[i];
                std::cout << "offset=" << body.base_offset + static_cast<logmq::Offset>(i)
                          << " timestamp=" << record.timestamp << " key=" << record.key
                          << " value=" << record.value << "\n";
            }
            return 0;
        }
    }
    return 1;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        CliOptions options = ParseGlobalOptions(argc, argv);
        logmq::RequestEnvelope request = BuildRequest(argc, argv, options);
        logmq::ResponseEnvelope response = RoundTrip(options, request);
        return PrintResponse(response);
    } catch (const std::exception& error) {
        std::cerr << error.what() << "\n";
        PrintUsage();
        return 1;
    }
}
