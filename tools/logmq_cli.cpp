#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>
#include <variant>
#include <vector>

#include "logmq/client/client.h"
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
              << "  fetch --topic TOPIC --partition N --offset OFFSET [--max-bytes N]\n"
              << "  commit-offset --group GROUP --topic TOPIC --partition N --offset N\n"
              << "  fetch-offset --group GROUP --topic TOPIC --partition N\n"
              << "  join-group --group GROUP --topic TOPIC [--member MEMBER]\n"
              << "  sync-group --group GROUP --member MEMBER --generation N\n"
              << "  heartbeat --group GROUP --member MEMBER --generation N\n"
              << "  leave-group --group GROUP --member MEMBER\n";
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

logmq::ClientOptions ToClientOptions(const CliOptions& options) {
    logmq::ClientOptions client;
    client.host = options.host;
    client.port = options.port;
    return client;
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
    } else if (options.command == "commit-offset") {
        envelope.api_key = logmq::ApiKey::kCommitOffset;
        logmq::CommitOffsetRequest request;
        request.group_id = RequiredOption(argc, argv, options.command_index, "--group");
        request.topic = RequiredOption(argc, argv, options.command_index, "--topic");
        request.partition_id = static_cast<logmq::PartitionId>(
            std::stoi(RequiredOption(argc, argv, options.command_index, "--partition")));
        request.offset = static_cast<logmq::Offset>(
            std::stoll(RequiredOption(argc, argv, options.command_index, "--offset")));
        envelope.body = logmq::RequestBody{std::in_place_type<logmq::CommitOffsetRequest>,
                                           std::move(request)};
    } else if (options.command == "fetch-offset") {
        envelope.api_key = logmq::ApiKey::kFetchCommittedOffset;
        logmq::FetchCommittedOffsetRequest request;
        request.group_id = RequiredOption(argc, argv, options.command_index, "--group");
        request.topic = RequiredOption(argc, argv, options.command_index, "--topic");
        request.partition_id = static_cast<logmq::PartitionId>(
            std::stoi(RequiredOption(argc, argv, options.command_index, "--partition")));
        envelope.body =
            logmq::RequestBody{std::in_place_type<logmq::FetchCommittedOffsetRequest>,
                               std::move(request)};
    } else if (options.command == "join-group") {
        envelope.api_key = logmq::ApiKey::kJoinGroup;
        logmq::JoinGroupRequest request;
        request.group_id = RequiredOption(argc, argv, options.command_index, "--group");
        request.topic = RequiredOption(argc, argv, options.command_index, "--topic");
        request.member_id = OptionValue(argc, argv, options.command_index, "--member");
        envelope.body = logmq::RequestBody{std::in_place_type<logmq::JoinGroupRequest>,
                                           std::move(request)};
    } else if (options.command == "sync-group") {
        envelope.api_key = logmq::ApiKey::kSyncGroup;
        logmq::SyncGroupRequest request;
        request.group_id = RequiredOption(argc, argv, options.command_index, "--group");
        request.member_id = RequiredOption(argc, argv, options.command_index, "--member");
        request.generation_id = static_cast<std::int32_t>(
            std::stoi(RequiredOption(argc, argv, options.command_index, "--generation")));
        envelope.body = logmq::RequestBody{std::in_place_type<logmq::SyncGroupRequest>,
                                           std::move(request)};
    } else if (options.command == "heartbeat") {
        envelope.api_key = logmq::ApiKey::kHeartbeat;
        logmq::HeartbeatRequest request;
        request.group_id = RequiredOption(argc, argv, options.command_index, "--group");
        request.member_id = RequiredOption(argc, argv, options.command_index, "--member");
        request.generation_id = static_cast<std::int32_t>(
            std::stoi(RequiredOption(argc, argv, options.command_index, "--generation")));
        envelope.body = logmq::RequestBody{std::in_place_type<logmq::HeartbeatRequest>,
                                           std::move(request)};
    } else if (options.command == "leave-group") {
        envelope.api_key = logmq::ApiKey::kLeaveGroup;
        logmq::LeaveGroupRequest request;
        request.group_id = RequiredOption(argc, argv, options.command_index, "--group");
        request.member_id = RequiredOption(argc, argv, options.command_index, "--member");
        envelope.body = logmq::RequestBody{std::in_place_type<logmq::LeaveGroupRequest>,
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
        case logmq::ApiKey::kCommitOffset: {
            const auto& body = std::get<logmq::CommitOffsetResponse>(response.body);
            std::cout << "group=" << body.group_id << " topic=" << body.topic
                      << " partition=" << body.partition_id << " offset=" << body.offset
                      << "\n";
            return 0;
        }
        case logmq::ApiKey::kFetchCommittedOffset: {
            const auto& body = std::get<logmq::FetchCommittedOffsetResponse>(response.body);
            std::cout << "group=" << body.group_id << " topic=" << body.topic
                      << " partition=" << body.partition_id
                      << " committed=" << (body.committed ? "true" : "false")
                      << " offset=" << body.offset << "\n";
            return 0;
        }
        case logmq::ApiKey::kJoinGroup: {
            const auto& body = std::get<logmq::JoinGroupResponse>(response.body);
            std::cout << "group=" << body.group_id << " member=" << body.member_id
                      << " generation=" << body.generation_id
                      << " leader=" << body.leader_id << "\n";
            return 0;
        }
        case logmq::ApiKey::kSyncGroup: {
            const auto& body = std::get<logmq::SyncGroupResponse>(response.body);
            std::cout << "group=" << body.group_id << " member=" << body.member_id
                      << " generation=" << body.generation_id
                      << " topic=" << body.assignment.topic << " partitions=";
            for (std::size_t i = 0; i < body.assignment.partition_ids.size(); ++i) {
                if (i != 0) {
                    std::cout << ",";
                }
                std::cout << body.assignment.partition_ids[i];
            }
            std::cout << "\n";
            return 0;
        }
        case logmq::ApiKey::kHeartbeat: {
            const auto& body = std::get<logmq::HeartbeatResponse>(response.body);
            std::cout << "group=" << body.group_id << " member=" << body.member_id
                      << " generation=" << body.generation_id << "\n";
            return 0;
        }
        case logmq::ApiKey::kLeaveGroup: {
            const auto& body = std::get<logmq::LeaveGroupResponse>(response.body);
            std::cout << "group=" << body.group_id << " member=" << body.member_id
                      << " generation=" << body.generation_id << "\n";
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
        logmq::ClientConnection connection(ToClientOptions(options));
        auto round_trip = connection.RoundTrip(request);
        if (!round_trip.ok()) {
            throw std::runtime_error(round_trip.status().ToString());
        }
        logmq::ResponseEnvelope response = std::move(round_trip).value();
        return PrintResponse(response);
    } catch (const std::exception& error) {
        std::cerr << error.what() << "\n";
        PrintUsage();
        return 1;
    }
}
