#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>
#include <thread>

#include "logmq/base/config.h"
#include "logmq/broker/broker_service.h"
#include "logmq/net/tcp_server.h"

namespace {

std::atomic<bool> g_stop_requested{false};

void HandleSignal(int) {
    g_stop_requested.store(true);
}

std::string ReadArg(int& index, int argc, char** argv) {
    if (index + 1 >= argc) {
        throw std::runtime_error(std::string("missing value for ") + argv[index]);
    }
    ++index;
    return argv[index];
}

void PrintUsage() {
    std::cerr << "Usage: logmq_broker [--host HOST] [--port PORT] [--data-dir DIR] "
                 "[--io-threads N] [--worker-threads N]\n";
}

}  // namespace

int main(int argc, char** argv) {
    logmq::Config config;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--host") {
                config.broker.host = ReadArg(i, argc, argv);
            } else if (arg == "--port") {
                config.broker.port = static_cast<std::uint16_t>(
                    std::stoul(ReadArg(i, argc, argv)));
            } else if (arg == "--data-dir") {
                config.storage.data_dir = ReadArg(i, argc, argv);
            } else if (arg == "--io-threads") {
                config.network.io_threads = std::stoi(ReadArg(i, argc, argv));
            } else if (arg == "--worker-threads") {
                config.network.worker_threads = std::stoi(ReadArg(i, argc, argv));
            } else if (arg == "--help" || arg == "-h") {
                PrintUsage();
                return 0;
            } else {
                std::cerr << "unknown argument: " << arg << "\n";
                PrintUsage();
                return 1;
            }
        }

        const logmq::Status config_status = config.Validate();
        if (!config_status.ok()) {
            std::cerr << config_status.ToString() << "\n";
            return 1;
        }

        logmq::BrokerService service(logmq::BrokerServiceOptions{config.storage});
        logmq::TcpServer server(logmq::TcpServerOptionsFromConfig(config), &service);
        logmq::Status status = server.Start();
        if (!status.ok()) {
            std::cerr << status.ToString() << "\n";
            return 1;
        }

        std::signal(SIGINT, HandleSignal);
        std::signal(SIGTERM, HandleSignal);
        std::thread watcher([&server] {
            while (!g_stop_requested.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            server.Stop();
        });

        std::cout << "LogMQ broker listening on " << config.broker.host << ":"
                  << server.bound_port() << "\n";
        server.Loop();
        server.Stop();
        status = service.Close();
        if (!status.ok()) {
            std::cerr << status.ToString() << "\n";
        }
        g_stop_requested.store(true);
        if (watcher.joinable()) {
            watcher.join();
        }
        return status.ok() ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << error.what() << "\n";
        PrintUsage();
        return 1;
    }
}
