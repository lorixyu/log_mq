cmake_minimum_required(VERSION 3.20)

project(
  LogMQ
  VERSION 0.1.0
  DESCRIPTION "A C++20 commit-log based message queue"
  LANGUAGES CXX)

option(LOGMQ_BUILD_TESTS "Build LogMQ tests" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(logmq_core
    src/base/config.cpp
    src/base/status.cpp
    src/broker/broker_service.cpp
    src/broker/offset_store.cpp
    src/broker/topic_manager.cpp
    src/client/client.cpp
    src/net/buffer.cpp
    src/net/channel.cpp
    src/net/event_loop.cpp
    src/net/event_loop_thread_pool.cpp
    src/net/tcp_connection.cpp
    src/net/tcp_server.cpp
    src/net/worker_pool.cpp
    src/protocol/codec.cpp
    src/storage/crc32.cpp
    src/storage/commit_log.cpp
    src/storage/file.cpp
    src/storage/offset_index.cpp
    src/storage/record.cpp
    src/storage/record_batch.cpp
    src/storage/segment.cpp
)

add_library(logmq::core ALIAS logmq_core)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_sources(logmq_core
        PRIVATE
        src/net/poll/poller.cpp
        src/net/poll/epoll_poller.cpp
    )
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin|FreeBSD|OpenBSD|NetBSD")
    target_sources(logmq_core
        PRIVATE
        src/net/poll/poller.cpp
        src/net/poll/kqueue_poller.cpp
    )
endif()

target_include_directories(logmq_core
    PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(logmq_core PUBLIC cxx_std_20)

add_executable(logmq_broker
    src/broker/main.cpp
)

target_link_libraries(logmq_broker
    PRIVATE
    logmq::core
)

add_executable(logmq_cli
    tools/logmq_cli.cpp
)

target_link_libraries(logmq_cli
    PRIVATE
    logmq::core
)

if(MSVC)
    target_compile_options(logmq_core PRIVATE /W4 /permissive-)
else()
    target_compile_options(logmq_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(LOGMQ_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
