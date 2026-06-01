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
    src/storage/crc32.cpp
    src/storage/file.cpp
    src/storage/record.cpp
    src/storage/record_batch.cpp
    src/storage/segment.cpp
)

add_library(logmq::core ALIAS logmq_core)

target_include_directories(logmq_core
    PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(logmq_core PUBLIC cxx_std_20)

if(MSVC)
    target_compile_options(logmq_core PRIVATE /W4 /permissive-)
else()
    target_compile_options(logmq_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(LOGMQ_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
