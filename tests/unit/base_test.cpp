#include <gtest/gtest.h>

#include <string>

#include "logmq/base/config.h"
#include "logmq/base/result.h"
#include "logmq/base/status.h"
#include "logmq/base/types.h"

namespace logmq {
namespace {

TEST(StatusTest, OkStatusHasStableString) {
    const Status status = Status::Ok();

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::kOk);
    EXPECT_EQ(status.ToString(), "OK");
}

TEST(StatusTest, ErrorStatusCarriesCodeAndMessage) {
    const Status status = Status::InvalidArgument("missing topic");

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::kInvalidArgument);
    EXPECT_EQ(status.message(), "missing topic");
    EXPECT_EQ(status.ToString(), "InvalidArgument: missing topic");
}

TEST(ResultTest, HoldsValueOrStatus) {
    const Result<Offset> offset = Offset{42};
    const Result<Offset> error = Status::NotFound("offset not indexed");

    EXPECT_TRUE(offset.ok());
    EXPECT_EQ(offset.value(), 42);
    EXPECT_FALSE(error.ok());
    EXPECT_EQ(error.status().code(), ErrorCode::kNotFound);
}

TEST(ConfigTest, DefaultConfigIsValid) {
    const Config config;

    EXPECT_TRUE(config.Validate().ok());
    EXPECT_EQ(config.broker.id, 1);
    EXPECT_EQ(config.broker.port, 9092);
    EXPECT_EQ(config.storage.flush_policy, FlushPolicy::kAsync);
}

TEST(ConfigTest, RejectsInvalidValues) {
    Config config;
    config.network.io_threads = 0;

    const Status status = config.Validate();

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::kInvalidArgument);
}

TEST(ConfigTest, ParsesFlushPolicy) {
    const Result<FlushPolicy> sync = ParseFlushPolicy("sync");
    const Result<FlushPolicy> async = ParseFlushPolicy("async");
    const Result<FlushPolicy> invalid = ParseFlushPolicy("sometimes");

    ASSERT_TRUE(sync.ok());
    EXPECT_EQ(sync.value(), FlushPolicy::kSync);
    ASSERT_TRUE(async.ok());
    EXPECT_EQ(async.value(), FlushPolicy::kAsync);
    EXPECT_FALSE(invalid.ok());
}

}  // namespace
}  // namespace logmq
