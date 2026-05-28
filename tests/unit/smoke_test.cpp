#include <cstdlib>

#include "logmq/base/config.h"

int main() {
    const logmq::Config config;
    return config.Validate().ok() ? EXIT_SUCCESS : EXIT_FAILURE;
}
