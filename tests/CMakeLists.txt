find_package(GTest CONFIG QUIET)

if(NOT GTest_FOUND)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(GTEST QUIET IMPORTED_TARGET gtest_main)
    endif()
endif()

if(GTest_FOUND)
    add_executable(logmq_base_test
        unit/base_test.cpp
    )

    target_link_libraries(logmq_base_test
        PRIVATE
        logmq::core
        GTest::gtest_main
    )

    add_executable(logmq_storage_test
        unit/storage_test.cpp
    )

    target_link_libraries(logmq_storage_test
        PRIVATE
        logmq::core
        GTest::gtest_main
    )

    add_executable(logmq_protocol_test
        unit/protocol_test.cpp
    )

    target_link_libraries(logmq_protocol_test
        PRIVATE
        logmq::core
        GTest::gtest_main
    )

    include(GoogleTest)
    gtest_discover_tests(logmq_base_test)
    gtest_discover_tests(logmq_storage_test)
    gtest_discover_tests(logmq_protocol_test)
elseif(TARGET PkgConfig::GTEST)
    add_executable(logmq_base_test
        unit/base_test.cpp
    )

    target_link_libraries(logmq_base_test
        PRIVATE
        logmq::core
        PkgConfig::GTEST
    )

    add_executable(logmq_storage_test
        unit/storage_test.cpp
    )

    target_link_libraries(logmq_storage_test
        PRIVATE
        logmq::core
        PkgConfig::GTEST
    )

    add_executable(logmq_protocol_test
        unit/protocol_test.cpp
    )

    target_link_libraries(logmq_protocol_test
        PRIVATE
        logmq::core
        PkgConfig::GTEST
    )

    include(GoogleTest)
    gtest_discover_tests(logmq_base_test)
    gtest_discover_tests(logmq_storage_test)
    gtest_discover_tests(logmq_protocol_test)
else()
    message(WARNING "GoogleTest not found; building smoke test only.")

    add_executable(logmq_smoke_test
        unit/smoke_test.cpp
    )

    target_link_libraries(logmq_smoke_test
        PRIVATE
        logmq::core
    )

    add_test(NAME logmq_smoke_test COMMAND logmq_smoke_test)
endif()
