//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_device2.hpp>
//
#include <llfs/ioring_log_device2.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/constants.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/int_types.hpp>
#include <llfs/ioring_log_device.test.hpp>

//#include <batteries/async/runtime.hpp>

namespace {

using namespace llfs::constants;
using namespace llfs::int_types;

TEST(IoringLogDevice2Test, Test)
{
  llfs::run_log_device_benchmark([&](usize log_size, auto&& workload_fn) {
    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Set configuration and options.
    //
    llfs::IoRingLogConfig2 config{
        .control_block_offset = 0,
        .control_block_size = 4096,
        .log_capacity = (i64)log_size,
        .device_page_size_log2 = 12,
    };

    llfs::IoRingLogRuntimeOptions2 options{
        .flush_delay_threshold = 2 * kMiB,
        .max_concurrent_writes = 64,
    };

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Erase any existing file.
    //
    std::string_view test_log_path = "/mnt/optane905p_960_1/llfs_benchmark/log2.llfs";
    {
      std::filesystem::path file_path{test_log_path};
      std::filesystem::remove_all(file_path);
      ASSERT_FALSE(std::filesystem::exists(file_path));
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Create a new log file and size it to the configured capacity.
    //
    llfs::StatusOr<int> status_or_fd =
        llfs::create_file_read_write(test_log_path, llfs::OpenForAppend{false});

    ASSERT_TRUE(status_or_fd.ok()) << BATT_INSPECT(status_or_fd);

    const int fd = *status_or_fd;

    llfs::Status enable_raw_status = llfs::enable_raw_io_fd(fd, true);

    ASSERT_TRUE(enable_raw_status.ok()) << BATT_INSPECT(enable_raw_status);

    llfs::Status truncate_status =
        llfs::truncate_fd(fd, /*size=*/config.control_block_size + config.log_capacity);

    ASSERT_TRUE(truncate_status.ok());

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Initialize the IoRing and IoRing::File inside a storage object wrapper.
    //
    llfs::StatusOr<llfs::DefaultIoRingLogDeviceStorage> status_or_storage =
        llfs::DefaultIoRingLogDeviceStorage::make_new(llfs::MaxQueueDepth{256}, fd);

    ASSERT_TRUE(status_or_storage.ok()) << BATT_INSPECT(status_or_storage.status());

    llfs::DefaultIoRingLogDeviceStorage& storage = *status_or_storage;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Write the initial contents of the file.
    //
    llfs::Status init_status = llfs::initialize_log_device2(storage, config);

    ASSERT_TRUE(init_status.ok()) << BATT_INSPECT(init_status);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Create LogDevice object and open.
    //
    llfs::IoRingLogDevice2 log_device{config, options, std::move(storage)};
    batt::Status open_status = log_device.driver().open();

    ASSERT_TRUE(open_status.ok()) << BATT_INSPECT(open_status);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Run the passed workload.
    //
    workload_fn(log_device);
  });
}

}  // namespace
