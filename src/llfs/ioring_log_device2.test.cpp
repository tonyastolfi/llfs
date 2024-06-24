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
#include <llfs/storage_simulation.hpp>

namespace {

using namespace llfs::constants;
using namespace llfs::int_types;

using llfs::Interval;
using llfs::None;
using llfs::Status;
using llfs::StatusOr;

// Test Plan:
//   - Use StorageSimulation to create an IoRingLogDevice2 with SimulatedLogDeviceStorage
//   - Choose random operations:
//     - append
//     - trim
//   - Crash the simulation at a predetermined number of steps; when log is recovered, verify that
//   the confirmed trim and flush never go backwards, and that all confirmed flushed data can be
//   read.

TEST(IoringLogDevice2Test, Simulation)
{
  const std::string kTestStorageName = "test_storage";

  const usize kNumSeeds = 1000 * 1000;
  const u64 kTestLogSize = 16 * kKiB;
  const i64 kTestLogBegin = 7 * 4 * kKiB;
  const i64 kTestLogEnd = kTestLogBegin + 4 * kKiB /*control block (aligned)*/ + kTestLogSize;
  const usize kTestMinSlotSize = 50;
  const usize kTestMaxSlotSize = 1500;
  const usize kNumSlots = 50;

  const auto log_config = llfs::IoRingLogConfig2{
      .control_block_offset = kTestLogBegin,
      .log_capacity = kTestLogSize,
      .device_page_size_log2 = 9,
      .data_alignment_log2 = 12,
  };

  usize partial_slot_flush_count = 0;

  for (usize seed = 0; seed < kNumSeeds; ++seed) {
    std::mt19937 rng{seed};

    llfs::StorageSimulation sim{batt::StateMachineEntropySource{
        /*entropy_fn=*/[&rng](usize min_value, usize max_value) -> usize {
          std::uniform_int_distribution<usize> pick_value{min_value, max_value};
          return pick_value(rng);
        }}};

    {
      llfs::SimulatedLogDeviceStorage storage = sim.get_log_device_storage(
          kTestStorageName, /*capacity=*/kTestLogSize, Interval<i64>{kTestLogBegin, kTestLogEnd});

      ASSERT_FALSE(storage.is_initialized());

      Status init_status = llfs::initialize_log_device2(*storage.get_raw_block_file(), log_config);

      ASSERT_TRUE(init_status.ok()) << BATT_INSPECT(init_status);

      storage.set_initialized(true);
    }

    auto main_task_fn = [&] {
      std::deque<llfs::SlotRange> appended_slots;
      std::deque<llfs::SlotRange> maybe_trimmed_slots;
      std::deque<llfs::SlotRange> maybe_flushed_slots;
      llfs::slot_offset_type observed_trim = 0;
      llfs::slot_offset_type observed_flush = 0;
      llfs::slot_offset_type observed_slot_flush = 0;
      {
        llfs::SimulatedLogDeviceStorage storage =
            sim.get_log_device_storage(kTestStorageName, None, None);

        ASSERT_TRUE(storage.is_initialized());

        llfs::BasicIoRingLogDevice2<llfs::SimulatedLogDeviceStorage> log_device{
            log_config, llfs::LogDeviceRuntimeOptions::with_default_values(), std::move(storage)};

        BATT_CHECK_OK(log_device.open());

        auto on_scope_exit = batt::finally([&] {
          log_device.halt();
          log_device.join();
        });

        llfs::LogDevice::Writer& log_writer = log_device.writer();
        std::uniform_int_distribution<usize> pick_slot_size{kTestMinSlotSize, kTestMaxSlotSize};

        usize total_bytes_appended = 0;

        sim.set_inject_failures_mode((seed % 2) == 1);

        LLFS_LOG_INFO_EVERY_N(1000) << BATT_INSPECT(seed);

        for (usize i = 0; i < kNumSlots; ++i) {
          LLFS_VLOG(1) << "Writing slot " << i << " of " << kNumSlots << ";"
                       << BATT_INSPECT(total_bytes_appended) << BATT_INSPECT(seed);

          const usize payload_size = pick_slot_size(rng);
          const usize header_size = llfs::packed_sizeof_varint(payload_size);
          const usize slot_size = header_size + payload_size;

          u64 observed_space = log_writer.space();
          while (observed_space < slot_size && !appended_slots.empty()) {
            const llfs::SlotRange trimmed_slot = appended_slots.front();

            appended_slots.pop_front();
            maybe_trimmed_slots.push_back(trimmed_slot);

            Status trim_status = log_device.trim(trimmed_slot.upper_bound);
            if (!trim_status.ok()) {
              break;
            }

            Status await_status = log_writer.await(llfs::BytesAvailable{
                .size = trimmed_slot.size() + observed_space,
            });
            if (!await_status.ok()) {
              break;
            }

            llfs::SlotRange slot_range = log_device.slot_range(llfs::LogReadMode::kDurable);
            while (!maybe_trimmed_slots.empty() &&
                   llfs::slot_less_than(maybe_trimmed_slots.front().lower_bound,
                                        slot_range.lower_bound)) {
              LLFS_CHECK_SLOT_LE(maybe_trimmed_slots.front().upper_bound, slot_range.lower_bound);
              observed_space += maybe_trimmed_slots.front().size();
              maybe_trimmed_slots.pop_front();
            }
          }

          // Update the observed trim/flush pointers.
          {
            llfs::SlotRange slot_range = log_device.slot_range(llfs::LogReadMode::kDurable);

            ASSERT_FALSE(llfs::slot_less_than(slot_range.lower_bound, observed_trim))
                << BATT_INSPECT(slot_range) << BATT_INSPECT(observed_trim);

            ASSERT_FALSE(llfs::slot_less_than(slot_range.upper_bound, observed_flush))
                << BATT_INSPECT(slot_range) << BATT_INSPECT(observed_flush);

            observed_trim = slot_range.lower_bound;
            observed_flush = slot_range.upper_bound;

            while (!maybe_flushed_slots.empty() &&
                   !llfs::slot_less_than(observed_flush, maybe_flushed_slots.front().upper_bound)) {
              observed_slot_flush = maybe_flushed_slots.front().upper_bound;
              maybe_flushed_slots.pop_front();
            }
          }

          // If waiting for the required space failed, then exit the loop.
          //
          if (observed_space < slot_size) {
            break;
          }

          llfs::slot_offset_type slot_offset = log_writer.slot_offset();
          StatusOr<llfs::MutableBuffer> buffer = log_writer.prepare(slot_size);
          BATT_CHECK_OK(buffer);

          *buffer = batt::get_or_panic(llfs::pack_varint_to(*buffer, payload_size));
          std::memset(buffer->data(), 'a' + (slot_offset % 26), buffer->size());

          StatusOr<llfs::slot_offset_type> end_offset = log_writer.commit(slot_size);
          BATT_CHECK_OK(end_offset);
          BATT_CHECK_EQ(slot_offset + slot_size, *end_offset);

          appended_slots.push_back(llfs::SlotRange{
              .lower_bound = slot_offset,
              .upper_bound = *end_offset,
          });

          maybe_flushed_slots.push_back(appended_slots.back());

          total_bytes_appended += slot_size;
        }
      }

      sim.crash_and_recover();

      sim.set_inject_failures_mode(false);

      {
        llfs::SimulatedLogDeviceStorage storage =
            sim.get_log_device_storage(kTestStorageName, None, None);

        ASSERT_TRUE(storage.is_initialized());

        llfs::BasicIoRingLogDevice2<llfs::SimulatedLogDeviceStorage> log_device{
            log_config, llfs::LogDeviceRuntimeOptions::with_default_values(), std::move(storage)};

        BATT_CHECK_OK(log_device.open());

        auto on_scope_exit = batt::finally([&] {
          log_device.halt();
          log_device.join();
        });

        llfs::SlotRange slot_range = log_device.slot_range(llfs::LogReadMode::kDurable);

        ASSERT_LE(slot_range.size(), kTestLogSize) << BATT_INSPECT(slot_range);

        ASSERT_FALSE(llfs::slot_less_than(slot_range.lower_bound, observed_trim))
            << BATT_INSPECT(slot_range) << BATT_INSPECT(observed_trim) << BATT_INSPECT(seed);

        ASSERT_FALSE(llfs::slot_less_than(slot_range.upper_bound, observed_slot_flush))
            << BATT_INSPECT(slot_range) << BATT_INSPECT(observed_slot_flush);

        if (llfs::slot_less_than(slot_range.upper_bound, observed_flush)) {
          partial_slot_flush_count += 1;
        }

        // Remove any actually trimmed slots from the maybe_trimmed list.
        //
        while (
            !maybe_trimmed_slots.empty() &&
            llfs::slot_less_than(maybe_trimmed_slots.front().lower_bound, slot_range.lower_bound)) {
          LLFS_CHECK_SLOT_LE(maybe_trimmed_slots.front().upper_bound, slot_range.lower_bound);
          maybe_trimmed_slots.pop_front();
        }

        // Move any remaining maybe trimmed slots to the front of the appended list.
        //
        while (!maybe_trimmed_slots.empty()) {
          appended_slots.push_front(maybe_trimmed_slots.back());
          maybe_trimmed_slots.pop_back();
        }

        // Verify all recovered slots.
        //
        std::unique_ptr<llfs::LogDevice::Reader> log_reader =
            log_device.new_reader(/*slot_lower_bound=*/None, llfs::LogReadMode::kDurable);

        llfs::SlotReader slot_reader{*log_reader};

        StatusOr<usize> n_parsed = slot_reader.run(
            batt::WaitForResource::kFalse, [&](const llfs::SlotParse& slot) -> Status {
              BATT_CHECK_EQ(slot.offset, appended_slots.front());
              appended_slots.pop_front();

              // Verify the data.
              //
              const char expected_ch = 'a' + (slot.offset.lower_bound % 26);
              for (char actual_ch : slot.body) {
                EXPECT_EQ(expected_ch, actual_ch);
                if (actual_ch != expected_ch) {
                  LLFS_LOG_INFO() << BATT_INSPECT(slot.offset) << BATT_INSPECT(slot_range)
                                  << BATT_INSPECT(observed_trim) << BATT_INSPECT(observed_flush);
                  break;
                }
              }

              return batt::OkStatus();
            });

        ASSERT_TRUE(n_parsed.ok()) << BATT_INSPECT(n_parsed);

        // If there are any unmatched slots, then assert they are in the unflushed range.
        //
        if (!appended_slots.empty()) {
          ASSERT_GE(appended_slots.front().lower_bound, slot_range.upper_bound);
        }
      }
    };

    ASSERT_NO_FATAL_FAILURE(sim.run_main_task(main_task_fn));
  }

  EXPECT_GT(partial_slot_flush_count, 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST(IoringLogDevice2Test, Benchmark)
{
  llfs::run_log_device_benchmark([&](usize log_size, bool create, auto&& workload_fn) {
    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Set configuration and options.
    //
    llfs::IoRingLogConfig2 config{
        .control_block_offset = 0,
        .log_capacity = log_size,
        .device_page_size_log2 = 9,
        .data_alignment_log2 = 12,
    };

    llfs::LogDeviceRuntimeOptions options{
        .name = "test log",
        .flush_delay_threshold = 2 * kMiB,
        .max_concurrent_writes = 64,
    };

    const char* file_name =  //
        std::getenv("LLFS_LOG_DEVICE_FILE");

    if (!file_name) {
      LLFS_LOG_INFO() << "LLFS_LOG_DEVICE_FILE not specified; skipping benchmark test";
      return;
    }

    std::cout << "LLFS_LOG_DEVICE_FILE=" << batt::c_str_literal(file_name) << std::endl;

    LLFS_LOG_INFO() << BATT_INSPECT(file_name);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Erase any existing file.
    //
    if (create) {
      {
        std::filesystem::path file_path{file_name};
        std::filesystem::remove_all(file_path);
        ASSERT_FALSE(std::filesystem::exists(file_path));
      }
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Create a new log file and size it to the configured capacity.
    //
    llfs::StatusOr<int> status_or_fd = [&] {
      if (create) {
        return llfs::create_file_read_write(file_name, llfs::OpenForAppend{false});
      } else {
        return llfs::open_file_read_write(file_name, llfs::OpenForAppend{false},
                                          llfs::OpenRawIO{true});
      }
    }();

    ASSERT_TRUE(status_or_fd.ok()) << BATT_INSPECT(status_or_fd);

    const int fd = *status_or_fd;

    if (create) {
      llfs::Status enable_raw_status = llfs::enable_raw_io_fd(fd, true);

      ASSERT_TRUE(enable_raw_status.ok()) << BATT_INSPECT(enable_raw_status);

      llfs::Status truncate_status =
          llfs::truncate_fd(fd, /*size=*/config.control_block_size() + config.log_capacity);

      ASSERT_TRUE(truncate_status.ok());
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Initialize the IoRing and IoRing::File inside a storage object wrapper.
    //
    llfs::StatusOr<llfs::DefaultIoRingLogDeviceStorage> status_or_storage =
        llfs::DefaultIoRingLogDeviceStorage::make_new(llfs::MaxQueueDepth{256}, fd);

    ASSERT_TRUE(status_or_storage.ok()) << BATT_INSPECT(status_or_storage.status());

    llfs::DefaultIoRingLogDeviceStorage& storage = *status_or_storage;

    if (create) {
      llfs::DefaultIoRingLogDeviceStorage::RawBlockFileImpl file{storage};

      //+++++++++++-+-+--+----- --- -- -  -  -   -
      // Write the initial contents of the file.
      //
      llfs::Status init_status = llfs::initialize_log_device2(file, config);

      ASSERT_TRUE(init_status.ok()) << BATT_INSPECT(init_status);
    }

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
