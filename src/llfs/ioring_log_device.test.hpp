//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORINT_LOG_DEVICE_TEST_HPP
#define LLFS_IORINT_LOG_DEVICE_TEST_HPP

#include <llfs/int_types.hpp>
#include <llfs/log_device.hpp>

#include <batteries/env.hpp>

#include <cstdlib>
#include <functional>
#include <random>
#include <vector>

namespace llfs {

template <typename T>
T read_test_var(const char* name, T default_value)
{
  using value_type = decltype(default_value);
  const value_type value = batt::getenv_as<value_type>(name).value_or(default_value);
  std::cout << name << "=" << value << std::endl;
  return value;
}

inline void run_log_device_benchmark(
    const std::function<void(usize log_size, const std::function<void(LogDevice& log_device)>&)>&
        inject_log_device)
{
  const usize log_size = read_test_var("LLFS_LOG_DEVICE_SIZE_KB", usize{1024 * 64}) * 1024;
  const usize total_to_write = read_test_var("LLFS_LOG_DEVICE_WRITE_KB", usize{1024 * 1024}) * 1024;
  const usize append_size = read_test_var("LLFS_LOG_DEVICE_APPEND_SIZE", usize{256});
  const usize trim_size = read_test_var("LLFS_LOG_DEVICE_TRIM_SIZE", usize{4 * 1024 * 1024});
  const usize trim_trigger = read_test_var("LLFS_LOG_DEVICE_TRIM_TRIGGER",  //
                                           usize{log_size - trim_size * 2});
  const usize repeat_count = read_test_var("LLFS_LOG_DEVICE_REPEAT", usize{3});

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  for (usize retry = 0; retry < repeat_count; ++retry) {
    inject_log_device(log_size, [&](llfs::LogDevice& log_device) {
      // Generate some random data.
      //
      std::vector<u64> data(32 * 1024 * 1024);
      std::default_random_engine rng{1};
      for (u64& word : data) {
        word = rng();
      }

      auto start = std::chrono::steady_clock::now();

      std::thread writer_thread{[&] {
        llfs::LogDevice::Writer& log_writer = log_device.writer();

        std::uniform_int_distribution<usize> pick_offset{
            0, data.size() - (append_size + sizeof(u64) - 1) / sizeof(u64)};

        usize n_written = 0;
        while (n_written < total_to_write) {
          BATT_CHECK_OK(log_writer.await(llfs::BytesAvailable{.size = append_size}));

          llfs::StatusOr<llfs::MutableBuffer> buffer = log_writer.prepare(append_size);
          BATT_CHECK_OK(buffer);

          std::memcpy(buffer->data(), &data[pick_offset(rng)], buffer->size());

          BATT_CHECK_OK(log_writer.commit(buffer->size()));

          n_written += buffer->size();
        }

        BATT_CHECK_OK(log_device.flush());

        log_device.halt();
      }};

      std::thread trimmer_thread{[&] {
        llfs::SlotRange durable = log_device.slot_range(llfs::LogReadMode::kDurable);

        for (;;) {
          llfs::Status sync_status =
              log_device.sync(llfs::LogReadMode::kDurable,
                              llfs::SlotUpperBoundAt{durable.lower_bound + trim_trigger});

          if (!sync_status.ok()) {
            break;
          }

          llfs::Status trim_status = log_device.trim(durable.lower_bound + trim_size);

          if (!trim_status.ok()) {
            break;
          }

          durable.lower_bound += trim_size;
        }
      }};

      writer_thread.join();
      trimmer_thread.join();
      log_device.join();

      auto finish = std::chrono::steady_clock::now();

      double duration_sec =
          double(std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count()) /
          (1000.0 * 1000.0);

      LLFS_LOG_INFO() << total_to_write << " bytes written in " << duration_sec
                      << " seconds; rate=" << (double(total_to_write) / duration_sec) / 1000000.0
                      << "MB/s";
    });
  }
}

}  //namespace llfs

#endif  // LLFS_IORINT_LOG_DEVICE_TEST_HPP
