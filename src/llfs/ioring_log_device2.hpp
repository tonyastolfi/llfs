//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DEVICE2_HPP
#define LLFS_IORING_LOG_DEVICE2_HPP

#include <llfs/basic_ring_buffer_log_device.hpp>
#include <llfs/ioring_log_device_storage.hpp>
#include <llfs/log_device.hpp>
#include <llfs/packed_page_user_slot.hpp>  // for PackedSlotOffset
#include <llfs/ring_buffer.hpp>

#include <batteries/async/watch.hpp>

#include <chrono>
#include <thread>

namespace llfs {

class IoRingLogDevice2Factory;

struct PackedLogControlBlock2 {
  static constexpr u64 kMagic = 0x128095f84cfba8b0ull;

  big_u64 magic;
  little_i64 data_offset;
  little_i64 data_size;
  PackedSlotOffset trim_pos;
  PackedSlotOffset flush_pos;
  little_u32 control_block_size;
  little_u32 control_header_size;
};

struct IoRingLogConfig2 {
  usize capacity;
  i64 control_block_offset;
  usize control_block_size;
};

template <typename StorageT>
class IoRingLogStorageDriver2 /*Impl*/
{
 public:
  explicit IoRingLogStorageDriver2(
      LogStorageDriverContext& context,  //
      const IoRingLogConfig2& config,    //
      //                                   batt::TaskScheduler& task_scheduler,  //
      StorageT&& storage  //
      ) noexcept
      : context_{context}
      , config_{config}  //      , task_scheduler_{task_scheduler}
      , storage_{std::move(storage)}
  {
  }

  //----

  Status set_trim_pos(slot_offset_type trim_pos)
  {
    this->target_trim_pos_.set_value(trim_pos);

    return OkStatus();
  }

  slot_offset_type get_trim_pos() const
  {
    return this->trim_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_trim_pos(slot_offset_type min_offset)
  {
    return await_slot_offset(min_offset, this->trim_pos_);
  }

  //----

  slot_offset_type get_flush_pos() const
  {
    return this->flush_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_flush_pos(slot_offset_type min_offset)
  {
    return await_slot_offset(min_offset, this->flush_pos_);
  }

  //----

  Status set_commit_pos(slot_offset_type commit_pos)
  {
    this->commit_pos_.set_value(commit_pos);
    return OkStatus();
  }

  slot_offset_type get_commit_pos() const
  {
    return this->commit_pos_.get_value();
  }

  StatusOr<slot_offset_type> await_commit_pos(slot_offset_type min_offset)
  {
    return await_slot_offset(min_offset, this->commit_pos_);
  }

  //----

  Status open()
  {
    LLFS_DVLOG(1) << "open()";

    BATT_REQUIRE_OK(this->storage_.register_fd());

    this->init_control_block();

    this->storage_.on_work_started();
    this->io_thread_.emplace([this] {
      this->io_thread_main();
    });

    this->storage_.post_to_event_loop([this](auto&&... /*ignored*/) {
      LLFS_DVLOG(1) << "initial event";

      this->poll(this->commit_pos_.get_value(), this->target_trim_pos_.get_value());
    });

    return OkStatus();
  }

  Status close()
  {
    this->halt();
    this->join();

    return OkStatus();
  }

  void halt()
  {
    this->target_trim_pos_.close();
    this->trim_pos_.close();
    this->flush_pos_.close();
    this->commit_pos_.close();
    this->storage_.on_work_finished();
  }

  void join()
  {
    // Nothing to do.
    if (this->io_thread_) {
      this->io_thread_->join();
      this->io_thread_ = None;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void io_thread_main()
  {
    typename StorageT::EventLoopTask task{this->storage_, "io_thread_main"};
    task.join();
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void init_control_block()
  {
    auto* p_control_block =
        reinterpret_cast<PackedLogControlBlock2*>(std::addressof(this->control_block_memory_));

    std::memset(p_control_block, 0, sizeof(this->control_block_memory_));

    p_control_block->magic = PackedLogControlBlock2::kMagic;
    p_control_block->data_offset =
        this->config_.control_block_offset + this->config_.control_block_size;
    p_control_block->data_size = this->config_.capacity;
    p_control_block->trim_pos = this->trim_pos_.get_value();
    p_control_block->flush_pos = this->flush_pos_.get_value();
    p_control_block->control_header_size = sizeof(PackedLogControlBlock2);
    p_control_block->control_block_size = this->config_.control_block_size;

    this->control_block_ = p_control_block;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void poll(slot_offset_type observed_commit_pos, slot_offset_type observed_target_trim_pos)
  {
    if (slot_less_than(this->started_flush_upper_bound_, observed_commit_pos)) {
      usize n = observed_commit_pos - this->started_flush_upper_bound_;
      if ((n >= 2 * 1024 * 1024 || this->writes_pending_ == 0) && this->writes_pending_ < 256) {
        this->start_flush(SlotRange{
            .lower_bound = this->started_flush_upper_bound_,
            .upper_bound = observed_commit_pos,
        });
      }
    }

    this->start_control_block_update();
    this->wait_for_target_trim_pos(observed_target_trim_pos);
    this->wait_for_commit_pos(observed_commit_pos);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void wait_for_target_trim_pos(slot_offset_type observed_target_trim_pos)
  {
    if (this->waiting_on_target_trim_pos_) {
      return;
    }

    this->waiting_on_target_trim_pos_ = true;

    this->target_trim_pos_.async_wait(
        observed_target_trim_pos, [this](StatusOr<slot_offset_type> new_target_trim_pos) {
          this->storage_.post_to_event_loop([this, new_target_trim_pos](auto&&... /*ignored*/) {
            this->waiting_on_target_trim_pos_ = false;

            if (!new_target_trim_pos.ok()) {
              this->context_.update_error_status(new_target_trim_pos.status());
              return;
            }

            this->poll(this->commit_pos_.get_value(), *new_target_trim_pos);
          });
        });
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void wait_for_commit_pos(slot_offset_type observed_commit_pos)
  {
    if (this->waiting_on_commit_pos_) {
      return;
    }

    this->waiting_on_commit_pos_ = true;

    LLFS_VLOG(1) << "wait_for_commit_pos;" << BATT_INSPECT(observed_commit_pos)
                 << BATT_INSPECT(this->started_flush_upper_bound_);

    this->commit_pos_.async_wait(
        observed_commit_pos, [this](StatusOr<slot_offset_type> new_commit_pos) {
          this->storage_.post_to_event_loop([this, new_commit_pos](auto&&... /*ignored*/) {
            this->waiting_on_commit_pos_ = false;

            if (!new_commit_pos.ok()) {
              this->context_.update_error_status(new_commit_pos.status());
              return;
            }

            this->poll(*new_commit_pos, this->target_trim_pos_.get_value());
          });
        });
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  SlotRange get_aligned_range(const SlotRange& slot_range) const noexcept
  {
    return SlotRange{
        .lower_bound = batt::round_down_bits(this->device_page_size_log2_, slot_range.lower_bound),
        .upper_bound = batt::round_up_bits(this->device_page_size_log2_, slot_range.upper_bound),
    };
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  SlotRange get_aligned_tail(const SlotRange& aligned_range) const noexcept
  {
    return SlotRange{
        .lower_bound = aligned_range.upper_bound - this->device_page_size_,
        .upper_bound = aligned_range.upper_bound,
    };
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  bool start_flush(const SlotRange& slot_range)
  {
    LLFS_VLOG(1) << "start_flush(" << slot_range << ") size=" << slot_range.size();

    BATT_CHECK(!slot_range.empty());

    SlotRange aligned_range = this->get_aligned_range(slot_range);

    // If this flush would overlap with an ongoing one (at the last device page) then trim the
    // aligned_range so it doesn't.
    //
    if (this->flush_tail_) {
      if (slot_less_than(aligned_range.lower_bound, this->flush_tail_->upper_bound)) {
        aligned_range.lower_bound = this->flush_tail_->upper_bound;
        if (aligned_range.empty()) {
          return false;
        }
      }
    }

    // Replace the current flush_tail_ slot range.
    //
    const SlotRange new_flush_tail = this->get_aligned_tail(aligned_range);
    if (this->flush_tail_) {
      BATT_CHECK_NE(new_flush_tail, *this->flush_tail_);
    }
    BATT_CHECK(!new_flush_tail.empty());
    this->flush_tail_.emplace(new_flush_tail);

    // Start writing!
    //
    this->start_write(slot_range, aligned_range);

    return true;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void start_write(const SlotRange& slot_range, const SlotRange& aligned_range) noexcept
  {
    this->started_flush_upper_bound_ =
        slot_max(this->started_flush_upper_bound_, slot_range.upper_bound);

    const i64 data_start = this->config_.control_block_offset + this->config_.control_block_size;
    const i64 write_offset = data_start + (aligned_range.lower_bound % this->config_.capacity);

    const usize write_size = aligned_range.size();

    ConstBuffer buffer =
        resize_buffer(this->context_.buffer_.get(aligned_range.lower_bound), write_size);

    ++this->writes_pending_;
    this->writes_max_ = std::max(this->writes_max_, this->writes_pending_);

    LLFS_VLOG(1) << "start_write(offset=" << data_start << "+" << (write_offset - data_start)
                 << ", size=" << buffer.size() << ")" << BATT_INSPECT(slot_range)
                 << BATT_INSPECT(aligned_range) << BATT_INSPECT(this->writes_pending_)
                 << BATT_INSPECT(this->writes_max_);

    this->storage_.async_write_some(write_offset, buffer,
                                    [this, slot_range, aligned_range](StatusOr<i32> result) {
                                      --this->writes_pending_;
                                      this->handle_flush_result(slot_range, aligned_range, result);
                                    });
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void handle_flush_result(const SlotRange& slot_range, const SlotRange& aligned_range,
                           StatusOr<i32> result)
  {
    LLFS_VLOG(1) << "handle_flush_result(result=" << result << ")" << BATT_INSPECT(slot_range);

    SlotRange aligned_tail = this->get_aligned_tail(aligned_range);
    SlotRange flushed_range{
        .lower_bound = slot_max(slot_range.lower_bound, aligned_range.lower_bound),
        .upper_bound = slot_min(aligned_range.lower_bound + (result.ok() ? *result : 0),
                                slot_range.upper_bound),
    };

    const bool is_tail = (this->flush_tail_ && *this->flush_tail_ == aligned_tail);
    LLFS_DVLOG(1) << BATT_INSPECT(is_tail);
    if (is_tail) {
      this->flush_tail_ = None;
      this->started_flush_upper_bound_ = flushed_range.upper_bound;
    }

    LLFS_DVLOG(1) << BATT_INSPECT(flushed_range);

    if (!result.ok()) {
      this->context_.update_error_status(result.status());
      return;
    }

    BATT_CHECK(!flushed_range.empty());

    this->update_known_flush_pos(flushed_range);

    const slot_offset_type observed_commit_pos = this->commit_pos_.get_value();

    if (!is_tail) {
      SlotRange updated_range{
          .lower_bound = flushed_range.upper_bound,
          .upper_bound = slot_min(aligned_range.upper_bound, observed_commit_pos),
      };

      if (!updated_range.empty()) {
        this->start_write(updated_range, this->get_aligned_range(updated_range));
      }
    }

    this->poll(observed_commit_pos, this->target_trim_pos_.get_value());
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void update_known_flush_pos(const SlotRange& flushed_range) noexcept
  {
    LLFS_DVLOG(1) << "update_known_flush_pos(" << flushed_range << ")"
                  << BATT_INSPECT(this->known_flush_pos_) << " (before)";

    this->flushed_ranges_.emplace_back(flushed_range);
    std::push_heap(this->flushed_ranges_.begin(), this->flushed_ranges_.end(), SlotRangePriority{});

    while (!this->flushed_ranges_.empty()) {
      SlotRange& next_range = this->flushed_ranges_.front();

      if (next_range.lower_bound == this->known_flush_pos_) {
        this->known_flush_pos_ = next_range.upper_bound;
        std::pop_heap(this->flushed_ranges_.begin(), this->flushed_ranges_.end(),
                      SlotRangePriority{});
        this->flushed_ranges_.pop_back();
        continue;
      }

      LLFS_CHECK_SLOT_LT(this->known_flush_pos_, next_range.lower_bound);
      break;
    }

    LLFS_DVLOG(1) << BATT_INSPECT(this->known_flush_pos_) << " (after)";
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  bool start_control_block_update() noexcept
  {
    if (this->writing_control_block_) {
      LLFS_DVLOG(1) << "start_control_block_update() - already flushing";
      return false;
    }

    LLFS_DVLOG(1) << "start_control_block_update() - checking for updates";

    BATT_CHECK_NOT_NULLPTR(this->control_block_);

    slot_offset_type observed_target_trim_pos = this->target_trim_pos_.get_value();
    slot_offset_type observed_trim_pos = this->trim_pos_.get_value();
    slot_offset_type observed_flush_pos = this->flush_pos_.get_value();

    if (observed_target_trim_pos != observed_trim_pos ||
        observed_flush_pos != this->known_flush_pos_) {
      LLFS_DVLOG(1) << "start_control_block_update(): trim=" << observed_trim_pos << "->"
                    << observed_target_trim_pos << " flush=" << observed_flush_pos << "->"
                    << this->known_flush_pos_;

      this->control_block_->trim_pos = observed_target_trim_pos;
      this->control_block_->flush_pos = this->known_flush_pos_;
      this->writing_control_block_ = true;

      this->storage_.async_write_some(this->config_.control_block_offset,
                                      ConstBuffer{std::addressof(this->control_block_memory_),
                                                  sizeof(this->control_block_memory_)},
                                      [this](StatusOr<i32> result) {
                                        this->handle_control_block_update(result);
                                      });

      return true;
    }
    return false;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void handle_control_block_update(StatusOr<i32> result) noexcept
  {
    LLFS_DVLOG(1) << "handle_control_block_update(" << result << ")";

    BATT_CHECK(this->writing_control_block_);

    this->writing_control_block_ = false;

    if (!result.ok()) {
      this->context_.update_error_status(result.status());
      return;
    }

    const slot_offset_type old_trim_pos =  //
        this->trim_pos_.set_value(this->control_block_->trim_pos);

    const slot_offset_type old_flush_pos =  //
        this->flush_pos_.set_value(this->control_block_->flush_pos);

    LLFS_VLOG(1) << "handle_control_block_update(): trim=" << old_trim_pos << "->"
                 << this->control_block_->trim_pos << " flush=" << old_flush_pos << "->"
                 << this->control_block_->flush_pos << BATT_INSPECT(this->writes_pending_)
                 << BATT_INSPECT(this->writes_max_);

    this->poll(this->commit_pos_.get_value(), this->target_trim_pos_.get_value());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  LogStorageDriverContext& context_;
  const IoRingLogConfig2 config_;
  //batt::TaskScheduler& task_scheduler_;
  StorageT storage_;
  i32 device_page_size_log2_ = 9 /*== log_2(512)*/;
  usize device_page_size_ = (1 << this->device_page_size_log2_);
  usize capacity_;

  batt::Watch<slot_offset_type> target_trim_pos_{0};
  batt::Watch<slot_offset_type> trim_pos_{0};
  batt::Watch<slot_offset_type> flush_pos_{0};
  batt::Watch<slot_offset_type> commit_pos_{0};

  slot_offset_type known_flush_pos_ = this->flush_pos_.get_value();
  slot_offset_type started_flush_upper_bound_ = this->known_flush_pos_;

  Optional<SlotRange> flush_tail_;

  bool waiting_on_target_trim_pos_ = false;
  bool waiting_on_commit_pos_ = false;
  bool writing_control_block_ = false;

  usize writes_pending_ = 0;
  usize writes_max_ = 0;

  std::aligned_storage_t<4096, 512> control_block_memory_;

  PackedLogControlBlock2* control_block_ = nullptr;

  std::vector<SlotRange> flushed_ranges_;

  Optional<std::thread> io_thread_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief An emphemeral LogDevice that stores data in memory only.
 *
 * The commit pos and flush pos are always in sync, so there is no difference between
 * LogReadMode::kSpeculative and LogReadMode::kDurable for this log device type.
 */
class IoRingLogDevice2
    : public BasicRingBufferLogDevice<
          /*Impl=*/IoRingLogStorageDriver2<DefaultIoRingLogDeviceStorage>>
{
 public:
  using Super = BasicRingBufferLogDevice<
      /*Impl=*/IoRingLogStorageDriver2<DefaultIoRingLogDeviceStorage>>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingLogDevice2(const IoRingLogConfig2& config,  //batt::TaskScheduler& task_scheduler,
                            DefaultIoRingLogDeviceStorage&& storage) noexcept
      : Super{RingBuffer::TempFile{.byte_size = config.capacity}, config,  //task_scheduler,
              std::move(storage)}
  {
  }
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A factory that produces IoRingLogDevice2 instances of the given size.
 */
class IoRingLogDevice2Factory : public LogDeviceFactory
{
 public:
  explicit IoRingLogDevice2Factory(slot_offset_type size) noexcept;

  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override;

 private:
  slot_offset_type size_;
};

}  // namespace llfs

#endif  // LLFS_IORING_LOG_DEVICE2_HPP
