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

#include <llfs/config.hpp>
//
#include <llfs/basic_ring_buffer_log_device.hpp>
#include <llfs/ioring_log_device_storage.hpp>
#include <llfs/log_device.hpp>
#include <llfs/packed_page_user_slot.hpp>  // for PackedSlotOffset
#include <llfs/ring_buffer.hpp>

#include <batteries/async/watch.hpp>
#include <batteries/math.hpp>
#include <batteries/status.hpp>

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
  little_i32 device_page_size_log2;
};

struct IoRingLogConfig2 {
  i64 control_block_offset;
  i64 control_block_size;
  i64 log_capacity;
  i32 device_page_size_log2;
};

struct IoRingLogRuntimeOptions2 {
  usize flush_delay_threshold;
  usize max_concurrent_writes;
};

/*
TODO [tastolfi 2024-06-11] :

DONE 1. Add code to initialize log device 2 within a storage file
2. Recover state from file
3. Handler buffers for trim/commit wait handlers; pool for flush writes
4. Correctly deal with "split" writes at the end of the ring buffer
DONE 5. Remove extra thread layer (use EventLoopTask directly)

 */

/** \brief Initializes an IoRingLogDevice2 using the given storage device and configuration (which
 * includes offset within the passed device).
 */
template <typename StorageT>
Status initialize_log_device2(StorageT& storage, const IoRingLogConfig2& config);

template <typename StorageT>
class IoRingLogDriver2
{
 public:
  using Self = IoRingLogDriver2;
  using AlignedUnit = std::aligned_storage_t<kLogAtomicWriteSize, kLogAtomicWriteSize>;
  using EventLoopTask = typename StorageT::EventLoopTask;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingLogDriver2(LogStorageDriverContext& context,         //
                            const IoRingLogConfig2& config,           //
                            const IoRingLogRuntimeOptions2& options,  //
                            StorageT&& storage                        //
                            ) noexcept
      : context_{context}
      , config_{config}
      , options_{options}
      , storage_{std::move(storage)}
      , control_block_memory_{new AlignedUnit[(config.control_block_size + sizeof(AlignedUnit) -
                                               1) /
                                              sizeof(AlignedUnit)]}
      , control_block_buffer_{this->control_block_memory_.get(),
                              batt::round_up_bits(this->config_.device_page_size_log2,
                                                  sizeof(PackedLogControlBlock2))}
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

  Status open() noexcept;

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
    if (this->event_loop_task_) {
      this->event_loop_task_->join();
      this->event_loop_task_ = None;
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status read_control_block();
  Status read_log_data();

  /** \brief Forces all slot lower bound pointers to `new_trim_pos`.
   *
   * Resets:
   *  - this->trim_pos_
   *  - this->target_trim_pos_
   *
   * Should only be called during recovery.
   */
  void reset_trim_pos(slot_offset_type new_trim_pos);

  /** \brief Forces all slot upper bound pointers to `new_flush_pos`.
   *
   * Resets:
   *  - this->flush_pos_
   *  - this->commit_pos_
   *  - this->started_flush_upper_bound_
   *  - this->known_flush_pos_
   *
   * Should only be called during recovery.
   */
  void reset_flush_pos(slot_offset_type new_flush_pos);

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void poll(slot_offset_type observed_commit_pos, slot_offset_type observed_target_trim_pos)
  {
    LLFS_VLOG(1) << "poll(" << observed_commit_pos << ", " << observed_target_trim_pos << ")";

    this->start_flush(observed_commit_pos);
    this->start_control_block_update(observed_target_trim_pos);
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

    LLFS_VLOG(1) << "wait_for_target_trim_pos;" << BATT_INSPECT(observed_target_trim_pos);

    this->target_trim_pos_.async_wait(
        observed_target_trim_pos,
        this->make_watch_handler([this](const StatusOr<slot_offset_type>& new_target_trim_pos) {
          LLFS_VLOG(1) << BATT_INSPECT(new_target_trim_pos);

          this->waiting_on_target_trim_pos_ = false;

          if (!new_target_trim_pos.ok()) {
            this->context_.update_error_status(new_target_trim_pos.status());
            return;
          }

          this->poll(this->commit_pos_.get_value(), *new_target_trim_pos);
        }));
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void wait_for_commit_pos(slot_offset_type observed_commit_pos)
  {
    if (this->waiting_on_commit_pos_) {
      return;
    }

    this->waiting_on_commit_pos_ = true;

    this->commit_pos_.async_wait(
        observed_commit_pos,
        this->make_watch_handler([this](const StatusOr<slot_offset_type>& new_commit_pos) {
          this->waiting_on_commit_pos_ = false;

          if (!new_commit_pos.ok()) {
            this->context_.update_error_status(new_commit_pos.status());
            return;
          }

          this->poll(*new_commit_pos, this->target_trim_pos_.get_value());
        }));
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  SlotRange get_aligned_range(const SlotRange& slot_range) const noexcept
  {
    return SlotRange{
        .lower_bound = batt::round_down_bits(this->config_.device_page_size_log2,  //
                                             slot_range.lower_bound),
        .upper_bound = batt::round_up_bits(this->config_.device_page_size_log2,  //
                                           slot_range.upper_bound),
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
  batt::SmallVec<ConstBuffer, 2> get_unflushed_data(slot_offset_type observed_commit_pos)
  {
    batt::SmallVec<ConstBuffer, 2> result;

    if (slot_less_than(this->unflushed_upper_bound_, observed_commit_pos)) {
      usize n = observed_commit_pos - this->unflushed_upper_bound_;
      const usize buffer_offset = this->unflushed_upper_bound_ % this->context_.buffer_.size();
    }

    return result;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void start_flush(slot_offset_type observed_commit_pos)
  {
    slot_offset_type flush_lower_bound = this->unflushed_upper_bound_;

    for (usize repeat = 0; repeat < 2; ++repeat) {
      if (this->writes_pending_ == this->options_.max_concurrent_writes) {
        LLFS_VLOG(1) << "start_flush - at max writes pending";
        break;
      }

      const usize unflushed_size = slot_clamp_distance(flush_lower_bound, observed_commit_pos);
      if (unflushed_size == 0) {
        LLFS_VLOG(1) << "start_flush - unflushed_size == 0";
        break;
      }

      if (this->writes_pending_ != 0 && unflushed_size < this->options_.flush_delay_threshold) {
        LLFS_VLOG(1) << "start_flush - no action taken: " << BATT_INSPECT(unflushed_size)
                     << BATT_INSPECT(this->writes_pending_);
        break;
      }

      SlotRange slot_range{
          .lower_bound = this->unflushed_upper_bound_,
          .upper_bound = observed_commit_pos,
      };

      // Check for split range.
      {
        const usize physical_lower_bound = slot_range.lower_bound % this->context_.buffer_.size();
        const usize physical_upper_bound = slot_range.upper_bound % this->context_.buffer_.size();

        if (physical_lower_bound > physical_upper_bound && physical_upper_bound != 0) {
          const slot_offset_type new_upper_bound =
              slot_range.lower_bound + (this->context_.buffer_.size() - physical_lower_bound);

          LLFS_VLOG(1) << "Clipping: " << slot_range.upper_bound << " -> " << new_upper_bound << ";"
                       << BATT_INSPECT(physical_lower_bound) << BATT_INSPECT(physical_upper_bound);

          slot_range.upper_bound = new_upper_bound;
        }
      }

      SlotRange aligned_range = this->get_aligned_range(slot_range);

      // If this flush would overlap with an ongoing one (at the last device page) then trim the
      // aligned_range so it doesn't.
      //
      if (this->flush_tail_) {
        if (slot_less_than(aligned_range.lower_bound, this->flush_tail_->upper_bound)) {
          aligned_range.lower_bound = this->flush_tail_->upper_bound;
          if (aligned_range.empty()) {
            flush_lower_bound = this->flush_tail_->upper_bound;
            continue;
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
      this->start_flush_write(slot_range, aligned_range);
      flush_lower_bound = this->unflushed_upper_bound_;
    }
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void start_flush_write(const SlotRange& slot_range, const SlotRange& aligned_range) noexcept
  {
    LLFS_VLOG(1) << "start_flush_write(" << slot_range << ", " << aligned_range << ")";

    this->unflushed_upper_bound_ = slot_max(this->unflushed_upper_bound_, slot_range.upper_bound);

    const i64 write_offset =
        this->data_begin_ + (aligned_range.lower_bound % this->context_.buffer_.size());

    ConstBuffer buffer =
        resize_buffer(this->context_.buffer_.get(aligned_range.lower_bound), aligned_range.size());

    BATT_CHECK_LE(write_offset + (i64)buffer.size(), this->data_end_);

    LLFS_VLOG(1) << " -- async_write_some(offset=" << write_offset << ".."
                 << write_offset + buffer.size() << ", size=" << buffer.size() << ")";

    ++this->writes_pending_;
    this->writes_max_ = std::max(this->writes_max_, this->writes_pending_);

    BATT_CHECK_LE(this->writes_pending_, this->options_.max_concurrent_writes);

    this->storage_.async_write_some(
        write_offset, buffer,
        this->make_write_handler([this, slot_range, aligned_range](StatusOr<i32> result) {
          --this->writes_pending_;
          this->handle_flush_write(slot_range, aligned_range, result);
        }));
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void handle_flush_write(const SlotRange& slot_range, const SlotRange& aligned_range,
                          StatusOr<i32> result)
  {
    LLFS_VLOG(1) << "handle_flush_result(result=" << result << ")" << BATT_INSPECT(slot_range);

    const usize bytes_written = result.ok() ? *result : 0;

    SlotRange aligned_tail = this->get_aligned_tail(aligned_range);

    SlotRange flushed_range{
        .lower_bound = slot_max(slot_range.lower_bound, aligned_range.lower_bound),
        .upper_bound = slot_min(aligned_range.lower_bound + bytes_written, slot_range.upper_bound),
    };

    const bool is_tail = (this->flush_tail_ && *this->flush_tail_ == aligned_tail);
    LLFS_DVLOG(1) << BATT_INSPECT(is_tail);
    if (is_tail) {
      this->flush_tail_ = None;
      this->unflushed_upper_bound_ = flushed_range.upper_bound;
    }

    LLFS_DVLOG(1) << BATT_INSPECT(flushed_range);

    if (!result.ok()) {
      LLFS_VLOG(1) << "(handle_flush_write) error: " << result.status();
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
        this->start_flush_write(updated_range, this->get_aligned_range(updated_range));
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
  void start_control_block_update(slot_offset_type observed_target_trim_pos) noexcept
  {
    if (this->writing_control_block_) {
      LLFS_DVLOG(1) << "start_control_block_update() - already flushing";
      return;
    }

    LLFS_DVLOG(1) << "start_control_block_update() - checking for updates";

    BATT_CHECK_NOT_NULLPTR(this->control_block_);

    slot_offset_type observed_trim_pos = this->trim_pos_.get_value();
    slot_offset_type observed_flush_pos = this->flush_pos_.get_value();

    if (observed_trim_pos == observed_target_trim_pos &&
        observed_flush_pos == this->known_flush_pos_) {
      return;
    }

    LLFS_VLOG(1) << "start_control_block_update(): trim=" << observed_trim_pos << "->"
                 << observed_target_trim_pos << " flush=" << observed_flush_pos << "->"
                 << this->known_flush_pos_;

    this->control_block_->trim_pos = observed_target_trim_pos;
    this->control_block_->flush_pos = this->known_flush_pos_;
    this->writing_control_block_ = true;

    this->storage_.async_write_some(this->config_.control_block_offset, this->control_block_buffer_,
                                    this->make_write_handler([this](StatusOr<i32> result) {
                                      this->handle_control_block_update(result);
                                    }));
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void handle_control_block_update(StatusOr<i32> result) noexcept
  {
    LLFS_VLOG(1) << "handle_control_block_update(" << result << ")";

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

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void initialize_handler_memory_pool()
  {
    LLFS_VLOG(1) << "initialize_handler_memory_pool()";

    BATT_CHECK_EQ(this->handler_memory_pool_, nullptr);

    usize pool_size = this->options_.max_concurrent_writes + 1 /* control block write */ +
                      1 /* target_trim_pos_ wait */ + 1 /* commit_pos_ wait */;

    this->handler_memory_.reset(new HandlerMemoryStorage[pool_size]);

    for (usize i = 0; i < pool_size; ++i) {
      HandlerMemoryStorage* p_storage = std::addressof(this->handler_memory_[i]);
      auto pp_next = (HandlerMemoryStorage**)p_storage;
      *pp_next = this->handler_memory_pool_;
      this->handler_memory_pool_ = p_storage;
    }
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  batt::HandlerMemory<160>* alloc_handler_memory() noexcept
  {
    BATT_CHECK_EQ(this->event_thread_id_, std::this_thread::get_id());

    HandlerMemoryStorage* const p_storage = this->handler_memory_pool_;
    auto pp_next = (HandlerMemoryStorage**)p_storage;
    this->handler_memory_pool_ = *pp_next;
    *pp_next = nullptr;

    LLFS_VLOG(1) << "alloc_handler_memory: " << (void*)p_storage;

    return new (p_storage) batt::HandlerMemory<160>{};
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void free_handler_memory(batt::HandlerMemory<160>* p_mem) noexcept
  {
    BATT_CHECK(!p_mem->in_use());
    BATT_CHECK_EQ(this->event_thread_id_, std::this_thread::get_id());

    using batt::HandlerMemory;

    LLFS_VLOG(1) << "free_handler_memory: " << (void*)p_mem;

    BATT_CHECK(!p_mem->in_use());
    p_mem->~HandlerMemory<160>();
    auto p_storage = (HandlerMemoryStorage*)p_mem;
    auto pp_next = (HandlerMemoryStorage**)p_storage;
    *pp_next = this->handler_memory_pool_;
    this->handler_memory_pool_ = p_storage;
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <typename HandlerFn>
  auto make_write_handler(HandlerFn&& handler_fn) noexcept
  {
    batt::HandlerMemory<160>* const p_mem = this->alloc_handler_memory();

    return batt::make_custom_alloc_handler(
        *p_mem, [this, p_mem, handler_fn = BATT_FORWARD(handler_fn)](const StatusOr<i32>& result) {
          this->free_handler_memory(p_mem);
          handler_fn(result);
        });
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <typename HandlerFn>
  auto make_watch_handler(HandlerFn&& handler_fn) noexcept
  {
    batt::HandlerMemory<160>* const p_mem = this->alloc_handler_memory();

    return batt::make_custom_alloc_handler(
        *p_mem, [this, p_mem, handler_fn = BATT_FORWARD(handler_fn)](
                    const StatusOr<slot_offset_type>& new_value) mutable {
          this->storage_.post_to_event_loop(batt::make_custom_alloc_handler(
              *p_mem, [this, p_mem, new_value,
                       handler_fn = std::move(handler_fn)](const StatusOr<i32>& /*ignored*/) {
                this->free_handler_memory(p_mem);
                handler_fn(new_value);
              }));
        });
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  LogStorageDriverContext& context_;
  const IoRingLogConfig2 config_;
  const IoRingLogRuntimeOptions2 options_;
  StorageT storage_;

  const usize device_page_size_ = usize{1} << this->config_.device_page_size_log2;

  i64 data_begin_;
  i64 data_end_;

  batt::Watch<slot_offset_type> target_trim_pos_{0};
  batt::Watch<slot_offset_type> trim_pos_{0};
  batt::Watch<slot_offset_type> flush_pos_{0};
  batt::Watch<slot_offset_type> commit_pos_{0};

  slot_offset_type known_flush_pos_ = 0;
  slot_offset_type unflushed_upper_bound_ = 0;

  Optional<SlotRange> flush_tail_;

  bool waiting_on_target_trim_pos_ = false;
  bool waiting_on_commit_pos_ = false;
  bool writing_control_block_ = false;

  usize writes_pending_ = 0;
  usize writes_max_ = 0;

  std::unique_ptr<AlignedUnit[]> control_block_memory_;

  ConstBuffer control_block_buffer_;

  PackedLogControlBlock2* control_block_ = nullptr;

  std::vector<SlotRange> flushed_ranges_;

  Optional<EventLoopTask> event_loop_task_;

  using HandlerMemoryStorage =
      std::aligned_storage_t<sizeof(batt::HandlerMemory<160>), alignof(batt::HandlerMemory<160>)>;

  std::unique_ptr<HandlerMemoryStorage[]> handler_memory_;

  HandlerMemoryStorage* handler_memory_pool_ = nullptr;

  std::thread::id event_thread_id_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief An emphemeral LogDevice that stores data in memory only.
 *
 * The commit pos and flush pos are always in sync, so there is no difference between
 * LogReadMode::kSpeculative and LogReadMode::kDurable for this log device type.
 */
class IoRingLogDevice2
    : public BasicRingBufferLogDevice<
          /*Impl=*/IoRingLogDriver2<DefaultIoRingLogDeviceStorage>>
{
 public:
  using Super = BasicRingBufferLogDevice<
      /*Impl=*/IoRingLogDriver2<DefaultIoRingLogDeviceStorage>>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingLogDevice2(const IoRingLogConfig2& config, const IoRingLogRuntimeOptions2& options,
                            DefaultIoRingLogDeviceStorage&& storage) noexcept
      : Super{RingBuffer::TempFile{.byte_size = BATT_CHECKED_CAST(usize, config.log_capacity)},
              config, options, std::move(storage)}
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

#include <llfs/ioring_log_device2.ipp>
