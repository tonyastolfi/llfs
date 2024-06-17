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
#include <llfs/ioring_log_config2.hpp>
#include <llfs/ioring_log_device_storage.hpp>
#include <llfs/log_device.hpp>
#include <llfs/log_device_runtime_options.hpp>
#include <llfs/packed_log_control_block2.hpp>
#include <llfs/ring_buffer.hpp>

#include <batteries/async/watch.hpp>
#include <batteries/math.hpp>
#include <batteries/status.hpp>
#include <batteries/tuples.hpp>

#include <chrono>
#include <thread>

namespace llfs {

BATT_STRONG_TYPEDEF(slot_offset_type, TargetTrimPos);
BATT_STRONG_TYPEDEF(slot_offset_type, CommitPos);

/** \brief Initializes an IoRingLogDevice2 using the given storage device and configuration (which
 * includes offset within the passed device).
 */
Status initialize_log_device2(RawBlockFile& file, const IoRingLogConfig2& config);

template <typename StorageT>
class IoRingLogDriver2
{
 public:
  using Self = IoRingLogDriver2;
  using AlignedUnit = std::aligned_storage_t<kLogAtomicWriteSize, kLogAtomicWriteSize>;
  using EventLoopTask = typename StorageT::EventLoopTask;

  static constexpr batt::StaticType<TargetTrimPos> kTargetTrimPos{};
  static constexpr batt::StaticType<CommitPos> kCommitPos{};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The size (bytes) of each preallocated completion handler memory object.
   */
  static constexpr usize kHandlerMemorySize = 160;

  using HandlerMemory = batt::HandlerMemory<kHandlerMemorySize>;

  using HandlerMemoryStorage =
      std::aligned_storage_t<sizeof(HandlerMemory), alignof(HandlerMemory)>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingLogDriver2(LogStorageDriverContext& context,        //
                            const IoRingLogConfig2& config,          //
                            const LogDeviceRuntimeOptions& options,  //
                            StorageT&& storage                       //
                            ) noexcept
      : context_{context}
      , config_{config}
      , options_{options}
      , storage_{std::move(storage)}
      , control_block_memory_{new AlignedUnit[(this->data_page_size_ + sizeof(AlignedUnit) - 1) /
                                              sizeof(AlignedUnit)]}
      , control_block_buffer_{
            this->control_block_memory_.get(),
            batt::round_up_bits(this->config_.data_alignment_log2, sizeof(PackedLogControlBlock2))}
  {
    BATT_CHECK_GE(this->config_.data_alignment_log2, this->config_.device_page_size_log2);
  }

  //----

  Status set_trim_pos(slot_offset_type trim_pos)
  {
    this->observed_watch_[kTargetTrimPos].set_value(trim_pos);
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
    this->observed_watch_[kCommitPos].set_value(commit_pos);
    return OkStatus();
  }

  slot_offset_type get_commit_pos() const
  {
    return this->observed_watch_[kCommitPos].get_value();
  }

  StatusOr<slot_offset_type> await_commit_pos(slot_offset_type min_offset)
  {
    return await_slot_offset(min_offset, this->observed_watch_[kCommitPos]);
  }

  //----

  Status open() noexcept;

  Status close()
  {
    this->halt();
    this->join();

    return OkStatus();
  }

  Status force_close()
  {
    return this->storage_.close();
  }

  void halt()
  {
    for (batt::Watch<llfs::slot_offset_type>& watch : this->observed_watch_) {
      watch.close();
    }
    this->trim_pos_.close();
    this->flush_pos_.close();
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

  /** \brief Returns the current value of either the TargetTrimPos or CommitPos Watch.
   */
  template <typename T>
  T observe(T) const noexcept
  {
    return T{this->observed_watch_[batt::StaticType<T>{}].get_value()};
  }

  /** \brief Reads the control block into memory and initializes member data that depend on it.
   */
  Status read_control_block();

  /** \brief Reads the entire contents of the log into the ring buffer.
   */
  Status read_log_data();

  /** \brief Returns upper bound of known slot commit points recovered from the control block.
   *
   * This will either be the highest of the commit points inside the control block, without
   * exceeding the recovered flush position, or the recovered trim position, whichever is greater.
   */
  slot_offset_type recover_flushed_commit_point() const noexcept;

  /** \brief Starts at the value returned by this->recover_flushed_commit_point() and scans forward
   * in the log data, parsing slot headers until we reach a partially flushed slot or run out of
   * data.  Calls this->reset_flush_pos with the slot upper bound of the highest confirmed slot from
   * the scan.
   */
  Status recover_flush_pos() noexcept;

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
  void poll(CommitPos observed_commit_pos, TargetTrimPos observed_target_trim_pos)
  {
    LLFS_VLOG(1) << "poll(" << observed_commit_pos << ", " << observed_target_trim_pos << ")";

    this->start_flush(observed_commit_pos);
    this->start_control_block_update(observed_target_trim_pos);
    this->wait_for_slot_offset_change(TargetTrimPos{observed_target_trim_pos});
    this->wait_for_slot_offset_change(CommitPos{observed_commit_pos});
  }

  void poll(TargetTrimPos observed_target_trim_pos)
  {
    this->poll(this->observe(CommitPos{}), observed_target_trim_pos);
  }

  void poll(CommitPos observed_commit_pos)
  {
    this->poll(observed_commit_pos, this->observe(TargetTrimPos{}));
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <typename T>
  void wait_for_slot_offset_change(T observed_value)
  {
    static constexpr batt::StaticType<T> kKey;

    if (this->waiting_[kKey]) {
      return;
    }
    this->waiting_[kKey] = true;

    HandlerMemory* const p_mem = this->alloc_handler_memory();

    this->observed_watch_[kKey].async_wait(
        observed_value,

        // Use pre-allocated memory to store the handler in the watch observer list.
        //
        batt::make_custom_alloc_handler(
            *p_mem, [this, p_mem](const StatusOr<slot_offset_type>& new_value) mutable {
              // The callback must run on the IO event loop task thread, so post it
              // here, re-using the pre-allocated handler memory.
              //
              this->storage_.post_to_event_loop(batt::make_custom_alloc_handler(
                  *p_mem, [this, p_mem, new_value](const StatusOr<i32>& /*ignored*/) {
                    // We no longer need the handler memory, so free now.
                    //
                    this->free_handler_memory(p_mem);

                    this->waiting_[kKey] = false;

                    if (!new_value.ok()) {
                      this->context_.update_error_status(new_value.status());
                      return;
                    }

                    this->poll(T{*new_value});
                  }));
            }));
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  SlotRange get_aligned_range(const SlotRange& slot_range) const noexcept
  {
    return SlotRange{
        .lower_bound = batt::round_down_bits(this->config_.data_alignment_log2,  //
                                             slot_range.lower_bound),
        .upper_bound = batt::round_up_bits(this->config_.data_alignment_log2,  //
                                           slot_range.upper_bound),
    };
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  SlotRange get_aligned_tail(const SlotRange& aligned_range) const noexcept
  {
    return SlotRange{
        .lower_bound = aligned_range.upper_bound - this->data_page_size_,
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
      //----- --- -- -  -  -   -

      // Don't start a write if we are at the max concurrent writes limit.
      //
      if (this->writes_pending_ == this->options_.max_concurrent_writes) {
        LLFS_VLOG(1) << "start_flush - at max writes pending";
        break;
      }

      // Don't start a write if we have no data to flush.
      //
      const usize unflushed_size = slot_clamp_distance(flush_lower_bound, observed_commit_pos);
      if (unflushed_size == 0) {
        LLFS_VLOG(1) << "start_flush - unflushed_size == 0";
        break;
      }

      // Don't start a write if there is already a pending write and we aren't at the threshold.
      //
      if (this->writes_pending_ != 0 && unflushed_size < this->options_.flush_delay_threshold) {
        LLFS_VLOG(1) << "start_flush - no action taken: " << BATT_INSPECT(unflushed_size)
                     << BATT_INSPECT(this->writes_pending_);
        break;
      }

      // All gates have been passed!  We are ready to flush some data.
      //
      SlotRange slot_range{
          .lower_bound = flush_lower_bound,
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

  //==#==========+=t=+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
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

    this->async_write_some(write_offset, buffer,
                           [this, slot_range, aligned_range](StatusOr<i32> result) {
                             --this->writes_pending_;
                             this->handle_flush_write(slot_range, aligned_range, result);
                           });
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

    const auto observed_commit_pos = this->observe(CommitPos{});

    if (!is_tail) {
      SlotRange updated_range{
          .lower_bound = flushed_range.upper_bound,
          .upper_bound = slot_min(aligned_range.upper_bound, observed_commit_pos),
      };

      if (!updated_range.empty()) {
        this->start_flush_write(updated_range, this->get_aligned_range(updated_range));
      }
    }

    this->poll(observed_commit_pos, this->observe(TargetTrimPos{}));
  }

  /** \brief Updates this->known_flush_pos_ to include the passed flushed_range.
   *
   * Inserts the flushed range into this->flushed_ranges_ min-heap and then consumes all available
   * contiguous ranges to advance this->known_flush_pos_.
   *
   * This should be called once some slot range is known to have been successfully flushed to the
   * storage media.
   */
  void update_known_flush_pos(const SlotRange& flushed_range) noexcept;

  /** \brief Initiates a rewrite of the control block if necessary.
   *
   * The control block must be updated when the target trim pos or unknown flush pos become out of
   * sync with the last written values.
   *
   * Only one pending async write to the control block is allowed at a time.
   */
  void start_control_block_update(slot_offset_type observed_target_trim_pos) noexcept;

  /** \brief I/O callback that handles the completion of a write to the control block.
   */
  void handle_control_block_update(StatusOr<i32> result) noexcept;

  /** \brief Initiates an asynchronous write to the storage media.
   *
   * This function MUST only be called on the event loop thread.
   */
  template <typename HandlerFn>
  void async_write_some(i64 offset, const ConstBuffer& buffer, HandlerFn&& handler);

  /** \brief Allocates an array of HandlerMemoryStorage objects, adding each to the free pool linked
   * list (this->handler_memory_pool_).
   */
  void initialize_handler_memory_pool();

  /** \brief Pops the next HandlerMemoryStorage object off this->handler_memory_pool_ and uses it to
   * construct a new HandlerMemory object, returning a pointer to the newly constructed
   * HandlerMemory.
   *
   * This function MUST only be called on the event loop thread.
   */
  HandlerMemory* alloc_handler_memory() noexcept;

  /** \brief Destructs the passed HandlerMemory object and adds its storage back to the pool.
   *
   * This function MUST only be called on the event loop thread.
   */
  void free_handler_memory(HandlerMemory* p_mem) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  LogStorageDriverContext& context_;
  const IoRingLogConfig2 config_;
  const LogDeviceRuntimeOptions options_;
  StorageT storage_;

  const usize device_page_size_ = usize{1} << this->config_.device_page_size_log2;
  const usize data_page_size_ = usize{1} << this->config_.data_alignment_log2;
  const usize control_block_size_ = this->data_page_size_;

  i64 data_begin_;
  i64 data_end_;

  batt::StaticTypeMap<std::tuple<TargetTrimPos, CommitPos>, batt::Watch<slot_offset_type>>
      observed_watch_;

  batt::StaticTypeMap<std::tuple<TargetTrimPos, CommitPos>, bool> waiting_;

  batt::Watch<slot_offset_type> trim_pos_{0};
  batt::Watch<slot_offset_type> flush_pos_{0};

  slot_offset_type known_flush_pos_ = 0;
  slot_offset_type unflushed_upper_bound_ = 0;

  Optional<SlotRange> flush_tail_;

  bool writing_control_block_ = false;

  usize writes_pending_ = 0;
  usize writes_max_ = 0;

  std::unique_ptr<AlignedUnit[]> control_block_memory_;

  /** \brief The data buffer used to write updates to the control block; points at
   * this->control_block_memory_.
   */
  ConstBuffer control_block_buffer_;

  /** \brief Pointer to initialized control block for the log.
   */
  PackedLogControlBlock2* control_block_ = nullptr;

  /** \brief A min-heap of confirmed flushed slot ranges; used to advance this->known_flush_pos_,
   * which in turn drives the update of the control block and this->flush_pos_.
   */
  std::vector<SlotRange> flushed_ranges_;

  Optional<EventLoopTask> event_loop_task_;

  std::thread::id event_thread_id_;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // HandlerMemory pool.
  //----- --- -- -  -  -   -

  /** \brief Aligned storage for HandlerMemory objects.
   */
  std::unique_ptr<HandlerMemoryStorage[]> handler_memory_;

  /** \brief The head of a single-linked list of free HandlerMemoryStorage objects.
   */
  HandlerMemoryStorage* handler_memory_pool_ = nullptr;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief An emphemeral LogDevice that stores data in memory only.
 *
 * The commit pos and flush pos are always in sync, so there is no difference between
 * LogReadMode::kSpeculative and LogReadMode::kDurable for this log device type.
 */
template <typename StorageT>
class BasicIoRingLogDevice2
    : public BasicRingBufferLogDevice<
          /*Impl=*/IoRingLogDriver2<StorageT>>
{
 public:
  using Super = BasicRingBufferLogDevice<
      /*Impl=*/IoRingLogDriver2<StorageT>>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit BasicIoRingLogDevice2(const IoRingLogConfig2& config,
                                 const LogDeviceRuntimeOptions& options,
                                 StorageT&& storage) noexcept
      : Super{RingBuffer::TempFile{.byte_size = BATT_CHECKED_CAST(usize, config.log_capacity)},
              config, options, std::move(storage)}
  {
  }
};

using IoRingLogDevice2 = BasicIoRingLogDevice2<DefaultIoRingLogDeviceStorage>;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A factory that produces IoRingLogDevice2 instances of the given size.
 */
class IoRingLogDevice2Factory : public LogDeviceFactory
{
 public:
  explicit IoRingLogDevice2Factory(
      int fd, const FileOffsetPtr<const PackedLogDeviceConfig2&>& packed_config,
      const LogDeviceRuntimeOptions& options) noexcept;

  explicit IoRingLogDevice2Factory(int fd, const IoRingLogConfig2& config,
                                   const LogDeviceRuntimeOptions& options) noexcept;

  ~IoRingLogDevice2Factory() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  StatusOr<std::unique_ptr<IoRingLogDevice2>> open_ioring_log_device();

  StatusOr<std::unique_ptr<LogDevice>> open_log_device(const LogScanFn& scan_fn) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  int fd_;
  IoRingLogConfig2 config_;
  LogDeviceRuntimeOptions options_;
};

}  // namespace llfs

#endif  // LLFS_IORING_LOG_DEVICE2_HPP

#include <llfs/ioring_log_device2.ipp>
