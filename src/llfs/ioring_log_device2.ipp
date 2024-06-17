//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DEVICE2_IPP
#define LLFS_IORING_LOG_DEVICE2_IPP

#include <llfs/data_reader.hpp>
#include <llfs/slot_writer.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline Status IoRingLogDriver2<StorageT>::open() noexcept
{
  LLFS_VLOG(1) << "open()";

  this->initialize_handler_memory_pool();

  BATT_REQUIRE_OK(this->storage_.register_fd());

  LLFS_VLOG(1) << "starting event loop";

  this->storage_.on_work_started();
  this->event_loop_task_.emplace(this->storage_, "IoRingLogDriver2::open()");

  BATT_REQUIRE_OK(this->read_control_block());
  BATT_REQUIRE_OK(this->read_log_data());

  this->storage_.post_to_event_loop([this](auto&&... /*ignored*/) {
    LLFS_VLOG(1) << "initial event";

    this->event_thread_id_ = std::this_thread::get_id();
    this->poll(this->observe(CommitPos{}), this->observe(TargetTrimPos{}));
  });

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline Status IoRingLogDriver2<StorageT>::read_control_block()
{
  LLFS_VLOG(1) << "read_control_block()";

  auto* p_control_block =
      reinterpret_cast<PackedLogControlBlock2*>(this->control_block_memory_.get());

  std::memset(p_control_block, 0, this->data_page_size_);

  MutableBuffer mutable_buffer{p_control_block, this->control_block_size_};

  BATT_REQUIRE_OK(this->storage_.read_all(this->config_.control_block_offset, mutable_buffer));

  const slot_offset_type recovered_trim_pos = p_control_block->trim_pos;
  const slot_offset_type recovered_flush_pos = p_control_block->flush_pos;

  LLFS_VLOG(1) << BATT_INSPECT(recovered_trim_pos) << BATT_INSPECT(recovered_flush_pos)
               << BATT_INSPECT(this->control_block_->generation);

  LLFS_CHECK_SLOT_LE(recovered_trim_pos, recovered_flush_pos);

  this->reset_trim_pos(recovered_trim_pos);
  this->reset_flush_pos(recovered_flush_pos);

  this->data_begin_ = this->config_.control_block_offset + this->data_page_size_;
  this->data_end_ = this->data_begin_ + p_control_block->data_size;

  this->control_block_buffer_ = ConstBuffer{
      p_control_block,
      batt::round_up_to<usize>(this->device_page_size_, p_control_block->control_header_size),
  };
  this->control_block_ = p_control_block;

  // TODO [tastolfi 2024-06-11] verify control block values against config where possible.

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::reset_trim_pos(slot_offset_type new_trim_pos)
{
  this->trim_pos_.set_value(new_trim_pos);
  this->observed_watch_[kTargetTrimPos].set_value(new_trim_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::reset_flush_pos(slot_offset_type new_flush_pos)
{
  this->observed_watch_[kCommitPos].set_value(new_flush_pos);
  this->unflushed_upper_bound_ = new_flush_pos;
  this->known_flush_pos_ = new_flush_pos;
  this->flush_pos_.set_value(new_flush_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline Status IoRingLogDriver2<StorageT>::read_log_data()
{
  LLFS_VLOG(1) << "read_log_data()";

  const slot_offset_type recovered_trim_pos = this->trim_pos_.get_value();
  const slot_offset_type recovered_flush_pos = this->flush_pos_.get_value();

  const slot_offset_type aligned_trim_pos =
      batt::round_down_bits(this->config_.device_page_size_log2, recovered_trim_pos);

  const slot_offset_type aligned_flush_pos =
      batt::round_up_bits(this->config_.device_page_size_log2, recovered_flush_pos);

  const i64 read_begin_offset =
      this->data_begin_ + BATT_CHECKED_CAST(i64, aligned_trim_pos % this->context_.buffer_.size());

  MutableBuffer mutable_buffer = resize_buffer(this->context_.buffer_.get_mut(aligned_trim_pos),
                                               aligned_flush_pos - aligned_trim_pos);

  const i64 read_end_offset = read_begin_offset + BATT_CHECKED_CAST(i64, mutable_buffer.size());

  LLFS_VLOG(1) << BATT_INSPECT(recovered_trim_pos) << BATT_INSPECT(recovered_flush_pos);

  if (read_end_offset <= this->data_end_) {
    BATT_REQUIRE_OK(this->storage_.read_all(read_begin_offset, mutable_buffer));

  } else {
    // lower == lower offset in the file.
    //
    const usize lower_part_size = read_end_offset - this->data_end_;
    const usize upper_part_size = this->data_end_ - read_begin_offset;

    BATT_CHECK_EQ(mutable_buffer.size(), lower_part_size + upper_part_size);

    MutableBuffer lower_part_buffer = mutable_buffer + upper_part_size;
    MutableBuffer upper_part_buffer = resize_buffer(mutable_buffer, upper_part_size);

    BATT_CHECK_EQ(lower_part_buffer.size(), lower_part_size);
    BATT_CHECK_EQ(upper_part_buffer.size(), upper_part_size);

    BATT_REQUIRE_OK(this->storage_.read_all(this->data_begin_, lower_part_buffer));
    BATT_REQUIRE_OK(this->storage_.read_all(read_begin_offset, upper_part_buffer));
  }

  return this->recover_flush_pos();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
slot_offset_type IoRingLogDriver2<StorageT>::recover_flushed_commit_point() const noexcept
{
  const slot_offset_type recovered_trim_pos = this->control_block_->trim_pos;
  const slot_offset_type recovered_flush_pos = this->control_block_->flush_pos;

  slot_offset_type slot_offset = recovered_trim_pos;

  std::vector<slot_offset_type> sorted_commit_points(this->control_block_->commit_points.begin(),
                                                     this->control_block_->commit_points.end());

  std::sort(sorted_commit_points.begin(), sorted_commit_points.end(), SlotOffsetOrder{});

  auto iter = std::lower_bound(sorted_commit_points.begin(), sorted_commit_points.end(),
                               recovered_flush_pos, SlotOffsetOrder{});

  if (iter != sorted_commit_points.end()) {
    slot_offset = slot_max(slot_offset, *iter);
  }

  return slot_offset;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
Status IoRingLogDriver2<StorageT>::recover_flush_pos() noexcept
{
  const slot_offset_type recovered_flush_pos = this->flush_pos_.get_value();

  slot_offset_type slot_offset = this->recover_flushed_commit_point();

  ConstBuffer buffer =
      resize_buffer(this->context_.buffer_.get(slot_offset), recovered_flush_pos - slot_offset);

  slot_offset_type confirmed_flush_pos = slot_offset;

  // This should be correct, since commit is called only once per atomic range, and atomic ranges
  // are only recoverable if no part of the range (including the begin/end tokens) has been trimmed.
  //
  bool inside_atomic_range = false;

  for (;;) {
    DataReader reader{buffer};
    const usize bytes_available_before = reader.bytes_available();
    Optional<u64> slot_body_size = reader.read_varint();

    if (!slot_body_size) {
      // Partially committed slot (couldn't even read a whole varint for the slot header!)  Break
      // out of the loop.
      //
      LLFS_VLOG(1) << " -- Incomplete slot header, exiting loop;" << BATT_INSPECT(slot_offset)
                   << BATT_INSPECT(bytes_available_before);
      break;
    }

    const usize bytes_available_after = reader.bytes_available();
    const usize slot_header_size = bytes_available_before - bytes_available_after;
    const usize slot_size = slot_header_size + *slot_body_size;

    if (slot_size > buffer.size()) {
      // Partially committed slot; break out of the loop without updating slot_offset (we're
      // done!)
      //
      LLFS_VLOG(1) << " -- Incomplete slot body, exiting loop;" << BATT_INSPECT(slot_offset)
                   << BATT_INSPECT(bytes_available_before) << BATT_INSPECT(bytes_available_after)
                   << BATT_INSPECT(slot_header_size) << BATT_INSPECT(slot_body_size)
                   << BATT_INSPECT(slot_size);
      break;
    }

    // Check for control token; this indicates the beginning or end of an atomic slot range.
    //
    if (*slot_body_size == 0) {
      if (slot_header_size == SlotWriter::WriterLock::kBeginAtomicRangeTokenSize) {
        inside_atomic_range = true;
      } else if (slot_header_size == SlotWriter::WriterLock::kEndAtomicRangeTokenSize) {
        inside_atomic_range = false;
      }
    }

    buffer += slot_size;
    slot_offset += slot_size;

    // If inside an atomic slot range, we hold off on updating the confirmed_flush_pos, just in
    // case the flushed data is cut off before the end of the atomic range.
    //
    if (!inside_atomic_range) {
      confirmed_flush_pos = slot_offset;
    }
  }

  LLFS_VLOG(1) << " -- Slot scan complete;" << BATT_INSPECT(slot_offset);

  this->reset_flush_pos(confirmed_flush_pos);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::update_known_flush_pos(const SlotRange& flushed_range) noexcept
{
  // Insert the flushed_range into the min-heap.
  //
  this->flushed_ranges_.emplace_back(flushed_range);
  std::push_heap(this->flushed_ranges_.begin(), this->flushed_ranges_.end(), SlotRangePriority{});

  // Advance this->known_flush_pos_ as long as we see flushed ranges without gaps in between.
  //
  while (!this->flushed_ranges_.empty()) {
    SlotRange& next_range = this->flushed_ranges_.front();

    // Found a gap; we are done!
    //
    if (next_range.lower_bound != this->known_flush_pos_) {
      LLFS_CHECK_SLOT_LT(this->known_flush_pos_, next_range.lower_bound);
      break;
    }

    this->known_flush_pos_ = next_range.upper_bound;

    // Pop the min range off the heap and keep going.
    //
    std::pop_heap(this->flushed_ranges_.begin(), this->flushed_ranges_.end(), SlotRangePriority{});
    this->flushed_ranges_.pop_back();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::start_control_block_update(
    slot_offset_type observed_target_trim_pos) noexcept
{
  if (this->writing_control_block_) {
    return;
  }

  BATT_CHECK_NOT_NULLPTR(this->control_block_) << "Forgot to call read_control_block()?";

  const slot_offset_type observed_trim_pos = this->trim_pos_.get_value();
  const slot_offset_type observed_flush_pos = this->flush_pos_.get_value();

  if (observed_trim_pos == observed_target_trim_pos &&
      observed_flush_pos == this->known_flush_pos_) {
    return;
  }

  LLFS_VLOG(1) << "start_control_block_update():"                                    //
               << " trim=" << observed_trim_pos << "->" << observed_target_trim_pos  //
               << " flush=" << observed_flush_pos << "->" << this->known_flush_pos_;

  BATT_CHECK_EQ(observed_trim_pos, this->control_block_->trim_pos);
  BATT_CHECK_EQ(observed_flush_pos, this->control_block_->flush_pos);

  this->control_block_->commit_points[this->control_block_->next_commit_i] =
      this->observe(CommitPos{});

  this->control_block_->next_commit_i =
      (this->control_block_->next_commit_i + 1) % this->control_block_->commit_points.size();

  this->control_block_->trim_pos = observed_target_trim_pos;
  this->control_block_->flush_pos = this->known_flush_pos_;
  this->control_block_->generation = this->control_block_->generation + 1;

  this->writing_control_block_ = true;

  this->async_write_some(this->config_.control_block_offset, this->control_block_buffer_,
                         [this](StatusOr<i32> result) {
                           this->handle_control_block_update(result);
                         });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::handle_control_block_update(StatusOr<i32> result) noexcept
{
  LLFS_VLOG(1) << "handle_control_block_update(" << result << ")";

  BATT_CHECK(this->writing_control_block_);

  this->writing_control_block_ = false;

  if (!result.ok()) {
    this->context_.update_error_status(result.status());
    return;
  }

  if (BATT_CHECKED_CAST(usize, *result) != this->control_block_buffer_.size()) {
    LLFS_LOG_ERROR() << "Failed to write entire log control block!";
    this->context_.update_error_status(batt::StatusCode::kInternal);
    return;
  }

  // We can now notify the outside world that the trim/flush pointers have been updated.
  //
  this->trim_pos_.set_value(this->control_block_->trim_pos);
  this->flush_pos_.set_value(this->control_block_->flush_pos);

  this->poll(this->observe(CommitPos{}), this->observe(TargetTrimPos{}));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
template <typename HandlerFn>
void IoRingLogDriver2<StorageT>::async_write_some(i64 offset, const ConstBuffer& buffer,
                                                  HandlerFn&& handler)
{
  HandlerMemory* const p_mem = this->alloc_handler_memory();

  this->storage_.async_write_some(
      offset, buffer,
      batt::make_custom_alloc_handler(
          *p_mem, [this, p_mem, handler = BATT_FORWARD(handler)](const StatusOr<i32>& result) {
            this->free_handler_memory(p_mem);
            handler(result);
          }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::initialize_handler_memory_pool()
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
template <typename StorageT>
auto IoRingLogDriver2<StorageT>::alloc_handler_memory() noexcept -> HandlerMemory*
{
  BATT_CHECK_EQ(this->event_thread_id_, std::this_thread::get_id());

  HandlerMemoryStorage* const p_storage = this->handler_memory_pool_;
  auto pp_next = (HandlerMemoryStorage**)p_storage;
  this->handler_memory_pool_ = *pp_next;
  *pp_next = nullptr;

  return new (p_storage) HandlerMemory{};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::free_handler_memory(HandlerMemory* p_mem) noexcept
{
  BATT_CHECK(!p_mem->in_use());
  BATT_CHECK_EQ(this->event_thread_id_, std::this_thread::get_id());

  p_mem->~HandlerMemory();

  auto p_storage = (HandlerMemoryStorage*)p_mem;
  auto pp_next = (HandlerMemoryStorage**)p_storage;
  *pp_next = this->handler_memory_pool_;
  this->handler_memory_pool_ = p_storage;
}

}  //namespace llfs

#endif  // LLFS_IORING_LOG_DEVICE2_IPP
