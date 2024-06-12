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
  LLFS_LOG_INFO() << "open()";

  this->initialize_handler_memory_pool();

  BATT_REQUIRE_OK(this->storage_.register_fd());

  LLFS_LOG_INFO() << "starting event loop";

  this->storage_.on_work_started();
  this->event_loop_task_.emplace(this->storage_, "IoRingLogDriver2::open()");

  BATT_REQUIRE_OK(this->read_control_block());
  BATT_REQUIRE_OK(this->read_log_data());

  this->storage_.post_to_event_loop([this](auto&&... /*ignored*/) {
    LLFS_LOG_INFO() << "initial event";

    this->event_thread_id_ = std::this_thread::get_id();
    this->poll(this->commit_pos_.get_value(), this->target_trim_pos_.get_value());
  });

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline Status IoRingLogDriver2<StorageT>::read_control_block()
{
  LLFS_LOG_INFO() << "read_control_block()";

  auto* p_control_block =
      reinterpret_cast<PackedLogControlBlock2*>(this->control_block_memory_.get());

  std::memset(p_control_block, 0, this->config_.control_block_size);

  MutableBuffer mutable_buffer{p_control_block, (usize)this->config_.control_block_size};

  BATT_REQUIRE_OK(this->storage_.read_all(this->config_.control_block_offset, mutable_buffer));

  const slot_offset_type recovered_trim_pos = p_control_block->trim_pos;
  const slot_offset_type recovered_flush_pos = p_control_block->flush_pos;

  LLFS_CHECK_SLOT_LE(recovered_trim_pos, recovered_flush_pos);

  this->reset_trim_pos(recovered_trim_pos);
  this->reset_flush_pos(recovered_flush_pos);

  this->data_begin_ = this->config_.control_block_offset + p_control_block->data_offset;
  this->data_end_ = this->data_begin_ + p_control_block->data_size;

  this->control_block_buffer_ = mutable_buffer;
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
  this->target_trim_pos_.set_value(new_trim_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
void IoRingLogDriver2<StorageT>::reset_flush_pos(slot_offset_type new_flush_pos)
{
  this->commit_pos_.set_value(new_flush_pos);
  this->unflushed_upper_bound_ = new_flush_pos;
  this->known_flush_pos_ = new_flush_pos;
  this->flush_pos_.set_value(new_flush_pos);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename StorageT>
inline Status IoRingLogDriver2<StorageT>::read_log_data()
{
  LLFS_LOG_INFO() << "read_log_data()";

  const slot_offset_type recovered_trim_pos = this->trim_pos_.get_value();
  const slot_offset_type recovered_flush_pos = this->flush_pos_.get_value();

  // TODO [tastolfi 2024-06-12] handle split range case!
  //
  MutableBuffer mutable_buffer = resize_buffer(this->context_.buffer_.get_mut(recovered_trim_pos),
                                               recovered_flush_pos - recovered_trim_pos);

  BATT_REQUIRE_OK(this->storage_.read_all(this->data_begin_, mutable_buffer));

  slot_offset_type slot_offset = recovered_trim_pos;
  ConstBuffer committed_bytes = mutable_buffer;
  slot_offset_type confirmed_flush_pos = slot_offset;
  bool inside_atomic_range = false;

  for (;;) {
    DataReader reader{committed_bytes};
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

    if (slot_size > committed_bytes.size()) {
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

    committed_bytes += slot_size;
    slot_offset += slot_size;

    // If inside an atomic slot range, we hold off on updating the confirmed_flush_pos, just in case
    // the flushed data is cut off before the end of the atomic range.
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
inline Status initialize_log_device2(StorageT& storage, const IoRingLogConfig2& config)
{
  using AlignedUnit = std::aligned_storage_t<kLogAtomicWriteSize, kLogAtomicWriteSize>;

  const usize device_page_size = usize{1} << config.device_page_size_log2;
  const usize buffer_size = batt::round_up_to(sizeof(AlignedUnit), device_page_size);

  BATT_CHECK_EQ(buffer_size, device_page_size);

  std::unique_ptr<AlignedUnit[]> buffer{new AlignedUnit[buffer_size / sizeof(AlignedUnit)]};
  std::memset(buffer.get(), 0, buffer_size);

  auto* control_block = reinterpret_cast<PackedLogControlBlock2*>(buffer.get());

  control_block->magic = PackedLogControlBlock2::kMagic;
  control_block->data_offset = device_page_size;
  control_block->data_size = config.log_capacity;
  control_block->trim_pos = 0;
  control_block->flush_pos = 0;
  control_block->control_block_size = device_page_size;
  control_block->control_header_size = sizeof(PackedLogControlBlock2);
  control_block->device_page_size_log2 = config.device_page_size_log2;

  // Start the I/O event loop in the background.
  //
  storage.on_work_started();
  typename StorageT::EventLoopTask event_loop_task{storage, "initialize_log_device2"};

  // Terminate the I/O event loop when we leave this scope.
  //
  auto on_scope_exit = batt::finally([&] {
    storage.on_work_finished();
    event_loop_task.join();
  });

  // Keep writing while we are making progress and not finished.
  //
  i64 offset = config.control_block_offset;
  ConstBuffer to_write{buffer.get(), device_page_size};

  while (to_write.size() > 0) {
    //----- --- -- -  -  -   -

    // Try to write the rest, but handle short writes.
    //
    StatusOr<i32> result = batt::Task::await<StatusOr<i32>>([&](auto&& handler) {
      storage.async_write_some(offset, to_write, BATT_FORWARD(handler));
    });
    BATT_REQUIRE_OK(result);

    // It's probably illegal for async_write_some to return something other than an aligned page
    // boundary, but adjust it here anyhow (checking for 0).
    //
    usize n_written = batt::round_down_to<usize>(kLogAtomicWriteSize, *result);
    if (n_written == 0) {
      return batt::StatusCode::kInternal;
    }

    // Progress!  Advance pointers and continue.
    //
    offset += n_written;
    to_write += n_written;
  }

  return OkStatus();
}

}  //namespace llfs

#endif  // LLFS_IORING_LOG_DEVICE2_IPP
