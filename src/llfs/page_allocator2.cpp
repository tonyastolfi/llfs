//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator2.hpp>
//
#include <llfs/slot_reader.hpp>

#include <batteries/stream_util.hpp>

namespace llfs {
namespace experimental {

static_assert(!is_self_contained_packed_type<PackedPageAllocatorTxn>());
static_assert(is_self_contained_packed_type<PackedPageAllocatorAttach>());
static_assert(is_self_contained_packed_type<PackedPageAllocatorDetach>());

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorAttach& t)
{
  return out << "PackedPageAllocatorAttach{.user_id=" << t.user_id << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorDetach& t)
{
  return out << "PackedPageAllocatorDetach{.user_id=" << t.user_id << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PageAllocatorTxn& t)
{
  return out << "PageAllocatorTxn{.user_id=" << t.user_id << ", .user_slot=" << t.user_slot
             << ", .ref_counts=" << batt::dump_range(t.ref_counts) << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorTxn& t)
{
  return out << "PackedPageAllocatorTxn{.user_id=" << t.user_id << ", .user_slot=" << t.user_slot
             << ", .ref_counts=" << batt::dump_range(t.ref_counts) << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedPageAllocatorTxn* pack_object_to(const PageAllocatorTxn& from, PackedPageAllocatorTxn* to,
                                       DataPacker* dst)
{
  to->user_id = from.user_id;
  to->user_slot = from.user_slot;
  to->ref_counts.initialize(0);

  BasicArrayPacker<PackedPageRefCount, DataPacker> packed_ref_counts{&to->ref_counts, dst};

  for (const PageRefCount& prc : from.ref_counts) {
    if (!packed_ref_counts.pack_item(prc)) {
      return nullptr;
    }
  }

  return to;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status validate_packed_value(const PackedPageAllocatorTxn& txn, const void* buffer_data,
                             usize buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(txn, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(txn.ref_counts, buffer_data, buffer_size));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof_page_allocator_txn(usize n_ref_counts)
{
  return sizeof(PackedPageAllocatorTxn) + sizeof(PackedPageRefCount) * n_ref_counts;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PageAllocatorTxn& txn)
{
  return packed_sizeof_page_allocator_txn(txn.ref_counts.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedPageAllocatorTxn& txn)
{
  return packed_sizeof_page_allocator_txn(txn.ref_counts.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof_page_allocator_attach_slot() noexcept
{
  static const usize size_ =
      packed_sizeof_slot_with_payload_size(sizeof(PackedPageAllocatorAttach));
  return size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<const PackedPageAllocatorTxn&> unpack_object(const PackedPageAllocatorTxn& obj,
                                                      DataReader*) noexcept
{
  return obj;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageAllocatorState

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageAllocatorState::PageAllocatorState(const PageIdFactory& page_ids,
                                                    SlotLockManager* trim_control) noexcept
    : page_ids{page_ids}
    , in_recovery_mode{true}
    , pending_recovery{}
    , attach_state{}
    , page_state(page_ids.get_physical_page_count())
    , trim_control{trim_control}
{
  std::memset(this->page_state.data(), 0, sizeof(PageState) * this->page_state.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot,
                                  const PackedPageAllocatorAttach& attach) noexcept
{
  if (this->in_recovery_mode) {
    (void)this->pending_recovery[attach.user_id];
  }

  AttachState& state = this->attach_state[attach.user_id];

  state.last_update = slot.offset.upper_bound;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot [[maybe_unused]],
                                  const PackedPageAllocatorDetach& detach) noexcept
{
  if (this->in_recovery_mode) {
    this->pending_recovery.erase(detach.user_id);
  }

  this->attach_state.erase(detach.user_id);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot,
                                  const PackedPageRefCount& ref_count) noexcept
{
  return this->update_ref_count(slot.offset.upper_bound, ref_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot, const PackedPageAllocatorTxn& txn) noexcept
{
  if (this->in_recovery_mode) {
    SlotReadLock& txn_slot_lock = this->pending_recovery[txn.user_id][txn.user_slot.value()];

    // There should be no read lock currently held for this user/slot; txns must have unique user
    // slot numbers!
    //
    if (txn_slot_lock) {
      return batt::StatusCode::kInternal;  // TODO [tastolfi 2024-03-20] be more specific
    }

    BATT_ASSIGN_OK_RESULT(txn_slot_lock, this->trim_control->lock_slots(
                                             slot.offset, "PageAllocator::recover_impl()"));
  } else {
    BATT_CHECK_NE(this->attach_state.count(txn.user_id), 0u)
        << "PageAllocator txn for user_id that is not attached!" << BATT_INSPECT(txn);
  }

  for (const PackedPageRefCount& pprc : txn.ref_counts) {
    BATT_REQUIRE_OK(this->update_ref_count(slot.offset.upper_bound, pprc));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocatorState::PageState* PageAllocatorState::get_page_state(PageId page_id) noexcept
{
  const u64 physical_page_count = this->page_ids.get_physical_page_count();
  const i64 physical_page = this->page_ids.get_physical_page(page_id);

  if (static_cast<u64>(physical_page) >= physical_page_count) {
    return nullptr;
  }

  return &this->page_state[physical_page];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageAllocatorState::PageState* PageAllocatorState::get_page_state(
    PageId page_id) const noexcept
{
  const u64 physical_page_count = this->page_ids.get_physical_page_count();
  const i64 physical_page = this->page_ids.get_physical_page(page_id);

  if (static_cast<u64>(physical_page) >= physical_page_count) {
    return nullptr;
  }

  return &this->page_state[physical_page];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i32 PageAllocatorState::get_ref_count(PageId page_id) const noexcept
{
  const PageState* p_state = this->get_page_state(page_id);
  BATT_CHECK_NOT_NULLPTR(p_state);
  return p_state->ref_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update_ref_count(slot_offset_type slot_upper_bound,
                                            const PackedPageRefCount& pprc) noexcept
{
  const PageId page_id = pprc.page_id.unpack();
  const u64 physical_page_count = this->page_ids.get_physical_page_count();
  const i64 physical_page = this->page_ids.get_physical_page(page_id);

  if (static_cast<u64>(physical_page) >= physical_page_count) {
    return batt::StatusCode::kDataLoss;  // TODO [tastolfi 2024-03-19] be more specific
  }

  PageState& state = this->page_state[physical_page];

  state.ref_count = pprc.ref_count;
  state.last_update = slot_upper_bound;
  state.generation = this->page_ids.get_generation(page_id);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageAllocatorState::updated_since(slot_offset_type slot_upper_bound,
                                       PageId page_id) const noexcept
{
  const PageState* p_state = this->get_page_state(page_id);
  BATT_CHECK_NOT_NULLPTR(p_state);
  return slot_less_than(slot_upper_bound, p_state->last_update);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageAllocatorState::updated_since(slot_offset_type slot_upper_bound,
                                       const boost::uuids::uuid& user_id) const noexcept
{
  auto iter = this->attach_state.find(user_id);

  return iter == this->attach_state.end() ||
         slot_less_than(slot_upper_bound, iter->second.last_update);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageAllocatorState::CheckpointInfo> PageAllocatorState::append_checkpoint(
    LogDevice::Reader& log_reader, batt::Grant& grant,
    TypedSlotWriter<PackedPageAllocatorEvent>& slot_writer)
{
  CheckpointInfo info;

  TypedSlotWriter<PackedPageAllocatorEvent>::MultiAppend multi_append{slot_writer};

  // Make note of the current slot offset to verify we don't go past this point when writing
  // checkpoint data, as a sanity check.
  //
  const slot_offset_type src_upper_bound = multi_append.slot_offset();

  info.trim_target = log_reader.slot_offset();

  TypedSlotReader<PackedPageAllocatorEvent> slot_reader{log_reader};

  //----- --- -- -  -  -   -
  const auto refresh_slot = [&](const SlotParse& slot, const auto& event) -> Status {
    using EventT = std::decay_t<decltype(event)>;

    if (this->updated_since(slot.offset.upper_bound, Self::get_event_entity(event))) {
      return OkStatus();
    }

    StatusOr<SlotParseWithPayload<const EventT*>> new_slot =
        multi_append.typed_append(grant, event);

    if (!new_slot.ok()) {
      if (new_slot.status() == ::llfs::make_status(StatusCode::kSlotGrantTooSmall)) {
        return batt::StatusCode::kLoopBreak;
      } else {
        return new_slot.status();
      }
    }

    return this->update(new_slot->slot, *new_slot->payload);
  };
  //----- --- -- -  -  -   -

  StatusOr<usize> result = slot_reader.run(
      batt::WaitForResource::kFalse,
      //----- --- -- -  -  -   -
      [&](const SlotParse& slot, const PackedPageAllocatorAttach& attach) -> Status {
        LLFS_CHECK_SLOT_LT(slot.offset.lower_bound, src_upper_bound);
        BATT_REQUIRE_OK(refresh_slot(slot, attach));
        clamp_min_slot(&info.trim_target, slot.offset.upper_bound);
        return OkStatus();
      },
      //----- --- -- -  -  -   -
      [&](const SlotParse& slot, const PackedPageAllocatorDetach& /*detach*/) -> Status {
        LLFS_CHECK_SLOT_LT(slot.offset.lower_bound, src_upper_bound);
        clamp_min_slot(&info.trim_target, slot.offset.upper_bound);
        return OkStatus();
      },
      //----- --- -- -  -  -   -
      [&](const SlotParse& slot, const PackedPageRefCount& pprc) -> Status {
        LLFS_CHECK_SLOT_LT(slot.offset.lower_bound, src_upper_bound);
        BATT_REQUIRE_OK(refresh_slot(slot, pprc));
        clamp_min_slot(&info.trim_target, slot.offset.upper_bound);
        return OkStatus();
      },
      //----- --- -- -  -  -   -
      [&](const SlotParse& slot, const PackedPageAllocatorTxn& txn) -> Status {
        LLFS_CHECK_SLOT_LT(slot.offset.lower_bound, src_upper_bound);
        for (const PackedPageRefCount& pprc : txn.ref_counts) {
          BATT_REQUIRE_OK(refresh_slot(slot, pprc));
        }
        clamp_min_slot(&info.trim_target, slot.offset.upper_bound);
        return OkStatus();
      });

  BATT_REQUIRE_OK(result);
  BATT_ASSIGN_OK_RESULT(info.checkpoint_slots, multi_append.finalize(grant));

  return info;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::validate_and_apply_ref_count_deltas(
    const Slice<PageRefCount>& ref_count_updates) const noexcept
{
  // TODO [tastolfi 2024-08-08] Change all status codes below to unique codes (add to
  // llfs::StatusCode).

  const u64 physical_page_count = this->page_ids.get_physical_page_count();

  for (PageRefCount& prc : ref_count_updates) {
    // If the delta is zero then this should have been filtered out.
    //
    if (prc.ref_count == 0) {
      return batt::StatusCode::kInvalidArgument;
    }

    // Sanity check: make sure device id matches.
    //
    if (this->page_ids.get_device_id(prc.page_id) != this->page_ids.get_device_id()) {
      return batt::StatusCode::kInvalidArgument;
    }

    // Validate that the physical page is in range.
    //
    const u64 physical_page = this->page_ids.get_physical_page(prc.page_id);
    if (physical_page >= physical_page_count) {
      return batt::StatusCode::kOutOfRange;
    }

    // Retrieve the current state.
    //
    const PageState& old_state = this->page_state[physical_page];
    BATT_CHECK_GE(old_state.ref_count, 0) << "The ref count of a page must never be negative!";

    // Validate the page generation.
    //
    const page_generation_int old_generation = this->page_ids.get_generation(prc.page_id);
    if (old_generation != old_state.generation) {
      return batt::StatusCode::kInvalidArgument;
    }

    // Handle two high level cases: ref_count == 1 and ref_count != 1.  This is equivalent to
    // whether prc.ref_count (update delta) is equal to kRefCount_1_to_0; i.e.,
    //   (prc.ref_count == kRefCount_1_to_0) == (old_state.ref_count == 1)
    //
    if (prc.ref_count == kRefCount_1_to_0) {
      if (old_state.ref_count != 1) {
        return batt::StatusCode::kFailedPrecondition;
      }

      // If dropping the page, we must advance the generation of the page_id.
      //
      const page_generation_int new_generation = old_generation + 1;

      prc.page_id = this->page_ids.make_page_id(physical_page, new_generation);
      prc.ref_count = 0;

    } else {
      // kRefCount_1_to_0 is the only allowed update once the ref count has become 1.
      //
      if (old_state.ref_count == 1) {
        return batt::StatusCode::kFailedPrecondition;
      }

      const i32 new_ref_count = old_state.ref_count + prc.ref_count;

      if (prc.ref_count > 0) {
        BATT_CHECK_GT(new_ref_count, old_state.ref_count)
            << "Integer wrap detected:" << BATT_INSPECT(prc);

        // When first writing a new page, we must increase ref count by at least 2.
        //
        if (old_state.ref_count == 0 && new_ref_count < 2) {
          return batt::StatusCode::kInvalidArgument;
        }

      } else {
        BATT_CHECK_LT(new_ref_count, old_state.ref_count)
            << "Integer wrap detected:" << BATT_INSPECT(prc);

        // We must never go from above 1 to below 1 in a single update.
        //
        if (new_ref_count < 1) {
          return batt::StatusCode::kInvalidArgument;
        }
      }

      prc.ref_count = new_ref_count;
    }
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ u64 PageAllocator::calculate_log_size(PageCount physical_page_count,
                                                 MaxAttachments max_attachments)
{
  return 2 *
         batt::round_up_bits(12, PageAllocator::kCheckpointTargetSize +
                                     physical_page_count * packed_sizeof_page_ref_count_slot() +
                                     max_attachments * packed_sizeof_page_allocator_attach_slot());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<PageAllocator>> PageAllocator::recover(
    const PageAllocatorRuntimeOptions& options, const PageIdFactory& page_ids,
    MaxAttachments max_attachments, LogDeviceFactory& log_device_factory)
{
  // Recover the page allocator log.
  //
  BATT_ASSIGN_OK_RESULT(std::unique_ptr<LogDevice> recovered_log,
                        open_log_device_no_scan(log_device_factory));

  // Create the PageAllocator object.
  //
  std::unique_ptr<PageAllocator> page_allocator{
      new PageAllocator{std::string{options.name}, page_ids, max_attachments,
                        std::move(recovered_log), std::make_unique<SlotLockManager>()}};

  // Recover state from the log.
  //
  BATT_REQUIRE_OK(page_allocator->recover_impl());

  // Start background compaction task.
  //
  page_allocator->start_checkpoint_task(options.scheduler);

  // Success!
  //
  return {std::move(page_allocator)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocator::PageAllocator(std::string&& name, const PageIdFactory& page_ids, u64 max_attachments,
                             std::unique_ptr<LogDevice>&& log_device,
                             std::unique_ptr<SlotLockManager>&& trim_control) noexcept
    : name_{std::move(name)}
    , page_ids_{page_ids}
    , max_attachments_{max_attachments}
    , recovering_user_count_{1}
    , free_pool_{}
    , free_pool_push_count_{0}
    , log_device_{std::move(log_device)}
    , trim_control_{std::move(trim_control)}
    , slot_writer_{*this->log_device_}
    , state_{this->page_ids_, this->trim_control_.get()}
    , checkpoint_grant_pool_{BATT_OK_RESULT_OR_PANIC(
          this->slot_writer_.reserve(0, batt::WaitForResource::kFalse))}
    , update_grant_{BATT_OK_RESULT_OR_PANIC(
          this->slot_writer_.reserve(0, batt::WaitForResource::kFalse))}
    , checkpoint_task_{None}
{
  BATT_CHECK(this->free_pool_.is_lock_free());
  this->free_pool_.reserve(this->page_ids_.get_physical_page_count());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocator::~PageAllocator() noexcept
{
  this->halt();
  this->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::halt() noexcept
{
  this->recovering_user_count_.close();
  this->free_pool_push_count_.close();
  this->log_device_->halt();
  this->trim_control_->halt();
  this->slot_writer_.halt();
  this->update_grant_.revoke();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::join() noexcept
{
  if (this->checkpoint_task_) {
    this->checkpoint_task_->join();
    this->checkpoint_task_ = None;
  }
  this->log_device_->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocator::recover_impl()
{
  batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

  const i64 physical_page_count =
      BATT_CHECKED_CAST(i64, this->page_ids_.get_physical_page_count().value());

  SlotRange checkpoint_lock_range{
      .lower_bound = 0,
      .upper_bound = 0,
  };

  // Scan the log slots to restore state.
  //
  {
    std::unique_ptr<LogDevice::Reader> log_reader =
        this->log_device_->new_reader(/*lower_bound=*/None, /*mode=*/LogReadMode::kDurable);

    checkpoint_lock_range.lower_bound = log_reader->slot_offset();
    {
      TypedSlotReader<PackedPageAllocatorEvent> slot_reader{*log_reader};

      BATT_REQUIRE_OK(slot_reader.run(batt::WaitForResource::kFalse,
                                      //----- --- -- -  -  -   -
                                      [&](const SlotParse& slot, const auto& event) -> Status {
                                        return locked_state->update(slot, event);
                                      }));
    }
    checkpoint_lock_range.upper_bound = log_reader->slot_offset();
  }

  // Do another scan to find the current trim target.
  //
  {
    std::unique_ptr<LogDevice::Reader> log_reader =
        this->log_device_->new_reader(/*lower_bound=*/checkpoint_lock_range.lower_bound,
                                      /*mode=*/LogReadMode::kDurable);

    TypedSlotReader<PackedPageAllocatorEvent> slot_reader{*log_reader};

    BATT_REQUIRE_OK(slot_reader.run(
        batt::WaitForResource::kFalse,
        //----- --- -- -  -  -   -
        [&](const SlotParse& slot, const PackedPageAllocatorAttach& attach) -> Status {
          if (!locked_state->updated_since(slot.offset.upper_bound, attach.user_id)) {
            return batt::StatusCode::kLoopBreak;
          }
          checkpoint_lock_range.lower_bound = slot.offset.upper_bound;
          return OkStatus();
        },

        //----- --- -- -  -  -   -
        [&](const SlotParse& slot, const PackedPageAllocatorDetach& /*event*/) -> Status {
          checkpoint_lock_range.lower_bound = slot.offset.upper_bound;
          return OkStatus();
        },

        //----- --- -- -  -  -   -
        [&](const SlotParse& slot, const PackedPageRefCount& pprc) -> Status {
          if (!locked_state->updated_since(slot.offset.upper_bound, pprc.page_id.unpack())) {
            return batt::StatusCode::kLoopBreak;
          }
          checkpoint_lock_range.lower_bound = slot.offset.upper_bound;
          return OkStatus();
        },

        //----- --- -- -  -  -   -
        [&](const SlotParse& slot, const PackedPageAllocatorTxn& txn) -> Status {
          for (const PackedPageRefCount& pprc : txn.ref_counts) {
            if (!locked_state->updated_since(slot.offset.upper_bound, pprc.page_id.unpack())) {
              return batt::StatusCode::kLoopBreak;
            }
          }
          checkpoint_lock_range.lower_bound = slot.offset.upper_bound;
          return OkStatus();
        }));
  }

  // Lock the "live" data range (i.e. slots not made obsolete by some later checkpoint or update).
  //
  this->checkpoint_trim_lock_ = BATT_OK_RESULT_OR_PANIC(
      this->trim_control_->lock_slots(checkpoint_lock_range, "PageAllocator::recover_impl"));

  // Trim as much as we can.
  //
  BATT_REQUIRE_OK(this->slot_writer_.trim(this->trim_control_->get_lower_bound()));

  this->checkpoint_grant_pool_.subsume(  //
      this->slot_writer_.reserve_or_panic(this->get_target_checkpoint_grant_pool_size(),
                                          batt::WaitForResource::kFalse));

  this->update_grant_.subsume(  //
      this->slot_writer_.reserve_or_panic(this->slot_writer_.pool_size(),
                                          batt::WaitForResource::kFalse));

  // Sanity checks.
  //
  if (locked_state->pending_recovery.size() > this->max_attachments_) {
    return batt::StatusCode::kInternal;  // TODO [tastolfi 2024-03-20]
  }
  if (locked_state->attach_state.size() > this->max_attachments_) {
    return batt::StatusCode::kInternal;  // TODO [tastolfi 2024-03-20]
  }
  for (const auto& [user_id, pending_txns] : locked_state->pending_recovery) {
    if (!locked_state->attach_state.count(user_id)) {
      return batt::StatusCode::kInternal;  // TODO [tastolfi 2024-03-20]
    }
  }

  // Populate the free pool.
  //
  {
    usize push_count = 0;
    for (i64 physical_page = 0; physical_page < physical_page_count; ++physical_page) {
      PageAllocatorState::PageState& page_state = locked_state->page_state[physical_page];
      if (page_state.ref_count == 0) {
        const PageId page_id = this->page_ids_.make_page_id(physical_page, page_state.generation);
        BATT_CHECK(this->free_pool_.unsynchronized_push(page_id));
        ++push_count;
      }
    }
    this->free_pool_push_count_.fetch_add(push_count);
  }

  // Update recovering_user_count_; remove the initial count of 1 only after increasing by the
  // number of attachments found.
  //
  this->recovering_user_count_.fetch_add(locked_state->attach_state.size());
  this->recovering_user_count_.fetch_sub(1);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::start_checkpoint_task(batt::TaskScheduler& scheduler)
{
  this->checkpoint_task_.emplace(
      scheduler.schedule_task(),
      [this]() mutable {
        this->checkpoint_task_main();
      },
      /*name=*/
      batt::to_string("PageAllocator{", batt::c_str_literal(this->name_), "}::checkpoint_task"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::checkpoint_task_main() noexcept
{
  Status status = [&]() -> Status {
    const usize target_checkpoint_grant_pool_size = this->get_target_checkpoint_grant_pool_size();

    for (;;) {
      // Wait for new slots to be appended.
      //
      std::unique_ptr<LogDevice::Reader> log_reader =
          this->log_device_->new_reader(/*slot_lower_bound=*/None, LogReadMode::kSpeculative);

      BATT_REQUIRE_OK(log_reader->await(BytesAvailable{
          .size = target_checkpoint_grant_pool_size + PageAllocator::kCheckpointTargetSize,
      }));

      // Spend some of the checkpoint grant pool to write new checkpoints.
      //
      StatusOr<batt::Grant> checkpoint_grant = this->checkpoint_grant_pool_.spend(
          PageAllocator::kCheckpointTargetSize, batt::WaitForResource::kFalse);

      BATT_REQUIRE_OK(checkpoint_grant);

      // Append checkpoint slots to the log.
      //
      auto checkpoint_info = [&]() -> StatusOr<PageAllocatorState::CheckpointInfo> {
        batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

        return locked_state->append_checkpoint(*log_reader, *checkpoint_grant, this->slot_writer_);
      }();

      BATT_REQUIRE_OK(checkpoint_info);

      // Return the unused grant to the pool.
      //
      if (checkpoint_grant->size() != 0) {
        this->checkpoint_grant_pool_.subsume(std::move(*checkpoint_grant));
      }

      // Wait for checkpoint slots to be flushed.
      //
      BATT_REQUIRE_OK(this->log_device_->sync(
          LogReadMode::kDurable, SlotUpperBoundAt{
                                     .offset = checkpoint_info->checkpoint_slots.upper_bound,
                                 }));

      // Update the checkpoint trim lock.
      //
      const slot_offset_type slot_upper_bound =
          this->log_device_->slot_range(LogReadMode::kDurable).upper_bound;

      BATT_ASSIGN_OK_RESULT(
          this->checkpoint_trim_lock_,
          this->trim_control_->update_lock(std::move(this->checkpoint_trim_lock_),
                                           SlotRange{
                                               .lower_bound = checkpoint_info->trim_target,
                                               .upper_bound = slot_upper_bound,
                                           },
                                           "PageAllocator::checkpoint_task_main"));

      for (;;) {
        // Trim as much as we can (this may be limited by outstanding slot read locks from active
        // txns).
        //
        const slot_offset_type trim_pos = this->trim_control_->get_lower_bound();
        StatusOr<batt::Grant> trimmed = this->slot_writer_.trim_and_reserve(trim_pos);
        BATT_REQUIRE_OK(trimmed);

        if (trimmed->size() != 0) {
          this->checkpoint_grant_pool_.subsume(std::move(*trimmed));
        }

        // Refill grants.
        //
        if (this->checkpoint_grant_pool_.size() > target_checkpoint_grant_pool_size) {
          const u64 surplus =
              this->checkpoint_grant_pool_.size() - target_checkpoint_grant_pool_size;

          batt::Grant grant = BATT_OK_RESULT_OR_PANIC(
              this->checkpoint_grant_pool_.spend(surplus, batt::WaitForResource::kFalse));

          this->update_grant_.subsume(std::move(grant));
          break;

        } else {
          const u64 deficit =
              target_checkpoint_grant_pool_size - this->checkpoint_grant_pool_.size();

          if (deficit == 0) {
            break;
          }

          BATT_REQUIRE_OK(this->trim_control_->await_lower_bound(trim_pos + deficit));

          // Try again now that released slot locks have allowed us to trim more.
          //
          continue;
        }
      }
    }
  }();

  LLFS_VLOG(1) << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageAllocator::get_target_checkpoint_grant_pool_size() const noexcept
{
  return this->page_ids_.get_physical_page_count() * packed_sizeof_page_ref_count_slot() +
         this->max_attachments_ * packed_sizeof_page_allocator_attach_slot();
}

}  //namespace experimental
}  //namespace llfs
