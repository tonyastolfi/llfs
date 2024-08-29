//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator2.hpp>
//

#include <batteries/operators.hpp>
#include <batteries/stream_util.hpp>
#include <batteries/utility.hpp>

#include <boost/preprocessor/stringize.hpp>

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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BATT_OBJECT_PRINT_IMPL((), PageAllocatorState::CheckpointInfo,
                       (checkpoint_slots,  //
                        trim_target        //
                        ))

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageAllocatorMetrics

BATT_OBJECT_PRINT_IMPL((), PageAllocatorMetrics,
                       (allocate_ok_count,      //
                        allocate_error_count,   //
                        deallocate_count,       //
                        checkpoints_count,      //
                        checkpoint_slots_count  //
                        ))

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorMetrics::export_to(MetricRegistry& registry,
                                     const MetricLabelSet& labels) noexcept
{
#define LLFS_EXPORT_METRIC_(name)                                                                  \
  registry.add(BOOST_PP_STRINGIZE(name), this->name, batt::make_copy(labels))

  LLFS_EXPORT_METRIC_(allocate_ok_count);
  LLFS_EXPORT_METRIC_(allocate_error_count);
  LLFS_EXPORT_METRIC_(deallocate_count);
  LLFS_EXPORT_METRIC_(checkpoints_count);
  LLFS_EXPORT_METRIC_(checkpoint_slots_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocatorMetrics::unexport_from(MetricRegistry& registry) noexcept
{
  registry                                   //
      .remove(this->allocate_ok_count)       //
      .remove(this->allocate_error_count)    //
      .remove(this->deallocate_count)        //
      .remove(this->checkpoints_count)       //
      .remove(this->checkpoint_slots_count)  //
      ;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageAllocatorState

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageAllocatorState::PageAllocatorState(const PageIdFactory& page_ids,
                                                    SlotLockManager* trim_control,
                                                    PageAllocatorMetrics& metrics) noexcept
    : metrics_{metrics}
    , page_ids_{page_ids}
    , pending_recovery_{}
    , attach_state_{}
    , page_state_(page_ids.get_physical_page_count())
    , trim_control_{trim_control}
{
  std::memset(this->page_state_.data(), 0, sizeof(PageState) * this->page_state_.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot, const PackedPageAllocatorAttach& attach,
                                  bool in_recovery_mode) noexcept
{
  if (in_recovery_mode) {
    (void)this->pending_recovery_[attach.user_id];
  }

  AttachState& state = this->attach_state_[attach.user_id];

  state.last_update = slot.offset.upper_bound;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot [[maybe_unused]],
                                  const PackedPageAllocatorDetach& detach,
                                  bool in_recovery_mode) noexcept
{
  if (in_recovery_mode) {
    this->pending_recovery_.erase(detach.user_id);
  }

  this->attach_state_.erase(detach.user_id);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot, const PackedPageRefCount& ref_count,
                                  bool in_recovery_mode) noexcept
{
  return this->update_ref_count(slot.offset.upper_bound, ref_count, in_recovery_mode);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot, const PackedPageAllocatorTxn& txn,
                                  bool in_recovery_mode) noexcept
{
  if (in_recovery_mode) {
    SlotReadLock& txn_slot_lock =
        this->pending_recovery_[txn.user_id].pending_txns[txn.user_slot.value()];

    // There should be no read lock currently held for this user/slot; txns must have unique user
    // slot numbers!
    //
    if (txn_slot_lock) {
      return batt::StatusCode::kInternal;  // TODO [tastolfi 2024-03-20] be more specific
    }

    BATT_ASSIGN_OK_RESULT(txn_slot_lock, this->trim_control_->lock_slots(
                                             slot.offset, "PageAllocator::recover_impl()"));
  } else {
    BATT_CHECK_NE(this->attach_state_.count(txn.user_id), 0u)
        << "PageAllocator txn for user_id that is not attached!" << BATT_INSPECT(txn);
  }

  for (const PackedPageRefCount& pprc : txn.ref_counts) {
    BATT_REQUIRE_OK(this->update_ref_count(slot.offset.upper_bound, pprc, in_recovery_mode));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> PageAllocatorState::get_ref_count(PageId page_id) noexcept
{
  const u64 physical_page = this->page_ids_.get_physical_page(page_id);

  BATT_ASSIGN_OK_RESULT(PageRefCount prc, this->get_ref_count(PhysicalPageId{physical_page}));

  if (prc.page_id != page_id) {
    return {batt::StatusCode::kInvalidArgument};
  }

  return prc.ref_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageRefCount> PageAllocatorState::get_ref_count(PhysicalPageId physical_page) noexcept
{
  const u64 physical_page_count = this->page_ids_.get_physical_page_count();
  if (physical_page >= physical_page_count) {
    return {batt::StatusCode::kOutOfRange};
  }

  const PageAllocatorState::PageState& page_state = this->page_state_[physical_page];
  return PageRefCount{
      .page_id = this->page_ids_.make_page_id(physical_page, page_state.generation),
      .ref_count = page_state.ref_count,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update_ref_count(slot_offset_type slot_upper_bound,
                                            const PackedPageRefCount& pprc,
                                            bool in_recovery_mode) noexcept
{
  const PageId page_id = pprc.page_id.unpack();
  const u64 physical_page_count = this->page_ids_.get_physical_page_count();
  const u64 physical_page = this->page_ids_.get_physical_page(page_id);

  if (in_recovery_mode) {
    if (physical_page >= physical_page_count) {
      return ::llfs::make_status(::llfs::StatusCode::kPageAllocatorRecoveredPageOutOfRange);
    }
  } else {
    BATT_CHECK_LT(physical_page, physical_page_count);
  }

  PageState& page_state = this->page_state_[physical_page];

  page_state.ref_count = pprc.ref_count;
  page_state.last_update = slot_upper_bound;
  page_state.generation = this->page_ids_.get_generation(page_id);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::validate_recovered_state(MaxAttachments max_attachments) const noexcept
{
  if (this->pending_recovery_.size() > max_attachments) {
    return ::llfs::make_status(::llfs::StatusCode::kPageAllocatorTooManyRecoveredTxnUsers);
  }
  if (this->attach_state_.size() > max_attachments) {
    return ::llfs::make_status(::llfs::StatusCode::kPageAllocatorTooManyRecoveredAttachments);
  }
  for (const auto& [user_id, pending_txns] : this->pending_recovery_) {
    if (!this->attach_state_.count(user_id)) {
      return ::llfs::make_status(::llfs::StatusCode::kPageAllocatorRecoveredTxnUserNotAttached);
    }
  }

  // Cross-check the attach_state and pending_recovery maps.
  //
  for (const auto& kvp : this->attach_state_) {
    const boost::uuids::uuid& user_id = kvp.first;
    BATT_CHECK_EQ(this->pending_recovery_.count(user_id), 1)
        << "Found attached user with no pending recovery state!" << BATT_INSPECT(user_id);
  }
  for (const auto& kvp : this->pending_recovery_) {
    const boost::uuids::uuid& user_id = kvp.first;
    BATT_CHECK_EQ(this->attach_state_.count(user_id), 1)
        << "Found one or more txns for non-attached user: " << user_id;
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::map<slot_offset_type, SlotReadLock>> PageAllocatorState::get_pending_recovery(
    const boost::uuids::uuid& user_id) noexcept
{
  std::map<slot_offset_type, SlotReadLock> result;

  auto iter = this->pending_recovery_.find(user_id);
  if (iter == this->pending_recovery_.end()) {
    return {batt::StatusCode::kNotFound};
  }

  if (iter->second.recovery_started) {
    return {batt::StatusCode::kUnavailable};
  }

  std::swap(result, iter->second.pending_txns);
  iter->second.recovery_started = true;

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::notify_user_recovered(const boost::uuids::uuid& user_id,
                                                 bool all_users_recovered) noexcept
{
  auto iter = this->pending_recovery_.find(user_id);
  if (iter == this->pending_recovery_.end()) {
    return {batt::StatusCode::kNotFound};
  }

  // Check to see if a user is telling us it has completed recovery, but it hasn't asked for
  // pending txns.
  //
  if (!iter->second.recovery_started) {
    return {batt::StatusCode::kFailedPrecondition};
  }

  // We no longer need to track this user's recovery state.
  //
  this->pending_recovery_.erase(iter);

  BATT_CHECK_EQ(all_users_recovered, this->pending_recovery_.empty());

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageAllocatorState::init_free_page_pool(boost::lockfree::stack<PageId>& free_pool,
                                              boost::dynamic_bitset<>& durable_page_in_use) noexcept
{
  usize push_count = 0;

  const i64 physical_page_count =
      BATT_CHECKED_CAST(i64, this->page_ids_.get_physical_page_count().value());

  BATT_CHECK_EQ(this->page_ids_.get_physical_page_count().value(), this->page_state_.size());

  for (i64 physical_page = 0; physical_page < physical_page_count; ++physical_page) {
    const PageState& page_state = this->page_state_[physical_page];
    if (page_state.ref_count == 0) {
      const PageId page_id = this->page_ids_.make_page_id(physical_page, page_state.generation);
      BATT_CHECK_EQ(free_pool.unsynchronized_push(page_id), true);
      BATT_CHECK_EQ(durable_page_in_use.test(physical_page), false);
      ++push_count;
    } else {
      durable_page_in_use.set(physical_page, true);
    }
  }

  return push_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageAllocatorState::is_live_at_slot(slot_offset_type slot_upper_bound,
                                         PageId page_id) const noexcept
{
  const u64 physical_page_count = this->page_ids_.get_physical_page_count();
  const u64 physical_page = this->page_ids_.get_physical_page(page_id);
  BATT_CHECK_LT(physical_page, physical_page_count);

  return !slot_less_than(slot_upper_bound, this->page_state_[physical_page].last_update);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageAllocatorState::is_live_at_slot(slot_offset_type slot_upper_bound,
                                         const boost::uuids::uuid& user_id) const noexcept
{
  auto iter = this->attach_state_.find(user_id);

  return iter != this->attach_state_.end() &&
         !slot_less_than(slot_upper_bound, iter->second.last_update);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageAllocatorState::CheckpointInfo> PageAllocatorState::append_checkpoint(
    LogDevice::Reader& log_reader, batt::Grant& grant,
    TypedSlotWriter<PackedPageAllocatorEvent>& slot_writer)
{
  this->metrics_.checkpoints_count.add(1);

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

    // Skip this event if it is not the "live" update for the page or attachment.
    //
    if (!this->is_live_at_slot(slot.offset.upper_bound, Self::get_event_entity(event))) {
      return OkStatus();
    }

    StatusOr<SlotParseWithPayload<const EventT*>> new_slot =
        multi_append.typed_append(grant, event);

    if (!new_slot.ok()) {
      if (new_slot.status() == ::llfs::make_status(StatusCode::kSlotGrantTooSmall)) {
        return batt::StatusCode::kLoopBreak;
      }
      return new_slot.status();
    }

    this->metrics_.checkpoint_slots_count.add(1);

    return this->update(new_slot->slot, *new_slot->payload, /*in_recovery_mode=*/false);
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

  const u64 physical_page_count = this->page_ids_.get_physical_page_count();

  for (PageRefCount& prc : ref_count_updates) {
    // If the delta is zero then this should have been filtered out.
    //
    if (prc.ref_count == 0) {
      return batt::StatusCode::kInvalidArgument;
    }

    // Sanity check: make sure device id matches.
    //
    if (this->page_ids_.get_device_id(prc.page_id) != this->page_ids_.get_device_id()) {
      return batt::StatusCode::kInvalidArgument;
    }

    // Validate that the physical page is in range.
    //
    const u64 physical_page = this->page_ids_.get_physical_page(prc.page_id);
    if (physical_page >= physical_page_count) {
      return batt::StatusCode::kOutOfRange;
    }

    // Retrieve the current state.
    //
    const PageState& old_state = this->page_state_[physical_page];
    BATT_CHECK_GE(old_state.ref_count, 0) << "The ref count of a page must never be negative!";

    // Validate the page generation.
    //
    const page_generation_int old_generation = this->page_ids_.get_generation(prc.page_id);
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

      prc.page_id = this->page_ids_.make_page_id(physical_page, new_generation);
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
          return ::llfs::make_status(StatusCode::kPageAllocatorInitRefCountTooSmall);
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
  return 2 * batt::round_up_bits(PageAllocator::kCheckpointTargetSizeLog2,
                                 PageAllocator::kCheckpointTargetSize +
                                     physical_page_count * packed_sizeof_page_ref_count_slot() +
                                     max_attachments * packed_sizeof_page_allocator_attach_slot());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<PageAllocator>> PageAllocator::recover(
    const PageAllocatorRuntimeOptions& options, const PageIdFactory& page_ids,
    MaxAttachments max_attachments, LogDeviceFactory& log_device_factory) noexcept
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
  BATT_ASSIGN_OK_RESULT(SlotRange recovered_slot_range, page_allocator->recover_impl());

  // Start the free page task.
  //
  page_allocator->start_free_page_task(options.scheduler, recovered_slot_range.upper_bound);

  // Start background compaction task.
  //
  page_allocator->start_checkpoint_task(options.scheduler);

  // Success!
  //
  return {std::move(page_allocator)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> PageAllocator::recover_impl()
{
  Optional<SlotReadLock> tmp_slot_lock;

  batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

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

    // Acquire an initial lock on the entire recovered range; this will be released later when we've
    // figured out which slots can be trimmed.
    //
    BATT_ASSIGN_OK_RESULT(tmp_slot_lock, this->trim_control_->lock_slots(
                                             SlotRange{
                                                 .lower_bound = checkpoint_lock_range.lower_bound,
                                                 .upper_bound = checkpoint_lock_range.lower_bound,
                                             },
                                             "PageAllocator::recover_impl() - tmp_slot_lock"));

    {
      TypedSlotReader<PackedPageAllocatorEvent> slot_reader{*log_reader};

      BATT_REQUIRE_OK(slot_reader.run(batt::WaitForResource::kFalse,
                                      //----- --- -- -  -  -   -
                                      [&](const SlotParse& slot, const auto& event) -> Status {
                                        LLFS_VLOG(1) << BATT_INSPECT(slot) << " " << event;
                                        return locked_state->update(slot, event,
                                                                    /*in_recovery_mode=*/true);
                                      }));
    }
    checkpoint_lock_range.upper_bound = log_reader->slot_offset();
  }

  // Do another scan to find the current trim target.  Stop scanning when we find a "live" event
  // record.
  //
  {
    std::unique_ptr<LogDevice::Reader> log_reader =
        this->log_device_->new_reader(/*lower_bound=*/checkpoint_lock_range.lower_bound,
                                      /*mode=*/LogReadMode::kDurable);

    TypedSlotReader<PackedPageAllocatorEvent> slot_reader{*log_reader};

    StatusOr<usize> read_status = slot_reader.run(
        batt::WaitForResource::kFalse,
        //----- --- -- -  -  -   -
        [&](const SlotParse& slot, const PackedPageAllocatorAttach& attach) -> Status {
          if (locked_state->is_live_at_slot(slot.offset.upper_bound, attach.user_id)) {
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
          if (locked_state->is_live_at_slot(slot.offset.upper_bound, pprc.page_id.unpack())) {
            return batt::StatusCode::kLoopBreak;
          }
          checkpoint_lock_range.lower_bound = slot.offset.upper_bound;
          return OkStatus();
        },

        //----- --- -- -  -  -   -
        [&](const SlotParse& slot, const PackedPageAllocatorTxn& txn) -> Status {
          for (const PackedPageRefCount& pprc : txn.ref_counts) {
            if (locked_state->is_live_at_slot(slot.offset.upper_bound, pprc.page_id.unpack())) {
              return batt::StatusCode::kLoopBreak;
            }
          }
          checkpoint_lock_range.lower_bound = slot.offset.upper_bound;
          return OkStatus();
        });

    if (!read_status.ok() && read_status.status() != batt::StatusCode::kLoopBreak) {
      BATT_REQUIRE_OK(read_status);
    }

    LLFS_CHECK_SLOT_LE(checkpoint_lock_range.lower_bound, checkpoint_lock_range.upper_bound);
  }

  // Lock the "live" data range (i.e. slots not made obsolete by some later checkpoint or update).
  //
  LLFS_VLOG(1) << "Acquiring checkpoint trim lock on slot range " << checkpoint_lock_range
               << BATT_INSPECT(this->trim_control_->get_locked_range()) << " "
               << this->trim_control_->debug_info();

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
  BATT_REQUIRE_OK(locked_state->validate_recovered_state(this->max_attachments_));

  // Update recovering_user_count_; remove the initial count of 1 only after increasing by the
  // number of attachments found.
  //
  this->recovering_user_count_.fetch_add(
      BATT_CHECKED_CAST(i64, locked_state->attached_user_count()));

  const i64 observed_recovering_users = this->recovering_user_count_.fetch_sub(1) - 1;

  // Initialize the free pool if there are no attached users that need to run recovery.
  //
  if (observed_recovering_users == 0) {
    const usize push_count =
        locked_state->init_free_page_pool(this->free_pool_, this->durable_page_in_use_);

    this->free_pool_push_count_.fetch_add(push_count);
  }

  return checkpoint_lock_range;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocator::PageAllocator(std::string&& name, const PageIdFactory& page_ids, u64 max_attachments,
                             std::unique_ptr<LogDevice>&& log_device,
                             std::unique_ptr<SlotLockManager>&& trim_control) noexcept
    : metrics_{}
    , name_{std::move(name)}
    , page_ids_{page_ids}
    , max_attachments_{max_attachments}
    , recovering_user_count_{1}
    , free_pool_{}
    , free_pool_push_count_{0}
    , log_device_{std::move(log_device)}
    , trim_control_{std::move(trim_control)}
    , slot_writer_{*this->log_device_}
    , state_{this->page_ids_, this->trim_control_.get(), this->metrics_}
    , checkpoint_grant_pool_{BATT_OK_RESULT_OR_PANIC(
          this->slot_writer_.reserve(0, batt::WaitForResource::kFalse))}
    , update_grant_{BATT_OK_RESULT_OR_PANIC(
          this->slot_writer_.reserve(0, batt::WaitForResource::kFalse))}
    , checkpoint_task_{None}
    , durable_page_in_use_(this->page_ids_.get_physical_page_count())
    , free_page_log_reader_{nullptr}
    , free_page_slot_reader_{None}
    , free_page_task_{None}
{
  BATT_CHECK(this->free_pool_.is_lock_free());
  this->free_pool_.reserve(this->page_ids_.get_physical_page_count());

  // Register all metrics.
  //
  MetricLabelSet labels{
      MetricLabel{Token{"object_type"}, Token{"llfs_PageAllocator"}},
      MetricLabel{Token{"name"}, Token{this->name_}},
  };

  this->metrics_.export_to(global_metric_registry(), labels);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocator::~PageAllocator() noexcept
{
  this->halt();
  this->join();

  this->metrics_.unexport_from(global_metric_registry());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::pre_halt() noexcept
{
  this->halt_expected_.store(true);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::halt() noexcept
{
  this->pre_halt();
  this->recovering_user_count_.close();
  this->free_pool_push_count_.close();
  this->log_device_->halt();
  this->trim_control_->halt();
  this->slot_writer_.halt();
  this->update_grant_.revoke();
  if (this->free_page_slot_reader_) {
    this->free_page_slot_reader_->halt();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::join() noexcept
{
  if (this->checkpoint_task_) {
    this->checkpoint_task_->join();
    this->checkpoint_task_ = None;
  }
  if (this->free_page_task_) {
    this->free_page_task_->join();
    this->free_page_task_ = None;
  }
  this->log_device_->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::map<slot_offset_type, SlotReadLock>> PageAllocator::get_pending_recovery(
    const boost::uuids::uuid& user_id)
{
  batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

  return locked_state->get_pending_recovery(user_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocator::notify_user_recovered(const boost::uuids::uuid& user_id) noexcept
{
  const bool all_users_recovered = (this->recovering_user_count_.get_value() == 1);
  {
    batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

    BATT_REQUIRE_OK(locked_state->notify_user_recovered(user_id, all_users_recovered));

    if (all_users_recovered) {
      const usize push_count =
          locked_state->init_free_page_pool(this->free_pool_, this->durable_page_in_use_);

      this->free_pool_push_count_.fetch_add(push_count);
    }
  }

  const i64 prior_count = this->recovering_user_count_.fetch_sub(1);
  BATT_CHECK_GT(prior_count, 0);
  BATT_CHECK_EQ(all_users_recovered, prior_count == 1) << BATT_INSPECT(prior_count);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocator::require_users_recovered(batt::WaitForResource wait_for_resource) noexcept
{
  if (wait_for_resource == batt::WaitForResource::kFalse) {
    if (BATT_HINT_FALSE(this->recovering_user_count_.get_value() > 0)) {
      return {batt::StatusCode::kUnavailable};
    }
  } else {
    BATT_REQUIRE_OK(this->recovering_user_count_.await_equal(0));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> PageAllocator::attach_user(const boost::uuids::uuid& user_id) noexcept
{
  return this->process_attach_event(PackedPageAllocatorAttach{
      .user_id = user_id,
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> PageAllocator::detach_user(const boost::uuids::uuid& user_id) noexcept
{
  return this->process_attach_event(PackedPageAllocatorDetach{
      .user_id = user_id,
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageAllocator::is_user_attached(const boost::uuids::uuid& user_id)
{
  batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

  return locked_state->is_user_attached(user_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageId> PageAllocator::allocate_page(const boost::uuids::uuid& user_id,
                                              batt::WaitForResource wait_for_resource,
                                              const batt::CancelToken& cancel_token) noexcept
{
  // If some attached users are still recovering, then fail/block (to prevent accidental
  // re-allocation of a page that belonged to some page job that was partially committed).
  //
  BATT_REQUIRE_OK(this->require_users_recovered(wait_for_resource));

  if (!this->is_user_attached(user_id)) {
    return {::llfs::make_status(::llfs::StatusCode::kPageAllocatorNotAttached)};
  }

  for (;;) {
    // First observe the push count in case we need to wait for it to change below (but only if
    // wait_for_resource is true).
    //
    const u64 observed_push_count = (wait_for_resource == batt::WaitForResource::kTrue)
                                        ? this->free_pool_push_count_.get_value()
                                        : 0;

    // Try to pop a free page from the lock-free free_pool_ stack.
    //
    {
      PageId page_id;
      if (this->free_pool_.pop(page_id)) {
        this->metrics_.allocate_ok_count.add(1);
        return page_id;
      }
    }

    if (wait_for_resource == batt::WaitForResource::kFalse) {
      LLFS_LOG_INFO_FIRST_N(1) << "Unable to allocate page (pool is empty)"
                               << "; device=" << this->page_ids_.get_device_id();

      this->metrics_.allocate_error_count.add(1);
      return {batt::StatusCode::kResourceExhausted};
    }

    // Wait until the allocate has been cancelled (via `cancel_token`) or the value of
    // `this->free_pool_push_count_` changes.
    //
    BATT_DEBUG_INFO("[PageAllocator::allocate_page] waiting for free page");
    if (cancel_token) {
      StatusOr<u64> new_count = cancel_token.await<u64>([&](auto&& handler) {
        this->free_pool_push_count_.async_wait(observed_push_count, BATT_FORWARD(handler));
      });
      BATT_REQUIRE_OK(new_count) << BATT_INSPECT(cancel_token.debug_info());
    } else {
      BATT_REQUIRE_OK(this->free_pool_push_count_.await_not_equal(observed_push_count));
    }
    //
    // Loop back around to try again...
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::deallocate_page(PageId page_id)
{
  this->metrics_.deallocate_count.add(1);

  const bool success = this->free_pool_.push(page_id);
  BATT_CHECK(success);

  this->free_pool_push_count_.fetch_add(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocator::sync(slot_offset_type min_slot) noexcept
{
  return this->log_device_->sync(LogReadMode::kDurable, SlotUpperBoundAt{
                                                            .offset = min_slot,
                                                        });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i32> PageAllocator::get_ref_count(PageId page_id) noexcept
{
  batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

  return locked_state->get_ref_count(page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageRefCount> PageAllocator::get_ref_count(PhysicalPageId physical_page) noexcept
{
  batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

  return locked_state->get_ref_count(physical_page);
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// private

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::start_checkpoint_task(batt::TaskScheduler& scheduler)
{
  BATT_CHECK_NOT_NULLPTR(this->free_page_log_reader_)
      << "start_free_page_task must be called before start_checkpoint_task";
  BATT_CHECK_NE(this->free_page_slot_reader_, None);
  BATT_CHECK_EQ(this->checkpoint_task_, None);

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

      LLFS_VLOG(1) << "[PageAllocator::checkpoint_task] waiting for log to grow;"
                   << BATT_INSPECT(log_reader->slot_offset());

      BATT_REQUIRE_OK(log_reader->await(BytesAvailable{
          .size = target_checkpoint_grant_pool_size + PageAllocator::kCheckpointTargetSize,
      }));

      LLFS_VLOG(1) << "[PageAllocator::checkpoint_task]" << BATT_INSPECT(this->log_device_->size())
                   << BATT_INSPECT(this->checkpoint_grant_pool_.size());

      // Spend some of the checkpoint grant pool to write new checkpoints.
      //
      StatusOr<batt::Grant> checkpoint_grant = this->checkpoint_grant_pool_.spend(
          PageAllocator::kCheckpointTargetSize, batt::WaitForResource::kFalse);

      BATT_REQUIRE_OK(checkpoint_grant);

      LLFS_VLOG(1) << "[PageAllocator::checkpoint_task] writing checkpoint";

      // Append checkpoint slots to the log.
      //
      auto checkpoint_info = [&]() -> StatusOr<PageAllocatorState::CheckpointInfo> {
        batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

        return locked_state->append_checkpoint(*log_reader, *checkpoint_grant, this->slot_writer_);
      }();

      LLFS_VLOG(1) << "[PageAllocator::checkpoint_task]" << BATT_INSPECT(checkpoint_info)
                   << BATT_INSPECT(this->metrics_);

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

      const auto new_lock_range = SlotRange{
          .lower_bound = checkpoint_info->trim_target,
          .upper_bound = slot_upper_bound,
      };

      LLFS_VLOG(1) << "[PageAllocator::checkpoint_task] updating checkpoint_trim_lock_; "
                   << this->checkpoint_trim_lock_.slot_range() << " -> " << new_lock_range;

      BATT_ASSIGN_OK_RESULT(
          this->checkpoint_trim_lock_,
          this->trim_control_->update_lock(std::move(this->checkpoint_trim_lock_), new_lock_range,
                                           "PageAllocator::checkpoint_task_main"));

      LLFS_VLOG(1) << "[PageAllocator::checkpoint_task]"
                   << BATT_INSPECT(this->trim_control_->debug_info());

      for (;;) {
        // Trim as much as we can (this may be limited by outstanding slot read locks from active
        // txns).
        //
        const slot_offset_type trim_pos = this->trim_control_->get_lower_bound();

        // Wait for the free page reader task to catch up to the trim point.
        //
        BATT_REQUIRE_OK(this->free_page_slot_reader_->await_consumed_upper_bound(trim_pos));

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

          BATT_DEBUG_INFO("[PageAllocator::checkpoint_task] awaiting slot read lock release;"
                          << BATT_INSPECT(deficit) << BATT_INSPECT(trim_pos)
                          << BATT_INSPECT(target_checkpoint_grant_pool_size)
                          << BATT_INSPECT(this->checkpoint_grant_pool_.size())
                          << BATT_INSPECT(this->trim_control_->debug_info()));

          BATT_REQUIRE_OK(this->trim_control_->await_lower_bound(trim_pos + deficit));

          // Try again now that released slot locks have allowed us to trim more.
          //
          continue;
        }
      }
    }
  }();

  if (this->halt_expected_.load()) {
    LLFS_VLOG(1) << BATT_INSPECT(status);
  } else {
    LLFS_LOG_WARNING() << BATT_INSPECT(status);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::start_free_page_task(batt::TaskScheduler& scheduler,
                                         slot_offset_type recovered_slot_upper_bound)
{
  BATT_CHECK_EQ(this->free_page_log_reader_, nullptr);
  BATT_CHECK_EQ(this->free_page_slot_reader_, None);
  BATT_CHECK_EQ(this->free_page_task_, None);

  this->free_page_log_reader_ =
      this->log_device_->new_reader(recovered_slot_upper_bound, LogReadMode::kDurable);

  this->free_page_slot_reader_.emplace(*this->free_page_log_reader_);

  this->free_page_task_.emplace(
      scheduler.schedule_task(),
      [this]() mutable {
        this->free_page_task_main();
      },
      /*name=*/
      batt::to_string("PageAllocator{", batt::c_str_literal(this->name_), "}::free_page_task"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::free_page_task_main() noexcept
{
  BATT_CHECK_NOT_NULLPTR(this->free_page_log_reader_);
  BATT_CHECK_NE(this->free_page_slot_reader_, None);

  Status status = [&]() -> Status {
    // First wait for all users to finish recovery.
    //
    BATT_REQUIRE_OK(this->require_users_recovered(batt::WaitForResource::kTrue));

    // Helper function - given a PackedPageRefCount, update the `durable_page_in_use_` bit set and
    // push newly freed pages onto `this->free_pool_`.
    //
    const auto update_page_state =
        [this, physical_page_count =
                   this->page_ids_.get_physical_page_count()](const PackedPageRefCount& pprc) {
          const PageId page_id = pprc.page_id.unpack();
          const u64 physical_page = this->page_ids_.get_physical_page(page_id);

          BATT_CHECK_LT(physical_page, physical_page_count);
          BATT_CHECK_GE(pprc.ref_count, 0);

          const bool old_in_use = this->durable_page_in_use_.test(physical_page);
          const bool new_in_use = (pprc.ref_count != 0);

          if (old_in_use != new_in_use) {
            if (!new_in_use) {
              const bool success = this->free_pool_.push(page_id);
              BATT_CHECK(success);

              const u64 prior_count = this->free_pool_push_count_.fetch_add(1);
              const u64 new_count = prior_count + 1;

              BATT_CHECK_GT(new_count, prior_count);
            }
            this->durable_page_in_use_.set(physical_page, new_in_use);
          }
        };

    // Do a blocking read of all durable Txn and Page Ref Count events as they are appended to the
    // log and flushed.
    //
    BATT_REQUIRE_OK(this->free_page_slot_reader_->run(
        batt::WaitForResource::kTrue,
        //----- --- -- -  -  -   -
        [&](const SlotParse& /*slot*/, const PackedPageAllocatorTxn& txn) -> Status {
          for (const PackedPageRefCount& pprc : txn.ref_counts) {
            update_page_state(pprc);
          }
          return OkStatus();
        },
        //----- --- -- -  -  -   -
        [&](const SlotParse& /*slot*/, const PackedPageRefCount& pprc) -> Status {
          update_page_state(pprc);
          return OkStatus();
        },
        //----- --- -- -  -  -   -
        [&](const SlotParse& /*slot*/, const PackedPageAllocatorAttach& /*ignored*/) -> Status {
          return OkStatus();
        },
        //----- --- -- -  -  -   -
        [&](const SlotParse& /*slot*/, const PackedPageAllocatorDetach& /*ignored*/) -> Status {
          return OkStatus();
        }));

    return OkStatus();
  }();

  if (this->halt_expected_.load()) {
    LLFS_VLOG(1) << BATT_INSPECT(status);
  } else {
    LLFS_LOG_WARNING() << BATT_INSPECT(status);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageAllocator::get_target_checkpoint_grant_pool_size() const noexcept
{
  return batt::round_up_bits(
      PageAllocator::kCheckpointTargetSizeLog2,
      this->page_ids_.get_physical_page_count() * packed_sizeof_page_ref_count_slot() +
          this->max_attachments_ * packed_sizeof_page_allocator_attach_slot());
}

}  //namespace experimental
}  //namespace llfs
