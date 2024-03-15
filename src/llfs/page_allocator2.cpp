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
std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorTxn& t)
{
  return out << "PackedPageAllocatorTxn{.user_id=" << t.user_id << ", .user_slot=" << t.user_slot
             << ", .ref_counts=" << batt::dump_range(t.ref_counts) << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof_page_allocator_txn(usize n_ref_counts)
{
  return sizeof(PackedPageAllocatorTxn) + sizeof(PackedPageRefCount) * n_ref_counts;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedPageAllocatorTxn& txn)
{
  return packed_sizeof_page_allocator_txn(txn.ref_counts.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof_page_ref_count_slot() noexcept
{
  static const usize size_ = packed_sizeof_slot_with_payload_size(sizeof(PackedPageRefCount));
  return size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize get_page_allocator_txn_grant_size(usize n_ref_counts)
{
  return packed_sizeof_slot_with_payload_size(packed_sizeof_page_allocator_txn(n_ref_counts)) +
         n_ref_counts * packed_sizeof_page_ref_count_slot();
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
/*explicit*/ PageAllocatorState::PageAllocatorState(const PageIdFactory& page_ids) noexcept
    : page_ids{page_ids}
    , in_recovery_mode{true}
    , pending_recovery{}
    , attach_state{}
    , page_state(page_ids.get_physical_page_count())
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

  state.user_id = attach.user_id;
  state.last_update = slot.offset.upper_bound;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot,
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
Status PageAllocatorState::update(const SlotParse& slot, const PackedPageAllocatorTxn& txn) noexcept
{
  if (this->in_recovery_mode) {
    auto iter = this->attach_state.find(txn.user_id);

    if (BATT_HINT_FALSE(iter == this->attach_state.end())) {
      return batt::StatusCode::kDataLoss;  // TODO [tastolfi 2024-03-22] be more specific
    }

    {
      AttachState& attach_state = iter->second;
      attach_state.last_update = slot_max(attach_state.last_update, slot.offset.upper_bound);
    }
    SlotReadLock& txn_slot_lock = this->pending_recovery[txn.user_id][txn.user_slot.value()];

    // There should be no read lock currently held for this user/slot; txns must have unique user
    // slot numbers!
    //
    if (txn_slot_lock) {
      return batt::StatusCode::kInternal;  // TODO [tastolfi 2024-03-20] be more specific
    }

    BATT_ASSIGN_OK_RESULT(txn_slot_lock, this->trim_control_->lock_slots(
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
Status PageAllocatorState::update_ref_count(slot_offset_type slot_upper_bound,
                                            const PackedPageRefCount& pprc) noexcept
{
  const u64 physical_page_count = this->page_ids.get_physical_page_count();

  const PageId page_id = pprc.page_id.unpack();
  const i64 physical_page = this->page_ids_.get_physical_page(page_id);

  if (static_cast<u64>(physical_page) >= physical_page_count) {
    return batt::StatusCode::kDataLoss;  // TODO [tastolfi 2024-03-19] be more specific
  }

  PageState& state = this->page_state[physical_page];

  state.ref_count = pprc.ref_count;
  state.last_update = slot_upper_bound;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageAllocatorState::get_target_checkpoint_grant_size() const noexcept
{
  return this->active_txn_ref_counts * packed_sizeof_page_ref_count_slot() +
         sizeof(PackedAllocatorAttach) * this->attach_state.size();
}

#if 0
    
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ u64 PageAllocator::calculate_log_size(u64 physical_page_count)
{
  return PageAllocator::kCheckpointGrantThreshold * 4 +
         packed_sizeof_page_allocator_txn(1) * physical_page_count * 3;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::unique_ptr<PageAllocator>> PageAllocator::recover(
    const PageAllocatorRuntimeOptions& options, const PageIdFactory& page_ids, u64 max_attachments,
    LogDeviceFactory& log_device_factory)
{
  // Recover the page allocator log.
  //
  BATT_ASSIGN_OK_RESULT(std::unique_ptr<LogDevice> recovered_log,
                        open_log_device_no_scan(log_device_factory));

  // Create the PageAllocator object.
  //
  std::unique_ptr<PageAllocator> page_allocator{
      new PageAllocator{std::string{options.name}, page_ids, std::move(recovered_log),
                        std::make_unique<SlotLockManager>()}};

  // Recover state from the log.
  //
  BATT_REQUIRE_OK(page_allocator->recover_impl());

  // Start background compaction task.
  //
  page_allocator->start_compaction_task(options.scheduler);

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
    , state_{this->page_ids_.get_physical_page_count()}
    , read_only_ref_counts_{as_slice(this->state_.lock()->ref_counts.get(),
                                     this->page_ids_.get_physical_page_count())}
    , append_grant_{BATT_OK_RESULT_OR_PANIC(
          this->state_.lock()->slot_writer.reserve(0, batt::WaitForResource::kFalse))}
    , compaction_task_{None}
{
  BATT_CHECK(this->free_pool_.is_lock_free());
  this->free_pool_.reserve(this->page_ids_.get_physical_page_count());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<BaseState> PageAllocator::recover_impl()
{
  batt::ScopedLock<PageAllocator::State> locked_state{this->state_};

  const i64 physical_page_count =
      BATT_CHECKED_CAST(i64, this->page_ids_.get_physical_page_count().value());

  // Scan the log slots to restore state.
  //
  {
    std::unique_ptr<LogDevice::Reader> log_reader =
        this->log_device_->new_reader(/*lower_bound=*/None, /*mode=*/LogReadMode::kDurable);

    compacted_upper_bound = log_reader->slot_offset();

    TypedSlotReader<PackedPageAllocatorEvent> slot_reader{*log_reader};

    BATT_REQUIRE_OK(slot_reader.run(batt::WaitForResource::kFalse,
                                    //----- --- -- -  -  -   -
                                    [&](const SlotParse& slot, const auto& event) -> Status {
                                      return locked_state->update(slot, event);
                                    }));

    this->trim_control_->update_upper_bound(log_reader->slot_offset());
  }

  // Now that we have finished scanning the log, we can remove deteched users; we needed them before
  // to prevent a Checkpoint event found after a Detach event from incorrectly re-attaching a user.
  //
  locked_state->prune_detached_users();

  // Sanity checks.
  //
  if (locked_state->pending_recovery.size() > this->max_attachments_) {
    return batt::StatusCode::kInternal;  // TODO [tastolfi 2024-03-20]
  }
  if (locked_state->attached_users.size() > this->max_attachments_) {
    return batt::StatusCode::kInternal;  // TODO [tastolfi 2024-03-20]
  }
  for (const auto& [user_id, pending_txns] : locked_state->pending_recovery) {
    if (!locked_state->attached_users.count(user_id)) {
      return batt::StatusCode::kInternal;  // TODO [tastolfi 2024-03-20]
    }
  }

  // Populate the free pool.
  //
  for (i64 physical_page = 0; physical_page < physical_page_count; ++physical_page) {
    if (locked_state->page_states[physical_page].ref_count == 0) {
      const i64 new_generation = base_state.page_generation[physical_page] + 1;
      BATT_CHECK_GE(new_generation, 0);

      const PageId page_id = this->page_ids_.make_page_id(physical_page, new_generation);
      BATT_CHECK(this->free_pool_.unsynchronized_push(page_id));
    }
  }

  // Update recovering_user_count_; remove the initial count of 1 only after increasing by the
  // number of attachments found.
  //
  this->recovering_user_count_.fetch_add(locked_state->attached_users.size());
  this->recovering_user_count_.fetch_sub(1);

  return {std::move(base_state)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::start_checkpoint_task(batt::TaskScheduler& scheduler)
{
  this->checkpoint_task_.emplace(
      scheduler.schedule_task(),
      [this, base_state = std::move(base_state)]() mutable {
        this->checkpoint_task_main(std::move(base_state));
      },
      /*name=*/
      batt::to_string("PageAllocator{", batt::c_str_literal(this->name_), "}::checkpoint_task"));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::checkpoint_task_main() noexcept
{
  Status status = [&]() -> Status {
    std::unique_ptr<LogDevice::Reader> log_reader =
        this->log_device_->new_reader(None, LogReadMode::kDurable);

    TypedSlotReader<PackedPageAllocatorEvent> slot_reader{*log_reader};

    // The purpose of the checkpoint SlotPosition is to optimize the case where we run out of Grant
    // to write the next checkpoint in the middle of a visited slot.  It's technically not required
    // for correctness, as we only include attachments and ref counts for which the visited slot is
    // the most recent update, so anything in a slot that's included in a checkpoint will be skipped
    // the next time its visited.  But tracking the index of the last attachment and ref count we
    // included at a given slot allows us to avoid re-scanning over items that can be skipped.
    //
    PageAllocatorCheckpointBuilder::SlotPosition checkpoint_slot_position;
    checkpoint_slot_position.slot_lower_bound = log_reader->slot_offset();

    // Main loop.
    //
    for (;;) {
      // Wait for checkpoint grant to accumulate.
      //
      StatusOr<batt::Grant> grant = this->checkpoint_grant_.spend(
          PageAllocator::kCheckpointGrantThreshold, batt::WaitForResource::kTrue);

      BATT_REQUIRE_OK(grant);

      // Give whatever remains of the local grant back to this->checkpoint_grant_ when the loop
      // exits.
      //
      auto on_scope_exit = batt::finally([&] {
        this->checkpoint_grant_.subsume(std::move(*grant));
      });

      // Wait for trim locks to be released.
      //
      StatusOr<slot_offset_type> observed_trim_slot = this->trim_control_->await_lower_bound(
          log_reader->slot_offset() + PageAllocator::kCheckpointGrantThreshold);

      BATT_REQUIRE_OK(observed_trim_slot);

      // Lock the state and write checkpoint.
      //
      Optional<slot_offset_type> sync_offset;
      {
        batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

        PageAllocatorCheckpointBuilder checkpoint_builder{
            *locked_state, /*slot_size_limit=*/PageAllocator::kCheckpointGrantThreshold,
            /*slot_upper_bound=*/*observed_trim_slot, &checkpoint_slot_position};

        BATT_REQUIRE_OK(slot_reader.run(batt::WaitForData::kFalse,
                                        [&](const SlotParse& slot, const auto& payload) {
                                          return checkpoint_builder.visit(slot, payload);
                                        }));

        StatusOr<SlotParseWithPayload<const PackedPageAllocatorCheckpoint*>> packed_checkpoint =
            this->slot_writer_.typed_append(*grant, checkpoint_builder);

        BATT_REQUIRE_OK(packed_checkpoint);

        sync_offset = packed_checkpoint->slot.offset.upper_bound;

        // Update the timestamps for all pages and attachments in the checkpoint.
        //
        BATT_REQUIRE_OK(locked_state->update(packed_checkpoint->slot, *packed_checkpoint->payload));
      }

      BATT_CHECK(sync_offset);

      BATT_REQUIRE_OK(
          this->log_device_->sync(LogReadMode::kDurable, SlotUpperBoundAt{.offset = *sync_offset}));

      // Sanity check: make sure the checkpoint builder stopped the slot reader at the current trim
      // point.
      //
      BATT_CHECK(
          !slot_less_than(this->trim_control_->get_lower_bound(), slot_reader.next_slot_offset()));

      BATT_REQUIRE_OK(this->slot_writer_->trim(slot_reader.next_slot_offset()));
    }
  }();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorState::update(const SlotParse& slot,
                                  const PackedPageAllocatorCheckpoint& checkpoint) noexcept
{
  if (this->in_recovery_mode) {
    if (checkpoint.attachments) {
      for (const PackedPageAllocatorAttachment& attachment : *checkpoint.attachments) {
        BATT_REQUIRE_OK(this->update(slot, attachment));
      }
    }

    BATT_REQUIRE_OK(this->update_ref_counts(slot.offset.upper_bound, checkpoint.ref_counts));

  } else {
    const slot_offset_type slot_upper_bound = slot.offset.upper_bound;

    // When not in recovery mode, checkpoints should *never* change the state of an attachment or
    // page ref count, so panic below if the current state disagrees with the checkpoint.

    if (checkpoint.attachments) {
      for (const PackedPageAllocatorAttachment& attachment : *checkpoint.attachments) {
        AttachState& attach_state = this->attach_states[attachment.index];

        BATT_CHECK_EQ(attach_state.user_id, attachment.user_id);
        attach_state.last_update = slot_upper_bound;
      }
    }

    const usize physical_page_count = this->page_ids.get_physical_page_count();

    for (const PackedPageRefCount pprc : checkpoint.ref_counts) {
      const PageId page_id = pprc.page_id.unpack();
      const i64 physical_page = this->page_ids_.get_physical_page(page_id);

      BATT_CHECK_LT(static_cast<usize>(physical_page), physical_page_count);

      PageState& page_state = this->page_states[physical_page];

      BATT_CHECK_EQ(page_state.ref_count, pprc.ref_count.value());
      page_state.last_update = slot_upper_bound;
    }
  }

  return OkStatus();
}


//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageAllocatorState::page_updated_since(slot_offset_type slot_upper_bound,
                                            PageId page_id) const noexcept
{
  const i64 physical_page = this->page_ids.get_physical_page(page_id);
  BATT_CHECK_LT(static_cast<usize>(physical_page), this->page_states.size());
  return slot_less_than(slot_upper_bound, this->page_states[physical_page].last_update);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageAllocatorState::attachment_updated_since(slot_offset_type slot_upper_bound,
                                                  const boost::uuids::uuid user_id) const noexcept
{
  auto iter = this->attach_states.find(user_id);

  return iter == this->attached_users.end() || !iter->second.attached;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i32 PageAllocatorState::get_ref_count(PageId page_id) const noexcept
{
  const u64 physical_page_count = page_ids.get_physical_page_count();
  const i64 physical_page = this->page_ids_.get_physical_page(page_id);

  BATT_CHECK_LT(static_cast<u64>(physical_page), physical_page_count);

  return this->page_states[physical_page].ref_count;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PageAllocatorCheckpointBuilder

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageAllocatorCheckpointBuilder::PageAllocatorCheckpointBuilder(
    const PageAllocatorState& state, usize slot_size_limit,
    slot_offset_type slot_upper_bound) noexcept
    : state_{state}
    , slot_size_limit_{slot_size_limit}
    , slot_upper_bound_{slot_upper_bound}
    , payload_size_{0}
    , attachments_{}
    , pages_{}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorCheckpointBuilder::visit(
    const SlotParse& slot, const PackedPageAllocatorAttachment& attachment) noexcept
{
  BATT_REQUIRE_OK(this->enter_visit(slot));

  return this->visit_attachment(slot.offset.upper_bound, attachment);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorCheckpointBuilder::visit(
    const SlotParse& slot, const PackedPageAllocatorCheckpoint& checkpoint) noexcept
{
  BATT_REQUIRE_OK(this->enter_visit(slot));

  if (checkpoint.attachments) {
    auto next = checkpoint.attachments.begin() + this->progress_->attachment_i;
    auto last = checkpoint.attachments.end();

    for (; next != last; ++next) {
      BATT_REQUIRE_OK(this->visit_attachment(slot.offset.upper_bound, *next));
    }
  }

  return this->visit_ref_counts(checkpoint.ref_counts);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorCheckpointBuilder::visit(const SlotParse& slot,
                                             const PackedPageAllocatorTxn& txn) noexcept
{
  BATT_REQUIRE_OK(this->enter_visit(slot));

  return this->visit_ref_counts(txn.ref_counts);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorCheckpointBuilder::enter_visit(const SlotParse& slot) noexcept
{
  if (slot_less_than(this->slot_upper_bound_, slot.offset.upper_bound)) {
    return batt::StatusCode::kLoopBreak;
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorCheckpointBuilder::visit_attachment(  //
    slot_offset_type slot_upper_bound,                    //
    const PackedPageAllocatorAttachment& attachment) noexcept
{
  if (!this->state_.attachment_updated_since(slot_upper_bound)) {
    BATT_REQUIRE_OK(this->increase_payload_size(
        sizeof(boost::uuids::uuid) +
        (this->attachments_.empty() ? PackedArray<PackedPageAllocatorAttachment> : 0)));

    this->attachments_.emplace_back(index);
  }
  this->position_->attachment_i += 1;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorCheckpointBuilder::visit_ref_counts(
    const SlotParse& slot, const PackedArray<PackedPageRefCount>& ref_counts) noexcept
{
  auto next = ref_counts.begin() + this->position_->ref_count_i;
  auto last = ref_counts.end();

  for (; next != last; ++next) {
    BATT_REQUIRE_OK(this->visit_ref_count(slot.offset.upper_bound, pprc));
  }

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorCheckpointBuilder::visit_ref_count(  //
    slot_offset_type slot_upper_bound,                   //
    const PackedPageRefCount& pprc) noexcept
{
  if (!this->state_.page_updated_since(slot_upper_bound)) {
    BATT_REQUIRE_OK(this->increase_payload_size(sizeof(PackedPageRefCount)));

    this->pages_.emplace_back(page_id);
  }
  this->position_->ref_count_i += 1;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocatorCheckpointBuilder::increase_payload_size(usize n_bytes) const noexcept
{
  if (packed_sizeof_slot_with_payload_size(this->payload_size_ + n_bytes) >
      this->slot_size_limit_) {
    return batt::StatusCode::kBreakLoop;
  }

  this->payload_size_ += n_bytes;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageAllocatorCheckpointBuilder::packed_size() const noexcept
{
  return this->payload_size_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PackedPageAllocatorCheckpoint* PageAllocatorCheckpointBuilder::pack_to(
    PackedPageAllocatorCheckpoint* packed, DataPacker* dst) const noexcept
{
  packed->ref_counts.initialize(0);
  {
    BasicArrayPacker<PackedPageRefCount> array_packer{&packed->ref_counts, dst};
    for (PageId page_id : this->pages_) {
      if (!array_packer.pack_item(PageRefCount{
              .page_id = page_id,
              .ref_count = this->state_.get_ref_count(page_id),
          })) {
        return nullptr;
      }
    }
  }

  if (!this->attachments_.empty()) {
    PackedArray<PackedPageAllocatorAttachment>* packed_attachments =
        dst->pack_record<PackedArray<PackedPageAllocatorAttachment>>();

    if (!packed_attachments) {
      return nullptr;
    }

    BasicArrayPacker<PackedPageAllocatorAttachment> array_packer{packed_attachments, dst};
    for (u32 index : this->attachments_) {
      if (!array_packer.pack_item(PackedPageAllocatorAttachment{
              .user_id = this->state_.attach_states[index].user_id,
              .index = index,
          })) {
        return nullptr;
      }
    }

    packed->attachments.reset(packed_attachments, dst);
  }

  return packed;
}

#endif

}  //namespace experimental
}  //namespace llfs
