//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR2_HPP
#define LLFS_PAGE_ALLOCATOR2_HPP

#include <llfs/config.hpp>
//
#include <llfs/constants.hpp>
#include <llfs/data_packer.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/log_device.hpp>
#include <llfs/metrics.hpp>
#include <llfs/optional.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/packed_page_ref_count.hpp>
#include <llfs/packed_variant.hpp>
#include <llfs/page_allocator_runtime_options.hpp>
#include <llfs/page_size.hpp>
#include <llfs/simple_packed_type.hpp>
#include <llfs/slot_lock_manager.hpp>
#include <llfs/slot_read_lock.hpp>
#include <llfs/slot_reader.hpp>
#include <llfs/slot_writer.hpp>
#include <llfs/uuid.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/async/grant.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/watch.hpp>

#include <boost/dynamic_bitset.hpp>
#include <boost/lockfree/stack.hpp>

namespace llfs {
namespace experimental {

struct PackedPageAllocatorAttach;
struct PackedPageAllocatorDetach;
struct PackedPageAllocatorTxn;

using PackedPageAllocatorEvent = PackedVariant<  //
    PackedPageAllocatorAttach,                   //
    PackedPageAllocatorDetach,                   //
    PackedPageRefCount,                          //
    PackedPageAllocatorTxn                       //
    >;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// NOTE: We need attachments, even in the new design, so that the allocator knows not to enter
// normal operation mode until all attached clients have notified that they are recovered.
//
struct PackedPageAllocatorAttach {
  boost::uuids::uuid user_id;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageAllocatorAttach), 16);

LLFS_SIMPLE_PACKED_TYPE(PackedPageAllocatorAttach);

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorAttach& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct PackedPageAllocatorDetach {
  boost::uuids::uuid user_id;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageAllocatorDetach), 16);

LLFS_SIMPLE_PACKED_TYPE(PackedPageAllocatorDetach);

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorDetach& t);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// PageAllocator Txn

usize packed_sizeof_page_allocator_txn(usize n_ref_counts);

struct PageAllocatorTxn {
  boost::uuids::uuid user_id;
  slot_offset_type user_slot;
  Slice<const PageRefCount> ref_counts;
};

std::ostream& operator<<(std::ostream& out, const PageAllocatorTxn& t);

LLFS_DEFINE_PACKED_TYPE_FOR(PageAllocatorTxn, PackedPageAllocatorTxn);

PackedPageAllocatorTxn* pack_object_to(const PageAllocatorTxn& from, PackedPageAllocatorTxn* to,
                                       DataPacker* dst);

usize packed_sizeof(const PageAllocatorTxn& txn);

//+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedPageAllocatorTxn {
  boost::uuids::uuid user_id;                  // 16
  PackedSlotOffset user_slot;                  // 8
  PackedArray<PackedPageRefCount> ref_counts;  // 8
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageAllocatorTxn), 32);

LLFS_IS_SELF_CONTAINED_PACKED_TYPE(PackedPageAllocatorTxn, false);

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorTxn& t);

usize packed_sizeof(const PackedPageAllocatorTxn& txn);

Status validate_packed_value(const PackedPageAllocatorTxn& txn, const void* buffer_data,
                             usize buffer_size);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

usize packed_sizeof_page_allocator_attach_slot() noexcept;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct PageAllocatorMetrics {
  CountMetric<u64> allocate_ok_count{0};
  CountMetric<u64> allocate_error_count{0};
  CountMetric<u64> deallocate_count{0};
  CountMetric<u64> checkpoints_count{0};
  CountMetric<u64> checkpoint_slots_count{0};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Exports all collectors in this object to the passed registry, adding the passed labels.
   *
   * IMPORTANT: Once export_to has been called, this->unexport_from(registry) must be called before
   * this object goes out of scope!
   */
  void export_to(MetricRegistry& registry, const MetricLabelSet& labels) noexcept;

  /** \brief Removes all previously exported collectors associated with this object from the passed
   * registry.
   */
  void unexport_from(MetricRegistry& registry) noexcept;
};

std::ostream& operator<<(std::ostream& out, const PageAllocatorMetrics& t);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class PageAllocatorState
{
 public:
  using Self = PageAllocatorState;

  //----- --- -- -  -  -   -

  struct UserRecoveryState {
    bool recovery_started = false;
    std::map<slot_offset_type, SlotReadLock> pending_txns;
  };

  struct PageState {
    /** \brief The slot upper bound of the most recent update to this page.
     */
    slot_offset_type last_update;

    /** \brief The current reference count for the page.
     */
    i32 ref_count;

    /** \brief The current generation for the page; this is incremented when ref count goes from 1
     * to 0, so when we recover a page with ref_count == 0, we can just use the given generation
     * to populate the free pool.
     */
    page_generation_int generation;
  };

  struct AttachState {
    /** \brief The slot upper bound of the most recent update to this attachment.
     */
    slot_offset_type last_update = 0;
  };

  struct CheckpointInfo {
    /** \brief The offset range of the newly appended checkpoint slots.
     */
    SlotRange checkpoint_slots;

    /** \brief The new trim offset (this is the slot upper bound of all checkpointed slots).
     */
    slot_offset_type trim_target = 0;
  };

  //----- --- -- -  -  -   -

  static const boost::uuids::uuid& get_event_entity(const PackedPageAllocatorAttach& event) noexcept
  {
    return event.user_id;
  }

  static PageId get_event_entity(const PackedPageRefCount& event) noexcept
  {
    return event.page_id.unpack();
  }

  //----- --- -- -  -  -   -

  explicit PageAllocatorState(const PageIdFactory& page_ids, SlotLockManager* trim_control,
                              PageAllocatorMetrics& metrics) noexcept;

  //----- --- -- -  -  -   -

  // The total checkpoint grant size we need to maintain in steady state is:
  //
  //  - one PackedPageRefCount slot for each PRC in each txn in the log
  //  - one attach/detach slot for each attached user_id
  //    (assumption: attach and detach are same size)

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // Design Note:
  //  PageAllocatorState::update is called for each slot on recovery and when each slot is
  //  appended during normal operation (post-recovery).
  //

  //----- --- -- -  -  -   -

  Status update(const SlotParse& slot, const PackedPageAllocatorAttach& attach) noexcept;
  Status update(const SlotParse& slot, const PackedPageAllocatorDetach& detach) noexcept;
  Status update(const SlotParse& slot, const PackedPageRefCount& ref_count) noexcept;
  Status update(const SlotParse& slot, const PackedPageAllocatorTxn& txn) noexcept;

  Status update_ref_count(slot_offset_type slot_upper_bound,
                          const PackedPageRefCount& pprc) noexcept;

  /** \brief Returns true iff the ref count for `page_id` has not been updated/refreshed at a slot
   * greater than `slot_upper_bound`.
   */
  bool updated_since(slot_offset_type slot_upper_bound, PageId page_id) const noexcept;

  /** \brief Returns true if `user_id` is attached and its `last_update` slot (upper bound) is
   * greater than `slot_upper_bound` *OR* if `user_id` is _not_ attached.
   */
  bool updated_since(slot_offset_type slot_upper_bound,
                     const boost::uuids::uuid& user_id) const noexcept;

  PageState* get_page_state(PageId page_id) noexcept;

  const PageState* get_page_state(PageId page_id) const noexcept;

  i32 get_ref_count(PageId page_id) const noexcept;

  Status validate_page_id(PageId page_id) const noexcept;

  /** \brief Returns true if the given `user_id` is attached.
   */
  bool is_user_attached(const boost::uuids::uuid& user_id) const noexcept
  {
    return this->attach_state.count(user_id) != 0;
  }

  /** \brief Consumes as much of `grant` as possible in order to append multiple checkpoint slots
   * as a single atomic MultiAppend.
   */
  StatusOr<CheckpointInfo> append_checkpoint(
      LogDevice::Reader& log_reader, batt::Grant& grant,
      TypedSlotWriter<PackedPageAllocatorEvent>& slot_writer);

  /** \brief Validates the passed list of PageRefCount delta updates, transforming the passed array
   * into the new absolute values of their respective page ref counts. NOTE: despite the word
   * 'apply' in the function name, this function does not modify the current page states (that is
   * done by update()).
   */
  Status validate_and_apply_ref_count_deltas(
      const Slice<PageRefCount>& ref_count_updates) const noexcept;

  //----- --- -- -  -  -   -

  /** \brief Reference to metrics object owned by the PageAllocator.
   */
  PageAllocatorMetrics& metrics;

  const PageIdFactory page_ids;

  /** \brief When true, update is being passed recovered slot data; otherwise, update is receiving
   * data we just appended.  When in recovery mode, update should return error status codes for
   * invalid data; when not, it should panic.
   */
  bool in_recovery_mode;

  std::unordered_map<boost::uuids::uuid, UserRecoveryState, boost::hash<boost::uuids::uuid>>
      pending_recovery;

  std::unordered_map<boost::uuids::uuid, AttachState, boost::hash<boost::uuids::uuid>> attach_state;

  std::vector<PageState> page_state;

  SlotLockManager* trim_control;
};

std::ostream& operator<<(std::ostream& out, const PageAllocatorState::CheckpointInfo& t);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PageAllocator
{
 public:
  static constexpr u64 kCheckpointTargetSizeLog2 = 12;
  static constexpr u64 kCheckpointTargetSize = 4 * kKiB;

  static_assert((u64{1} << kCheckpointTargetSizeLog2) == kCheckpointTargetSize);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static u64 calculate_log_size(PageCount physical_page_count, MaxAttachments max_attachments);

  static StatusOr<std::unique_ptr<PageAllocator>> recover(
      const PageAllocatorRuntimeOptions& options, const PageIdFactory& page_ids,
      MaxAttachments max_attachments, LogDeviceFactory& log_device_factory);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ~PageAllocator() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Notifies this object that halt is coming.
   */
  void pre_halt() noexcept;

  /** \brief Requests shutdown of all background tasks for this PageAllocator.
   */
  void halt() noexcept;

  /** \brief Blocks until all background tasks have completed.
   */
  void join() noexcept;

  /** \brief Returns a const reference to the diagnostic metrics for this object.
   */
  const PageAllocatorMetrics& metrics() const noexcept
  {
    return this->metrics_;
  }

  /** \brief Returns the number of bytes currently in the LogDevice for this allocator.
   */
  u64 log_size() const noexcept
  {
    return this->log_device_->size();
  }

  /** \brief Returns the configured size of the LogDevice for this allocator.
   */
  u64 log_capacity() const noexcept
  {
    return this->log_device_->capacity();
  }

  /** \brief Returns the target checkpoint grant pool size for the current configuration.
   */
  usize get_target_checkpoint_grant_pool_size() const noexcept;

  //----- --- -- -  -  -   -

  /** \brief First stage of per-user recovery; retrieves the list of recovered transactions
   * belonging to this user.  The user must call `notify_user_recovered` when it has finished all
   * recovery activities.
   *
   * If the given user_id is not attached, then returns error status.
   */
  StatusOr<std::map<slot_offset_type, SlotReadLock>> get_pending_recovery(
      const boost::uuids::uuid& user_id);

  /** \brief Called by attached users to indicate they have successfully recovered.
   */
  Status notify_user_recovered(const boost::uuids::uuid& user_id);

  /** \brief Blocks the caller until all users have completed recovery of any pending txns.
   *
   * Note: all attached users *must* at least ask for pending txns and notify the allocator that
   * they are done in order for this process to complete.
   */
  Status require_users_recovered(batt::WaitForResource wait_for_resource);

  //----- --- -- -  -  -   -

  /** \brief Adds `user_id` to the list of attached users for this allocator.  This list is used
   * during recovery to make sure that the allocator does not resume normal operation before each
   * user has had a chance to resolve any pending updates.
   */
  StatusOr<slot_offset_type> attach_user(const boost::uuids::uuid& user_id);

  /** \brief Removes `user_id` from the attached user list.
   */
  StatusOr<slot_offset_type> detach_user(const boost::uuids::uuid& user_id);

  //----- --- -- -  -  -   -

  /** \brief Removes a page from the free pool, leaving its refcount as zero.
   *
   * The returned PageId will have the lowest-valued generation number that hasn't yet been used
   * for that physical page.
   */
  StatusOr<PageId> allocate_page(batt::WaitForResource wait_for_resource,
                                 const batt::CancelToken& cancel_token = batt::CancelToken::none());

  /** \brief Return the given page to the free pool without updating its refcount.
   */
  void deallocate_page(PageId id);

  //----- --- -- -  -  -   -

  /** \brief Atomically and durably update a set of page reference counts.
   *
   * WARNING: if a given `page_id` appears more than once in the passed `ref_count_updates`
   * sequence, then only the **last** such item will have any effect on the PageAllocator state.
   * Callers of this function must combine the deltas for each page_id prior to passing the
   * sequence, if that is the desired behavior.
   */
  template <typename PageRefCountSeq,  //
            typename GarbageCollectFn = batt::DoNothing>
  StatusOr<SlotReadLock> update_page_ref_counts(
      const boost::uuids::uuid& user_id,    //
      slot_offset_type user_slot,           //
      PageRefCountSeq&& ref_count_updates,  //
      GarbageCollectFn&& garbage_collect_fn = GarbageCollectFn{});

  /** \brief Block until flushed (durable) updates have caught up with the specified slot number.
   */
  Status sync(slot_offset_type min_slot);

  //----- --- -- -  -  -   -

  /** \brief Returns the current ref count information for the given page.
   */
  StatusOr<i32> get_ref_count(PageId page_id);

  /** \brief Returns the current PageId (which includes the current generation number and device id)
   * and ref count for the given physical page.
   */
  StatusOr<PageRefCount> get_ref_count(PhysicalPageId physical_page);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageAllocator(std::string&& name, const PageIdFactory& page_ids, u64 max_attachments,
                std::unique_ptr<LogDevice>&& log_device,
                std::unique_ptr<SlotLockManager>&& trim_control) noexcept;

  //----- --- -- -  -  -   -

  /** \brief Implements PageAllocator state recovery.  Must be called by PageAllocator::recover
   * prior to returning the PageAllocator pointer.
   *
   * \return The recovered slot range.
   */
  StatusOr<SlotRange> recover_impl();

  /** \brief Populates the free page pool; this must only be done once all users have notified the
   * allocator that their recovery is complete, since their recovery process might involve updating
   * ref counts (0 -> 2) for new pages that they have written.
   */
  void init_free_page_pool(batt::ScopedLock<PageAllocatorState>& locked_state) noexcept;

  /** \brief Starts the compaction task; must be called after recovery.
   */
  void start_checkpoint_task(batt::TaskScheduler& scheduler);

  /** \brief Runs in the background, waiting for enough updates to accumulate, then compacting old
   * data to trim the log.
   *
   * \param base_state The full state of the allocator at the most recent Checkpoint event.
   */
  void checkpoint_task_main() noexcept;

  /** \brief Common implementation for attach_user and detach_user.
   */
  template <typename AttachEventT>
  StatusOr<slot_offset_type> process_attach_event(const AttachEventT& event);

  /** \brief Starts the free page task; see free_page_task_main() for details.
   */
  void start_free_page_task(batt::TaskScheduler& scheduler,
                            slot_offset_type recovered_slot_upper_bound);

  /** \brief Runs in the background, scanning durable log segments looking for newly freed pages to
   * replentish the free page pool.
   */
  void free_page_task_main() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Diagnostic metrics.
  //
  PageAllocatorMetrics metrics_;

  // Name for this object.
  //
  const std::string name_;

  // Contains the id and size of the associated PageDevice.
  //
  const PageIdFactory page_ids_;

  // The maximum number of configured client attachments.
  //
  const u64 max_attachments_;

  // Set by this->pre_halt(); controls verbosity of errors that might be logged during halt.
  //
  std::atomic<bool> halt_expected_{false};

  // On recover, set to the number of attached users.  Attached users should call
  // `notify_user_recovered` to indicate that they are now in a clean state; when the last of
  // these happens, the PageAllocator changes from safe mode to normal mode.
  //
  batt::Watch<i64> recovering_user_count_;

  // Pages available for allocation.
  //
  boost::lockfree::stack<PageId> free_pool_;

  // The number of PageIds pushed to the free_pool_, post-recovery; used to implement blocking
  // allocage_page.
  //
  batt::Watch<u64> free_pool_push_count_;

  // The LogDevice used to serialize the state of this allocator.
  //
  std::unique_ptr<LogDevice> log_device_;

  // Used to prevent premature trimming of updates.
  //
  std::unique_ptr<SlotLockManager> trim_control_;

  // Used to write slots.
  //
  TypedSlotWriter<PackedPageAllocatorEvent> slot_writer_;

  // The mutable state of the allocator.
  //
  batt::Mutex<PageAllocatorState> state_;

  // Grant used to append checkpoints.
  //
  batt::Grant checkpoint_grant_pool_;

  // Grant used to append updates (attach/detach/txn).
  //
  batt::Grant update_grant_;

  // Protects "live" slots against being trimmed before they are checkpointed.
  //
  SlotReadLock checkpoint_trim_lock_;

  // Background checkpoint task.
  //
  Optional<batt::Task> checkpoint_task_;

  // Bitset that tracks which pages are durably known to be in use (1) or free (0).
  //
  boost::dynamic_bitset<> durable_page_in_use_;

  std::unique_ptr<LogDevice::Reader> free_page_log_reader_;

  Optional<TypedSlotReader<PackedPageAllocatorEvent>> free_page_slot_reader_;

  // Background free page task.
  //
  Optional<batt::Task> free_page_task_;
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename PageRefCountSeq,  //
          typename GarbageCollectFn>
inline StatusOr<SlotReadLock> PageAllocator::update_page_ref_counts(
    const boost::uuids::uuid& user_id,    //
    slot_offset_type user_slot,           //
    PageRefCountSeq&& ref_count_updates,  //
    GarbageCollectFn&& garbage_collect_fn)
{
  //----- --- -- -  -  -   -
  // Summary:
  //  1. Copy ref count updates to a local vector (for relative-to-absolute conversion)
  //  2. Wait until we can reserve enough grant to append the Txn slot
  //  3. Lock the state mutex and:
  //     1. Verify that `user_id` is attached
  //     2. Calculate relative-to-absolute ref counts
  //     3. Acquire a SlotReadLock for the txn
  //     4. Append the txn slot
  //     5. Update ref counts in state
  //  4. Call passed garbage_collect_fn for all dead pages (ref_count == 1 due to this txn)
  //----- --- -- -  -  -   -

  SlotReadLock txn_slot_lock;
  batt::SmallVec<PageRefCount, 256> new_ref_counts;

  // 1. First copy the ref count _deltas_ to the local tmp vector; we will change them from relative
  //    deltas to new absolute counts later.
  //
  BATT_FORWARD(ref_count_updates) | seq::emplace_back(&new_ref_counts);

  // 2. Reserve grant from the `update_grant_` pool; this is replentished from our background
  //   (checkpoint) task, once it is confident it has enough grant for checkpoints (which take
  //   priority over updates).
  //
  const usize payload_size = packed_sizeof_page_allocator_txn(new_ref_counts.size());
  const usize slot_size = packed_sizeof_slot_with_payload_size(payload_size);

  BATT_ASSIGN_OK_RESULT(batt::Grant slot_grant, ([this, slot_size] {
                          BATT_DEBUG_INFO("[PageAllocator] waiting for update grant;"
                                          << BATT_INSPECT(this->update_grant_.size())
                                          << BATT_INSPECT(this->checkpoint_grant_pool_.size())
                                          << BATT_INSPECT(this->slot_writer_.pool_size())
                                          << BATT_INSPECT(this->slot_writer_.in_use_size()));

                          return this->update_grant_.spend(slot_size, batt::WaitForResource::kTrue);
                        }()));

  // This should be a no-op, but make sure we return the slot grant to the update grant pool and not
  // the default log pool maintained by the slot writer.
  //
  auto on_scope_exit = batt::finally([&] {
    this->update_grant_.subsume(std::move(slot_grant));
  });

  // 3. Lock the State and process the txn.
  //
  {
    batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

    // 3.1. The specified user must be attached.
    //
    if (!locked_state->is_user_attached(user_id)) {
      return {::llfs::make_status(::llfs::StatusCode::kPageAllocatorNotAttached)};
    }

    // 3.2. Convert local vector `new_ref_counts` from deltas to new ref_count absolute values.
    //
    BATT_REQUIRE_OK(locked_state->validate_and_apply_ref_count_deltas(as_slice(new_ref_counts)));

    // Create the op explicitly so we know the slot range before appending.
    //
    TypedSlotWriter<PackedPageAllocatorEvent>::MultiAppend append_op{this->slot_writer_};

    SlotRange slot_range;

    slot_range.lower_bound = append_op.slot_offset();
    slot_range.upper_bound = slot_range.lower_bound + slot_size;

    // 3.3. Lock the range of this slot before appending it, to be absolutely certain it can't be
    // trimmed!
    //
    BATT_ASSIGN_OK_RESULT(txn_slot_lock, this->trim_control_->lock_slots(
                                             slot_range, "PageAllocator::update_page_ref_counts"));

    // 3.4. Pack the txn into a new slot and append to the log.
    //
    StatusOr<SlotParseWithPayload<const PackedPageAllocatorTxn*>> packed_slot =
        this->slot_writer_.typed_append(append_op, slot_grant,
                                        PageAllocatorTxn{
                                            .user_id = user_id,
                                            .user_slot = user_slot,
                                            .ref_counts = as_slice(new_ref_counts),
                                        });

    BATT_REQUIRE_OK(packed_slot);

    // Make sure that the slot range calculation we did above was accurate.
    //
    BATT_CHECK_EQ(slot_range, packed_slot->slot.offset);

    // 3.5. Update the state before releasing mutex.
    //
    BATT_CHECK_OK(locked_state->update(packed_slot->slot, *packed_slot->payload))
        << "This should never fail!";
  }
  //
  // (unlock State)

  // 4. Finally, call the garbage collection function for all newly dead pages.
  //
  for (const PageRefCount& prc : new_ref_counts) {
    if (prc.ref_count == 1) {
      garbage_collect_fn(prc.page_id);
    }
  }

  // Done; return the slot lock.
  //
  return txn_slot_lock;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename AttachEventT>
inline StatusOr<slot_offset_type> PageAllocator::process_attach_event(const AttachEventT& event)
{
  StatusOr<slot_offset_type> slot_upper_bound;

  const usize payload_size = packed_sizeof(event);
  const usize slot_size = packed_sizeof_slot_with_payload_size(payload_size);

  BATT_ASSIGN_OK_RESULT(batt::Grant slot_grant, ([this, slot_size] {
                          BATT_DEBUG_INFO("[PageAllocator] waiting for update grant;"
                                          << BATT_INSPECT(this->update_grant_.size())
                                          << BATT_INSPECT(this->checkpoint_grant_pool_.size())
                                          << BATT_INSPECT(this->slot_writer_.pool_size())
                                          << BATT_INSPECT(this->slot_writer_.in_use_size()));

                          return this->update_grant_.spend(slot_size, batt::WaitForResource::kTrue);
                        }()));

  // Return any unused part of the slot grant to the update grant pool; _not_ the default log pool
  // maintained by the slot writer.
  //
  auto on_scope_exit = batt::finally([&] {
    this->update_grant_.subsume(std::move(slot_grant));
  });

  const bool desired_state = std::is_same_v<std::decay_t<AttachEventT>, PackedPageAllocatorAttach>;

  {
    batt::ScopedLock<PageAllocatorState> locked_state{this->state_};

    if (locked_state->is_user_attached(event.user_id) == desired_state) {
      return OkStatus();
    }

    // Check to make sure we have a free attachment available.
    //
    if (desired_state && (locked_state->attach_state.size() + 1 > this->max_attachments_)) {
      return ::llfs::make_status(llfs::StatusCode::kOutOfAttachments);
    }

    StatusOr<SlotParseWithPayload<const AttachEventT*>> packed_slot =
        this->slot_writer_.typed_append(slot_grant, event);

    BATT_REQUIRE_OK(packed_slot);
    BATT_REQUIRE_OK(locked_state->update(packed_slot->slot, *packed_slot->payload));

    slot_upper_bound = packed_slot->slot.offset.upper_bound;
  }

  BATT_CHECK_OK(slot_upper_bound);

  return slot_upper_bound;
}

}  //namespace experimental
}  //namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR2_HPP
