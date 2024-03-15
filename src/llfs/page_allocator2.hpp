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
#include <llfs/optional.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/packed_page_ref_count.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/packed_slot_range.hpp>
#include <llfs/packed_variant.hpp>
#include <llfs/page_allocator_runtime_options.hpp>
#include <llfs/simple_packed_type.hpp>
#include <llfs/slot_lock_manager.hpp>
#include <llfs/slot_read_lock.hpp>
#include <llfs/slot_writer.hpp>
#include <llfs/uuid.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/async/grant.hpp>
#include <batteries/async/task.hpp>
#include <batteries/async/watch.hpp>

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
//
struct PackedPageAllocatorTxn {
  boost::uuids::uuid user_id;                  // 16
  PackedSlotOffset user_slot;                  // 8
  PackedArray<PackedPageRefCount> ref_counts;  // 8
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageAllocatorTxn), 32);

std::ostream& operator<<(std::ostream& out, const PackedPageAllocatorTxn& t);

usize packed_sizeof_page_allocator_txn(usize n_ref_counts);

usize packed_sizeof(const PackedPageAllocatorTxn& txn);

usize get_page_allocator_txn_grant_size(usize n_ref_counts);

usize packed_sizeof_page_ref_count_slot() noexcept;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class PageAllocatorState
{
 public:
  using Self = PageAllocatorState;

  //----- --- -- -  -  -   -

  struct PageState {
    /** \brief The slot upper bound of the most recent update to this page.
     */
    slot_offset_type last_update = 0;

    /** \brief The current reference count for the page.
     */
    i32 ref_count = 0;
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
    slot_offset_type trim_target;

    /** \brief The number of page ref count updates in all trimmed txn slots for this checkpoint.
     */
    u64 trimmed_txn_ref_count_updates;
  };

  //----- --- -- -  -  -   -

  explicit PageAllocatorState(const PageIdFactory& page_ids) noexcept;

  //----- --- -- -  -  -   -

  // The total checkpoint grant size we need to maintain in steady state is:
  //
  //  - one PackedPageRefCount slot for each PRC in each txn in the log
  //  - one attach/detach slot for each attached user_id
  //    (assumption: attach and detach are same size)

  //----- --- -- -  -  -   -

  Status update(const SlotParse& slot, const PackedPageAllocatorAttach& attach) noexcept;
  Status update(const SlotParse& slot, const PackedPageAllocatorDetach& detach) noexcept;
  Status update(const SlotParse& slot, const PackedPageRefCount& ref_count) noexcept;
  Status update(const SlotParse& slot, const PackedPageAllocatorTxn& txn) noexcept;

  Status update_ref_count(slot_offset_type slot_upper_bound,
                          const PackedPageRefCount& pprc) noexcept;

  bool ref_count_updated_since(slot_offset_type slot_upper_bound, PageId page_id) const noexcept;

  /** \brief Returns true if `user_id` is attached and its `last_update` slot (upper bound) is
   * greater than `slot_upper_bound` *OR* if `user_id` is _not_ attached.
   */
  bool attachment_updated_since(slot_offset_type slot_upper_bound,
                                const boost::uuids::uuid& user_id) const noexcept;

  i32 get_ref_count(PageId page_id) const noexcept;

  /** \brief Consumes as much of `grant` as possible in order to append multiple checkpoint slots as
   * a single atomic MultiAppend.
   */
  StatusOr<CheckpointInfo> append_checkpoint(
      LogDevice& log_device, batt::Grant& grant,
      TypedSlotWriter<PackedPageAllocatorEvent>& slot_writer);

  /** \brief Returns the target checkpoint grant size for the current log contents.
   */
  usize get_target_checkpoint_grant_size() const noexcept;

  //----- --- -- -  -  -   -

  const PageIdFactory page_ids;

  /** \brief When true, update is being passed recovered slot data; otherwise, update is receiving
   * data we just appended.  When in recovery mode, update should return error status codes for
   * invalid data; when not, it should panic.
   */
  bool in_recovery_mode;

  std::unordered_map<boost::uuids::uuid, std::map<slot_offset_type, SlotReadLock>,
                     boost::hash<boost::uuids::uuid>>
      pending_recovery;

  std::unordered_map<boost::uuids::uuid, AttachState, boost::hash<boost::uuids::uuid>> attach_state;

  std::vector<PageState> page_state;

  usize active_txn_ref_counts = 0;
};

#if 0

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PageAllocator
{
 public:
  static constexpr u64 kCheckpointGrantThreshold = 1 * kMiB;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static u64 calculate_log_size(u64 physical_page_count, u64 max_attachments);

  static StatusOr<std::unique_ptr<PageAllocator>> recover(
      const PageAllocatorRuntimeOptions& options, const PageIdFactory& page_ids,
      u64 max_attachments, LogDeviceFactory& log_device_factory);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  ~PageAllocator() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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

  //----- --- -- -  -  -   -

  /** \brief Requests shutdown of all background tasks for this PageAllocator.
   */
  void halt() noexcept;

  /** \brief Blocks until all background tasks have completed.
   */
  void join() noexcept;

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
   * \return The compacted slot upper bound
   */
  Status recover_impl();

  /** \brief Starts the compaction task; must be called after recovery.
   */
  void start_checkpoint_task(batt::TaskScheduler& scheduler);

  /** \brief Runs in the background, waiting for enough updates to accumulate, then compacting old
   * data to trim the log.
   *
   * \param base_state The full state of the allocator at the most recent Checkpoint event.
   */
  void checkpoint_task_main() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Name for this object.
  //
  const std::string name_;

  // Contains the id and size of the associated PageDevice.
  //
  const PageIdFactory page_ids_;

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
  batt::Grant checkpoint_grant_;

  // Background checkpoint task.
  //
  Optional<batt::Task> checkpoint_task_;
};

#endif

}  //namespace experimental
}  //namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR2_HPP
