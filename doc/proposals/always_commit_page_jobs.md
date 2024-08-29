# Proposal: "Always-Commit" Page Jobs

## Problem Statement

The current (2024-08-16) multi-phase job commit protocol used by
`llfs::Volume` is more complicated and less efficient than necessary.
Specifically:

1. Because the `PrepareJob` slot and `PageDevice` data writes happen
   concurrently, with the first durable PRC (`PageRefCount`) update in
   any of the `PageAllocator` object associated with the job acting as
   the event that durably commits the job, in (we believe) rare
   situations, a durable `PrepareJob` slot will need to be rolled
   back.  If _no_ PRCs can be found during recovery for a given
   `PrepareJob` which is missing a `CommitJob`, we have no way to know
   whether all the page data was written.  In this case, we
   conservatively assume that not all page data was made durable
   before the last shutdown, so we cancel the job by writing a
   `RollbackJob` slot.  Unfortunately, this takes up space in the
   `Volume`'s root log, which means we run the risk of deadlock if the
   page job's purpose is to move data out of the log so it can be
   trimmed.  Although it seems very unlikely, it is nonetheless
   possible for repeated failures to commit a job to fill the log,
   leading to deadlock.
2. Currently, committing a job requires six round-trips to storage:
   1. Write page data and prepare slot
   2. Write control block for volume root log (flush prepare slot)
   3. Write all PRCs to `PageAllocator` logs
   4. Write control block for `PageAllocator` logs
   5. Write commit slot to volume root log
   6. Write volume root log control block 
   
   Because after step 4, we know that the job is guaranteed to commit,
   we can do steps 5 and 6 asynchronously, effectively utilizing
   parallel I/O via pipelineing.
3. User data and root page ref list data are duplicated between
   `PrepareJob` and `CommitJob` slots to avoid complications in Volume
   trimming, increasing write amplification.

We believe it is possible to eliminate the need to ever roll back a
job while simulataneously cutting the job commit latency by a factor
of 2 by modifying the page job protocol and the design of
`PageAllocator`.

## Design Overview

In the new design, committing a page job will be comprised of the
following steps:

1. Write all page data in parallel, and wait for I/O completion
2. Write a single `PageJob` slot containing a list of the new pages
   and new root references for the job
3. (return control to the application; the following steps happen
   asynchronously)
4. Wait for `PageJob` slot to be flushed
5. Calculate all PRC updates and write them in parallel to their
   respective `PageAllocator` logs
6. Once all PRCs are flushed, write a single `PageRefCountsCommitted`
   slot to the volume root log

The definitions of the new `Volume` event slot records are as follows:

```c++
struct PackedPageJob {
  PackedPointer<PackedArray<PackedPageId>> root_page_ids;
  PackedPointer<PackedArray<PackedPageId>> new_page_ids;
  PackedPointer<PackedArray<PackedPageId>> deleted_page_ids;
  PackableRef user_data;
};

struct PackedPageRefCountsCommitted {
  PackedSlotOffset page_job_slot;
};
```

## Requirements

A correct design must meet the following requirements:

- (R1) Crash-Consistency: all requirements must hold even when the
  application crashes at arbitrary points.  See Assumptions section
  for more details about our crash model.
- (R2) No-Leaks: No page may be unreferenced directly or indirectly
  via a `Volume` or `PageRecycler` log unless they are marked as
  "free" and available for future allocation.
- (R3) Consistent Data/PageId Mapping (or Unique-Allocation): All
  logs and pages that reference a given `PageId` _must_ have the same
  notion of the data contained within that page.  This is equivalent
  to saying that under no circumstances can there be a sequence of
  events leading to a `PageAllocator` handing out the same `PageId`
  to different clients; allocations must be unique.
- (R4) Always-Commit: a partially committed page job must always be
  lost during a crash or recovered and successfully completed; there
  must never be the need to roll-back a job.
- (R5) Atomicity: a page job must succeed or fail in its entirety,
  with no intermediate states (such as partial page writes,
  allocations, or reference count updates) being visible to the
  user/application.
- (R6) Fixed-Size Logs: all log-structured (slot) data must fit
  within one or more fixed maximum size `LogDevice`s.
- (R7) No-Deadlock: No `LogDevice` may ever enter a state where the
  log is full and it is impossible to trim the log to free up space
  for future appends without losing data or otherwise violating one of
  the other requirements.

## Detailed Design

Clients/Users of PageAllocator:

- PageCacheJob
- Volume
- VolumeTrimmer
- PageRecycler

This document is structured around the lifecycle phases of a page in LLFS:

- Free (ref count = 0, in free pool) 
- Allocated (ref count = 0, not in free pool)
- Live (ref count >= 2, reachable from >= 1 Volume logs)
- Dead (ref count == 1, owned by PageRecycler)

Each state transition (Free-to-Allocated, Allocated-to-Free, Allocated-to-Live, Live-to-Dead, Dead-to-Free) has unique issues to address in order to meet the requirements as stated above.  These issues will be discussed in the context of the pertinent design elements for each transition in the sections below.

Each state transition is performed by a specific client/job type, as follows:

- Free-to-Allocated, Allocated-to-Live: **New Page Job**
- Live-to-Dead: **Trim Page Job**
- Dead-to-Free: **Recycle Page Job**

Each job type has a specific protocol, which will be described in the state transition section for that job type.

### Page Job Types

#### New Page Job

- Introduces zero or more newly written pages
- `llfs::PageDevice::write` is called to write page data
- Introduces 1 or more page references (root references)
- All Page Ref Count deltas are positive
- All new pages have a delta of at least +2
- Appended to the root log of a Volume

#### Trim Page Job

- Removes one or more root (log) references
- All Page Ref Count deltas are negative
- May cause reference count for a page to go to 1, but never to 0
- Executed by the `llfs::VolumeTrimmer`  associated with an `llfs::Volume`, prior to trimming the root log

#### Recycle Page Job

- Removes the last reference to one or more pages (these are the recycled pages)
- Decrements the reference counts of all pages referenced by the recycled pages
- Executed by the `llfs::PageRecycler`
- `llfs::PageDevice::drop` is called to remove the data associated with the recycled pages (this may result in, for example, an NVMe trim operation, or the removal of a file representing the page in a host filesystem)

### Initial PageAllocator State

When a `PageAllocator` is created, several parameters are configured; these remain constant during the lifetime of the `PageAllocator`:

- ID of the associated `PageDevice`, both `UUID` and device id (`u16`), which is scoped to a particular `StorageContext` and `PageCache`, though a given `PageDevice` must have the same device id in all contexts in which it appears
- Page Count (reflected from the `PageDevice` configuration)
- Max Attachments: the maximum number of client `UUID`s that can be attached to (i.e., registered with) the allocator
- UUID of the `LogDevice` that will store the `PageAllocator` state.  This must be at least some minimum size, determined by the Page Count and Max Attachments parameters.

The durable state of a `PageAllocator` is comprised of the following:

1. For each physical page:
   1. The current generation number (`u64`)
   2. The current reference count (`i32`)
   3. (If ref count &gt; 0) Whether the page has outgoing references to other pages (`bool`)
2. The set of attached client `UUID`s; this set's size must not exceed Max Attachments
3. The set of unresolved client transactions; each transaction is comprised of:
   1. The `UUID` of an attached client (user)
   2. A client-defined `user_slot` (`u64`), which uniquely identifies that transaction in the context of the user
   3. A list of updates to page reference counts

Initially, the `PageAllocator` log starts out empty, and the state is assumed to be:

- All pages' generation and ref count are zero
- The set of attached clients is empty
- The set of unresolved transactions is empty

In the initial state, all pages are considered Free.

### Free-to-Allocated

A given `PageId` leaves the Free state and enters state Allocated via a call to `PageAllocator::allocate_page`:

```c++
batt::CancelToken cancel_token;

batt::StatusOr<llfs::PageId> new_page_id = 
  page_allocator->allocate_page(batt::WaitForResource::kTrue, cancel_token);

BATT_REQUIRE_OK(new_page_id);
```

On success,`allocate_page` returns the `PageId` of a page whose reference count is zero and which has never been written in the current generation (encoded in the middle bits of the `PageId` itself).  Once a given `PageId` has been returned by `allocate_page`, that `PageId` will **not** be returned again (unless the allocation is "cancelled" via `PageAllocator::deallocate_page`; see Allocated-to-Free below).  Thus `allocate_page` grants unique ownership of the page to the caller.

The `batt::WaitForResource` arg specifies what `allocate_page` should do if there are no free pages available; if true, then the caller is blocked until the request is explicitly cancelled or a page becomes available (see Dead-to-Free).  If false, `allocate_page` immediately returns `batt::StatusCode::kResourceExhausted` if there are no free pages available.

The `batt::CancelToken` arg is optional; if present, it allows the caller to cancel a blocked call to `allocate_page` from another task before it completes.  It doesn't make sense to specify a cancel token _and_ `batt::WaitForResource::kFalse`, since there is never a blocking call to interrupt in this case.

The fact that a page is in the Allocated state is something which must be durably recorded _outside_ the `PageAllocator` log.  The standard way to do this is by writing a `PackedPageJob` slot to a `Volume` log, as described in Allocated-to-Live.  When the state of a `PageAllocator` is recovered from its log, all slots are scanned.  From this scan, we get a list of attached users (clients) and pending (unresolved) transactions.  It is crucially important that each page's state be accurately recovered before the PageAllocator enters "normal operation."  For example, if the actual state of a page is Allocated, but we recover it as Free, then the page may be re-allocated to some different client, potentially violating Requirement (R3) "Consistent Data/PageId Mapping (or Unique-Allocation."  Thus, after `PageAllocator::recover` is called, each attached or potentially attached client must call `PageAllocator::get_pending_recovery(user_id)` to begin _external recovery_, which is terminated when the client calls `PageAllocator::notify_user_recovered`.  When the PageAllocator is in external recovery, `allocate_page` operations are temporarily suspended so that unique page ownership can be maintained.  See the page job protocol sections of each of the page state transition descriptions below for details on how each client/job type performs recovery and why each protocol maintains crash consistency.

### Allocated-to-Free

If a client allocates a page and then decides it doesn't need the allocated page, it can revert the page state back to free by calling `PageAllocator::deallocate_page`.  Once a client does this, it no longer claims exclusive access to that page, and must not attempt to write or reference it, etc.

### Allocated-to-Live

A page's state changes from Allocated to Live via `PageAllocator::update_page_ref_counts`:

```c++
std::vector<llfs::PageRefCount> updates;
updates.emplace_back(llfs::PageRefCount{.page_id = *new_page_id, .ref_count = 2,});

batt::StatusOr<llfs::SlotReadLock> txn_lock =
  page_allocator->update_page_ref_counts(user_id, user_slot, batt::as_seq(updates));

BATT_REQUIRE_OK(txn_lock);
```

When calling `update_page_ref_counts`, the following requirements must be met:

1. `user_id` must be the `UUID` (`boost::uuids::uuid`) of an attached client
2. `user_slot` must be an integer uniquely associated (by the client) to the specific set of updates being passed
3. Any `PageId`s in the updates sequence must refer to pages for which one of the following is true:
   1. The page is in the Live state, and the caller owns at least one reference to the page
   2. The page is in the Allocated state, the caller is the current owner of the page, and the ref count delta is at least +2

The `.ref_count` (`i32`) values in the updates sequence specify relative count deltas; i.e. the change in reference count relative to the current ref count of a page.  At no point may a page's ref count go below 0.  When a page's ref count goes from 0 to some positive value, it must go to at least 2 (1 future reference for the garbage collection pipeline, >= 1 reference for the client).

On success, `update_page_ref_counts` returns an `llfs::SlotReadLock` object that prevents the `PageAllocator` from trimming the portion of its log that contains a record of the update (a slot record of type `llfs::PackedPageAllocatorTxn`).

In LLFS, all pages are accessed either directly from the log of an `llfs::Volume` by reading a `PageId` from some slot record, or indirectly from another page.  The application must guarantee that reference cycles are never introduced when writing new pages; i.e., the set of new pages in a given page job must not contain reference cycles.  

#### New Page Job Protocol

The protocol for writing a new page reference to a Volume is:

1. If the Volume is not currently attached, call `PageAllocator::attach_user` with the Volume's UUID
2. Call `PageAllocator::allocate_page` one or more times to allocate new pages
3. Call `PageDevice::prepare(PageId)` for each allocated page, to obtain a buffer into which to write that page's data
4. Initialize the buffer(s) with page data
5. Call `PageDevice::write` to write the initialized data buffer to the storage device.  Write the pages in parallel, and wait for all to complete.
6. Append a `PackedPageJob` slot to the Volume's root log, containing the list of all new pages, root references, and any (opaque) user data associated with the job
7. Call `LogDevice::sync` on the Volume's root log to flush the `PackedPageJob` slot
8. Call `PageAllocator::update_page_ref_counts` with the updated reference counts for the new pages and any pages they reference; retain the returned `SlotReadLock` until released below (_Note: this step may entail calling `update_page_ref_counts` for many `PageAllocator` instances concurrently_); `user_slot` should be the slot offset of the `PackedPageJob` slot appended in (6)
9. Wait for all `PageAllocator`s involved in (8) to flush the page ref count transaction slots by calling `PageAllocator::sync`
10. Append a `PackedPageRefCountsCommitted` slot to the Volume log, referencing the slot offset from (6); **wait for this slot to be flushed**
11. Release the `SlotReadLock` from (8)

#### Crash Consistency of New Page Jobs

This section contains a proof of crash consistency for the protocol described above.  

Most of the heavy lifting here is done by the crash-consistency of `LogDevice::commit`.  Specifically, all implementations of `LogDevice` are required to guarantee the following:

- When a `LogDevice` is recovered (after crash), the recovered slot offset upper bound of the log must be some offset resulting from a prior call to `LogDevice::commit`
- All data up to the recovered slot offset upper bound of the log must be valid; i.e., it must be the same as the contents of the in-memory log ring buffer when `LogDevice::commit` was called prior to the crash

Given these properties, it is trivial to implement atomic log slot appends which are crash consistent.  Thus each page state transition involved in a New Page Job (Free-to-Allocated and Allocated-to-Live) is made durable via an atomic log append:

- Free-to-Allocated is made durable via the `PackedPageJob` record appended to the Volume log
- Allocated-to-Live is made durable via the `PackedPageAllocatorTxn` record appended to the PageAllocator log

The correct client recovery steps depend on which durable state the new page(s) are in when the crash occurs:

- Free: recovery should take _no_ steps, the page(s) should be added to the free pool, and made available for allocation by other clients
- Allocated: recovery should recompute the Page Ref Count deltas for the job and apply them to the proper `PageAllocator`s to complete the job
- Live: recovery should mark the job as completed so that the Txn slots can be removed from the `PageAllocator` logs

The recovery protocol is as follows:

1. When recovering a `Volume` root log, record all `PackedPageJob` slots that have no corresponding `PackedPageRefCountsCommitted` slot.  This forms the set of _pending New Page Jobs_.
2. For each pending New Page Job, load the page data for all new pages listed in the slot.  Since we read the `PackedPageJob` slot, we know that the crash occurred after (6), so it must also have occurred after (5), which writes all page data.
3. Reconstruct a list of `PageDevice`/`PageAllocator`s involved in the job from the new pages by (re-)calculating the Page Ref Count deltas using trace refs for each page.
4. Call `PageAllocator::get_pending_recovery` for each device.
5. Call `PageAllocator::update_page_ref_counts` for each device that did not return a txn lock for the page job slot
6. Call `PageAllocator::sync` to make sure the new updates are durable
7. Write a `PackedPageRefCountsCommitted` slot to the Volume log and wait for it to sync
8. Release any txn `SlotReadLock`s obtained in recovery step 4 (`get_pending_recovery`)
9. Call `PageAllocator::notify_user_recovered` once all New Page Jobs have been recovered

The recovery protocol shows that if we get as far as flushing a `PackedPageJob` before the crash, then enough data has been saved to always commit the job.  If we did not get this far, then since there is no record of the job ever having occurred, there is no root reference to any new pages in any log(s), so it doesn't matter that the data was written; it will simply be overwritten when the page is next allocated.  _Note: an important consequence of this design is that the `PackedPageJob` slot that introduces a new page **must** be appended strictly before any other slot that introduces another reference to that page, whether directly (from a log slot) or indirectly (from another new page)._

### Live-to-Dead

During the Live phase of a page's lifecycle, the reference count may go up and down.  However, once the reference count decreases to 1, it is considered Dead.  A Dead page may not become live again; once its reference count becomes 1, ownership of that page is transferred to a `PageRecycler`, which takes care of driving the transition from Dead-to-Free.

There are two ways a page can become dead:

1. The last reference to the page is in a Volume root log that is trimmed, removing the reference
2. The last reference to the page is in a dead page, which becomes free by being recycled



### Dead-to-Free





## Assumptions

### Crash Model

1. Every `PageDevice` has a _block size_, which must be a power of 2, independent of (though guaranteed not to be larger than) the page size for that device.  The page size must be a multiple of the block size.
2. The _block size_ for a `PageDevice` is by definition the maximum write size considered atomic (writes will either succeed or fail, even on power loss; never partially succeed).
3. The _block size_ for a `PageDeviec` is also by definition the minimum size and alignment when writing data to the underlying storage hardware for that device.
4. All page writes to a `PageDevice` are considered as equivalent to some number of _block_-sized writes covering the same address extent on the hardware, performed in parallel.
5. All _block_ writes to a `PageDevice` are partially ordered.
6. A crash is considered to be the following:
   1. The program writing to storage hardware using LLFS stops executing
   2. A "cut-point" is defined within the partial order of all block writes: 
      1. All block writes ordered _before_ the cut-point are considered durable and will be successfully read (unless the device fails or is damaged, which is considered undefined behavior) when the program is restarted.  
      2. All block writes that are _not-before_ the cut-point are permanently lost.

***NOTE: the precise place of a crash cut-point within the partial order of block writes is assumed to be unknowable in general, except for being able to observe its consequences as stated above.***

# (DEPRECATED CONTENT)

The following events can happen in any order, unless constrained by
one of the invariants stated in the next section (Invariants).

- (E1) Page is marked as "free" for a given generation
- (E2) Page is allocated to a client
- (E3) Page is deallocated
- (E4) `Volume` requests attachment to `PageAllocator`
- (E5) `Volume` attachment becomes durable in `PageAllocator` log
- (E6) `PageCacheJob` is finalized/application submits job to
  `Volume`
- (E7) Writing of page data to `PageDevice` objects is initiated
- (E8) Writing of page data completes
- (E9) `PackedPageJob` slot is appended to `Volume` log
- (E10) `Volume` returns success for job append, returning slot
  range of the appended job
- (E11) `PackedPageJob` slot becomes durable
- (E12) `Volume` indicates to application that a job is durable
- (E13) PRC updates for a job (user uuid + slot offset) are appended
  to all `PageAllocator` logs
- (E14) PRC updates for a job become durable (for all
  `PageAllocator`s)
- (E15) `PackedPageRefCountsCommitted` slot is appended to `Volume`
   log
- (E16) `PackedPageRefCountsCommitted` becomes durable
- (E17) PRC update slot is trimmed from `PageAllocator` log
- (E18) `PackedPageJob` slot becomes trimmable from `Volume` log
- (E19) `VolumeTrimmer` requests attachment to `PageAllocator`
- (E20) `VolumeTrimmer` attachment becomes durable in
  `PageAllocator` log
- (E21) `VolumeTrimmer` appends `PackedVolumeTrimEvent` to `Volume`
  log
- (E22) `PackedVolumeTrimEvent` becomes durable in `Volume` log
- (E23) PRC update for `PackedVolumeTrimEvent`, decrementing ref
  counts of any root refs in the to-be-trimmed region of the `Volume`
  log, appended to `PageAllocator` log
- (E24) PRC update lowers Page ref count to 1 (i.e. page becomes
  "dead")
- (E25) `PackedVolumeTrimEvent` PRC update slot becomes durable in
  `PageAllocator` log
- (E26) `PackedPageJob` slot is durably trimmed from `Volume` log
- (E27) PRC update slot for `PackedPageJob` is trimmed from
  `PageAllocator` log
- (E28) PRC update slot for `PackedVolumeTrimEvent` is trimmed from
  `PageAllocator` log
- (E29) `Volume` detaches from `PageAllocator`
- (E30) `VolumeTrimmer` detaches from `PageAllocator`
- (E31) Dead Page is durably written to `PageRecycler` log
- (E32) Dead Page is assigned a batch slot, which is recorded in
  `PageRecycler` log
- (E33) `PageRecycler` attaches to `PageAllocator`s for all devices
  containing Dead Page or a page directly referenced by it
- (E34) PRC updates for Dead Page (`RecycleBatch`) are appended to all
  `PageAllocator` logs (user: `PageRecycler`)
- (E35) Dead Page is dropped from `PageDevice` (e.g., NVMe trim
  operation)
- (E36) PRC updates for `RecycleBatch` become durable in
  `PageAllocator` logs
- (E37) `PackedRecycleBatchCommit` slot durably written to
  `PageRecycler` log
- (E38) `RecycleBatch` PRC update slot trimmed in `PageAllocator` logs

### Invariants

1. (I1) No PRC update slot may be trimmed from a `PageAllocator` log
   before the page job to which the update belongs has been _durably_
   marked as completed by the `Volume` (user)
2. (I2) No `PackedPageJob` slot may be written until _all_ page data
   has successfully written to the device
3. (I3) A `PageRefCountsCommitted` slot must always refer to a
   previous (lower offset) slot (the slot lower bound of the
   `PackedPageJob`) in the same `Volume` root log
4. (I4) 

### 
