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

Each state transition (Free -> Allocated, Allocated -> Free, Allocated -> Live, Live -> Dead, Dead -> Free) has unique issues to address in order to meet the requirements as stated above.  These issues will be discussed in the context of the pertinent design elements for each transition in the sections below.

### Initial Page Allocator State



### Free -> Allocated

### Allocated -> Live

### Live -> Dead

### Dead -> Free





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
