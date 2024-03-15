//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR2_IPP
#define LLFS_PAGE_ALLOCATOR2_IPP

namespace llfs {
namespace experimental {

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
}

}  //namespace experimental
}  //namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR2_IPP
