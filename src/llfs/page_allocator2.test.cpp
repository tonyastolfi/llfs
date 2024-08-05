//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_allocator2.hpp>
//
#include <llfs/page_allocator2.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using namespace batt::int_types;

using llfs::experimental::PageAllocator;

// Test Plan:
//  1. calculate log size
//  2. recover from empty log
//     a. verify all ref counts are 0
//     b. allocate_page should succeed without blocking until no more pages
//     c. deallocate_page should unblock an allocator; allow page to be reallocated (same
//        generation).
//  3. simulate workload with hot pages and cold pages; run for long enough so that log rolls over
//     several times
//     - verify that the hot page ref count updates don't cause the cold page ref counts to be lost
//  4. try to update pages without attaching - fail
//  5.
//
//

TEST(PageAllocator2Test, CalculateLogSize)
{
  for (auto page_count : {
           llfs::PageCount{100},
           llfs::PageCount{64},
           llfs::PageCount{1},
       }) {
    for (auto max_attachments : {
             llfs::MaxAttachments{5},
             llfs::MaxAttachments{8},
             llfs::MaxAttachments{1024},
         }) {
      usize expected_size = 0;

      expected_size += PageAllocator::kCheckpointTargetSize;
      expected_size += llfs::experimental::packed_sizeof_page_ref_count_slot() * page_count;
      expected_size +=
          llfs::experimental::packed_sizeof_page_allocator_attach_slot() * max_attachments;

      expected_size += 4095;
      expected_size /= 4096;
      expected_size *= 4096;
      expected_size *= 2;

      usize actual_size = PageAllocator::calculate_log_size(page_count, max_attachments);

      EXPECT_EQ(actual_size, expected_size);
    }
  }
}

}  // namespace
