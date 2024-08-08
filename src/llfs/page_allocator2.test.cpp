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

#include <llfs/uuid.hpp>

#include <llfs/testing/test_config.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <random>
#include <vector>

namespace {

using namespace batt::int_types;

using llfs::StatusOr;
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
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
      expected_size += llfs::packed_sizeof_page_ref_count_slot() * page_count;
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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Test Plan:
//   - For all of the following, verify:
//     a. Pack is successful when dst buffer is exactly the packed_sizeof...
//     b. ...fails when one less
//     c. Pack/unpack_cast recovers all information
//   1. No ref_counts
//   2. Single ref_count
//   3. Hundreds of ref_counts
//
TEST(PageAllocator2Test, PackPageAllocatorTxn)
{
  constexpr usize kNumSeeds = 100;

  using llfs::experimental::PackedPageAllocatorTxn;
  using llfs::experimental::PageAllocatorTxn;

  llfs::testing::TestConfig test_config;
  std::uniform_int_distribution<i32> pick_ref_delta{-10, 10};

  const auto first_seed = test_config.get_random_seed();

  for (usize seed_i = 0; seed_i < kNumSeeds; ++seed_i) {
    const auto seed = first_seed + seed_i;
    std::default_random_engine rng{seed};

    for (usize num_ref_counts : {0, 1, 10, 100, 1000}) {
      std::vector<llfs::PageRefCount> ref_counts;

      for (usize i = 0; i < num_ref_counts; ++i) {
        ref_counts.emplace_back(llfs::PageRefCount{
            .page_id = llfs::make_random_page_id(rng),
            .ref_count = pick_ref_delta(rng),
        });
      }

      const PageAllocatorTxn txn{
          .user_id = llfs::random_uuid(),
          .user_slot = 367812 + num_ref_counts,
          .ref_counts = batt::as_slice(ref_counts),
      };

      const usize packed_size = packed_sizeof(txn);

      std::vector<u8> memory(packed_size);
      std::memset(memory.data(), ('x' + seed) & 0xff, memory.size());

      // b. Fail to pack with 1 byte too little
      {
        llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size() - 1}};

        PackedPageAllocatorTxn* packed_txn = llfs::pack_object(txn, &packer);

        EXPECT_EQ(packed_txn, nullptr);
      }

      const auto verify_packed_txn = [&memory, &txn](const PackedPageAllocatorTxn* packed_txn) {
        ASSERT_NE(packed_txn, nullptr);
        EXPECT_EQ((void*)packed_txn, (void*)memory.data());
        EXPECT_EQ(packed_txn->user_id, txn.user_id);
        EXPECT_EQ(packed_txn->user_slot, txn.user_slot);
        ASSERT_EQ(packed_txn->ref_counts.size(), txn.ref_counts.size());

        for (usize i = 0; i < txn.ref_counts.size(); ++i) {
          EXPECT_EQ(packed_txn->ref_counts[i].unpack(), txn.ref_counts[i]);
        }
      };

      // a. Successful pack with correct space
      {
        llfs::DataPacker packer{llfs::MutableBuffer{memory.data(), memory.size()}};

        PackedPageAllocatorTxn* packed_txn = llfs::pack_object(txn, &packer);

        ASSERT_NO_FATAL_FAILURE(verify_packed_txn(packed_txn));
      }

      StatusOr<const PackedPageAllocatorTxn&> unpacked_txn =
          llfs::unpack_cast<PackedPageAllocatorTxn>(memory);

      ASSERT_TRUE(unpacked_txn.ok()) << BATT_INSPECT(unpacked_txn.status());
      ASSERT_NO_FATAL_FAILURE(verify_packed_txn(std::addressof(*unpacked_txn)));
    }
  }
}

}  // namespace
