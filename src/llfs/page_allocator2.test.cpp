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

#include <llfs/storage_simulation.hpp>
#include <llfs/uuid.hpp>

#include <llfs/testing/scenario_runner.hpp>
#include <llfs/testing/test_config.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <random>
#include <vector>

namespace {

using namespace batt::int_types;

using llfs::None;
using llfs::Optional;
using llfs::Status;
using llfs::StatusOr;
using llfs::experimental::PageAllocator;

class PageAllocator2SimTest : public ::testing::Test
{
 public:
  /** \brief All state of an individual simulation scenario is contained in the Scenario struct, to
   * allow simulations with different random seeds to be run in parallel on different threads.
   */
  struct Scenario {
    const std::string kTestObjectName = "page_allocator2_test";

    const std::string kTestLogName = "page_allocator2_test_log";

    //----- --- -- -  -  -   -

    llfs::RandomSeed seed;

    llfs::StorageSimulation sim;

    llfs::testing::TestConfig test_config;

    llfs::PageCount page_count{32};

    llfs::page_device_id_int page_device_id = 7;

    llfs::Optional<llfs::PageIdFactory> page_ids;

    llfs::MaxAttachments max_attachments{4};

    boost::uuids::uuid user_id = llfs::random_uuid();

    std::unique_ptr<PageAllocator> page_allocator;

    std::function<void(Scenario&)> test_body;

    //----- --- -- -  -  -   -

    explicit Scenario(llfs::RandomSeed seed,
                      const std::function<void(Scenario&)>& test_body) noexcept
        : seed{seed}
        , sim{this->seed}
        , test_body{test_body}
    {
    }

    ~Scenario() noexcept
    {
      BATT_CHECK_EQ(this->page_allocator, nullptr)
          << "Scenario::shut_down must be called while the simulation main function is still "
             "running!";
    }

    /** \brief Run the scenario.
     */
    void run()
    {
      this->sim.run_main_task([&] {
        auto on_scope_exit = batt::finally([&] {
          this->shut_down();
        });
        this->test_body(*this);
      });
    }

    /** \brief Recovers the page allocator under test from a simulated log (under name
     * `this->kTestLogName`), resetting the passed `page_allocator` pointer with the resulting
     * PageAllocator object.
     */
    void recover()
    {
      this->page_ids = llfs::PageIdFactory{this->page_count, this->page_device_id};

      StatusOr<std::unique_ptr<PageAllocator>> recovered = PageAllocator::recover(
          llfs::PageAllocatorRuntimeOptions{
              this->sim.task_scheduler(),
              kTestObjectName,
          },
          *this->page_ids, this->max_attachments,
          *this->sim.get_log_device_factory(kTestLogName,
                                            /*capacity=*/PageAllocator::calculate_log_size(
                                                this->page_count, this->max_attachments)));

      ASSERT_TRUE(recovered.ok()) << BATT_INSPECT(recovered.status());

      this->page_allocator = std::move(*recovered);
    }

    /** \brief Halts the page allocator (if it is recovered) and waits for background tasks to
     * complete.
     */
    void shut_down()
    {
      this->page_allocator.reset();
    }

    /** \brief Attaches the passed user_id to the allocator and optionally waits for it to flush the
     * attachment to the log.
     */
    void attach_user(const boost::uuids::uuid& user_id, bool wait_for_flush = true)
    {
      StatusOr<llfs::slot_offset_type> attach_slot = this->page_allocator->attach_user(user_id);
      ASSERT_TRUE(attach_slot.ok()) << BATT_INSPECT(attach_slot) << BATT_INSPECT(user_id);

      if (wait_for_flush) {
        Status attach_sync = this->page_allocator->sync(*attach_slot);
        ASSERT_TRUE(attach_sync.ok()) << BATT_INSPECT(attach_sync);
      }
    }

    /** \brief Allocates a randomly chosen number of pages (between min_pages and max_pages),
     * returning a corresponding vector of ref_count_updates that can be applied to the
     * PageAllocator.
     */
    void add_new_pages(usize min_pages, usize max_pages, i32 min_ref_count, i32 max_ref_count,
                       std::vector<llfs::PageRefCount>* ref_count_updates)
    {
      BATT_CHECK_NOT_NULLPTR(this->page_allocator);

      const usize n_pages = this->sim.pick_int(min_pages, max_pages);

      for (usize i = 0; i < n_pages; ++i) {
        StatusOr<llfs::PageId> page_id =
            this->page_allocator->allocate_page(batt::WaitForResource::kFalse);

        // If we run out of free pages, but have met the minimum number, succeed.
        //
        if (!page_id.ok() && ref_count_updates->size() >= min_pages) {
          break;
        }

        ASSERT_TRUE(page_id.ok()) << BATT_INSPECT(page_id);

        const i32 n_refs = this->sim.pick_int(min_ref_count, max_ref_count);

        ref_count_updates->emplace_back(llfs::PageRefCount{
            .page_id = *page_id,
            .ref_count = n_refs,
        });
      }
    }
  };
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(PageAllocator2SimTest, Recover)
{
  llfs::testing::ScenarioRunner{}  //
      .n_seeds(1000)
      .n_updates(4)
      .run(batt::StaticType<Scenario>{}, [](Scenario& scenario) {
        ASSERT_NO_FATAL_FAILURE(scenario.recover());

        // Verify that there are no attached users pending recovery.
        //
        {
          Status users_recovered =
              scenario.page_allocator->require_users_recovered(batt::WaitForResource::kFalse);

          ASSERT_TRUE(users_recovered.ok()) << BATT_INSPECT(users_recovered);
        }
        {
          Status users_recovered =
              scenario.page_allocator->require_users_recovered(batt::WaitForResource::kTrue);

          ASSERT_TRUE(users_recovered.ok()) << BATT_INSPECT(users_recovered);
        }

        for (u64 physical_page = 0; physical_page < scenario.page_count; ++physical_page) {
          //+++++++++++-+-+--+----- --- -- -  -  -   -
          // Verify that ref_count starts off as 0 for valid pages.
          //
          {
            llfs::PageId good_page_id =  //
                scenario.page_ids->make_page_id(physical_page, /*generation=*/0);

            {
              StatusOr<llfs::PageRefCount> ref_count =
                  scenario.page_allocator->get_ref_count(llfs::PhysicalPageId{physical_page});

              ASSERT_TRUE(ref_count.ok()) << BATT_INSPECT(ref_count);
              EXPECT_EQ(ref_count->page_id, good_page_id);
              EXPECT_EQ(ref_count->ref_count, 0);
            }
            {
              StatusOr<i32> ref_count = scenario.page_allocator->get_ref_count(good_page_id);

              ASSERT_TRUE(ref_count.ok()) << BATT_INSPECT(ref_count);
              EXPECT_EQ(*ref_count, 0);
            }
          }

          // Failure case: generation wrong: -> batt::StatusCode::kInvalidArgument
          //
          {
            llfs::PageId bad_generation_page_id =  //
                scenario.page_ids->make_page_id(physical_page, /*generation=*/1);

            StatusOr<i32> ref_count =
                scenario.page_allocator->get_ref_count(bad_generation_page_id);

            ASSERT_EQ(ref_count.status(), batt::StatusCode::kInvalidArgument);
          }

          // Failure case: physical_page too large -> batt::StatusCode::kOutOfRange
          //
          {
            llfs::PageIdFactory bad_page_ids{llfs::PageCount{scenario.page_count * 2},
                                             scenario.page_device_id};

            llfs::PageId bad_physical_page_id =  //
                bad_page_ids.make_page_id(physical_page + scenario.page_count, /*generation=*/1);

            StatusOr<i32> ref_count = scenario.page_allocator->get_ref_count(bad_physical_page_id);

            ASSERT_EQ(ref_count.status(), batt::StatusCode::kOutOfRange);
          }
        }
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(PageAllocator2SimTest, UpdateRefCountsNoAttach)
{
  llfs::testing::ScenarioRunner{}  //
      .n_seeds(1000)
      .n_updates(4)
      .run(batt::StaticType<Scenario>{}, [&](Scenario& scenario) {
        ASSERT_NO_FATAL_FAILURE(scenario.recover());

        StatusOr<llfs::SlotReadLock> update_status =
            scenario.page_allocator->update_page_ref_counts(
                scenario.user_id, /*user_slot=*/123456,
                /*ref_counts=*/llfs::as_seq(std::vector<llfs::PageRefCount>{}));

        ASSERT_FALSE(update_status.ok());
        EXPECT_EQ(update_status.status(),
                  llfs::make_status(llfs::StatusCode::kPageAllocatorNotAttached));
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(PageAllocator2SimTest, NewPageRefCountTooSmall)
{
  llfs::testing::ScenarioRunner{}  //
      .n_seeds(100)
      .n_updates(4)
      .run(batt::StaticType<Scenario>{}, [&](Scenario& scenario) {
        ASSERT_NO_FATAL_FAILURE(scenario.recover());
        ASSERT_NO_FATAL_FAILURE(scenario.attach_user(scenario.user_id));

        std::vector<llfs::PageRefCount> ref_count_updates;
        ASSERT_NO_FATAL_FAILURE(scenario.add_new_pages(1, 3, 1, 1, &ref_count_updates));

        StatusOr<llfs::SlotReadLock> txn_lock = scenario.page_allocator->update_page_ref_counts(
            scenario.user_id, /*user_slot=*/0, llfs::as_seq(ref_count_updates));

        ASSERT_EQ(txn_lock.status(),
                  llfs::make_status(llfs::StatusCode::kPageAllocatorInitRefCountTooSmall))
            << BATT_INSPECT(txn_lock.status());
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(PageAllocator2SimTest, AllocatePages)
{
  llfs::testing::ScenarioRunner{}  //
      .n_seeds(100 * 1000)
      .n_updates(4)
      .run(batt::StaticType<Scenario>{}, [&](Scenario& scenario) {
        // Keep track of expected page values.
        //
        std::map<llfs::PageId, i32> expected;
        llfs::slot_offset_type user_slot = 34567;

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        {
          // Create an object to test and attach our user id.
          //
          ASSERT_NO_FATAL_FAILURE(scenario.recover());
          ASSERT_NO_FATAL_FAILURE(scenario.attach_user(scenario.user_id));

          // Generate a set of random page ref count updates, using `allocate_page` to get unused
          // PageIds.
          //
          std::vector<llfs::PageRefCount> ref_count_updates;
          ASSERT_NO_FATAL_FAILURE(scenario.add_new_pages(1, 3, 2, 5, &ref_count_updates));

          // Apply updates.
          //
          StatusOr<llfs::SlotReadLock> txn_lock = scenario.page_allocator->update_page_ref_counts(
              scenario.user_id, user_slot, llfs::as_seq(ref_count_updates));
          ASSERT_TRUE(txn_lock.ok()) << BATT_INSPECT(txn_lock.status());

          // Update expected values.
          //
          for (const llfs::PageRefCount& prc : ref_count_updates) {
            expected[prc.page_id] += prc.ref_count;
          }

          // Validate expected values.
          //
          for (const auto [page_id, expected_ref_count] : expected) {
            StatusOr<i32> actual_ref_count = scenario.page_allocator->get_ref_count(page_id);
            ASSERT_TRUE(actual_ref_count.ok()) << BATT_INSPECT(actual_ref_count);

            EXPECT_EQ(*actual_ref_count, expected_ref_count);
          }

          Status txn_sync = scenario.page_allocator->sync(txn_lock->slot_range().upper_bound);
          ASSERT_TRUE(txn_sync.ok()) << BATT_INSPECT(txn_sync);

        }  // (close everything down)

        scenario.shut_down();

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        {
          // Recover the page allocator from the simulated storage layer.
          //
          ASSERT_NO_FATAL_FAILURE(scenario.recover());

          // Validate expected values.
          //
          for (const auto [page_id, expected_ref_count] : expected) {
            StatusOr<i32> actual_ref_count = scenario.page_allocator->get_ref_count(page_id);
            ASSERT_TRUE(actual_ref_count.ok()) << BATT_INSPECT(actual_ref_count);

            EXPECT_EQ(*actual_ref_count, expected_ref_count);
          }
        }
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(PageAllocator2SimTest, CheckpointAndTrim)
{
  llfs::testing::ScenarioRunner{}  //
      .n_seeds(100 * 1000)
      .n_updates(25)
      .run(batt::StaticType<Scenario>{}, [&](Scenario& scenario) {
        ASSERT_GT(scenario.page_count, 2);
        ASSERT_LE(scenario.page_count, 64);

        constexpr i32 kMaxTestRefCount = 5000;

        // Keep track of expected page values.
        //
        std::vector<i32> expected(scenario.page_count, 2);
        llfs::slot_offset_type user_slot = 0;

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        {
          // Create an object to test and attach our user id.
          //
          ASSERT_NO_FATAL_FAILURE(scenario.recover());
          ASSERT_NO_FATAL_FAILURE(scenario.attach_user(scenario.user_id));

          // Allocate all the pages.
          //
          std::vector<llfs::PageRefCount> ref_count_updates;
          ASSERT_NO_FATAL_FAILURE(scenario.add_new_pages(scenario.page_count, scenario.page_count,
                                                         2, 2, &ref_count_updates));

          usize past_count = 0;

          while (scenario.page_allocator->metrics().checkpoints_count + past_count < 10) {
            user_slot += 1;

            {
              // Apply updates and flush.
              //
              StatusOr<llfs::SlotReadLock> txn_lock =
                  scenario.page_allocator->update_page_ref_counts(scenario.user_id, user_slot,
                                                                  llfs::as_seq(ref_count_updates));
              ASSERT_TRUE(txn_lock.ok()) << BATT_INSPECT(txn_lock.status());

              Status txn_sync = scenario.page_allocator->sync(txn_lock->slot_range().upper_bound);
              ASSERT_TRUE(txn_sync.ok()) << BATT_INSPECT(txn_sync);
            }

            // Simulate a forced shutdown and recovery ~0.5% of the time.
            //
            if (scenario.sim.pick_int(1, 1000) <= 5) {
              past_count += scenario.page_allocator->metrics().checkpoints_count;
              scenario.shut_down();
              ASSERT_NO_FATAL_FAILURE(scenario.recover());

              {
                Status users_recovered_status =
                    scenario.page_allocator->require_users_recovered(batt::WaitForResource::kFalse);

                ASSERT_EQ(users_recovered_status, batt::StatusCode::kUnavailable);
              }

              // Clear out any slot locks from the recovery.
              //
              StatusOr<std::map<llfs::slot_offset_type, llfs::SlotReadLock>> recovered_txns =
                  scenario.page_allocator->get_pending_recovery(scenario.user_id);

              ASSERT_TRUE(recovered_txns.ok()) << BATT_INSPECT(recovered_txns.status());

              // Verify that all recovered txn slot numbers are in the expected range.
              //
              for (const auto& kvp : *recovered_txns) {
                EXPECT_GE(kvp.first, 0);
                EXPECT_LE(kvp.first, user_slot);
              }
              recovered_txns->clear();

              {
                Status notify_status =
                    scenario.page_allocator->notify_user_recovered(scenario.user_id);

                ASSERT_TRUE(notify_status.ok()) << BATT_INSPECT(notify_status);
              }
              {
                Status users_recovered_status =
                    scenario.page_allocator->require_users_recovered(batt::WaitForResource::kFalse);

                ASSERT_TRUE(users_recovered_status.ok()) << BATT_INSPECT(users_recovered_status);
              }
            }

            // Validate expected values.
            //
            for (usize physical_page = 0; physical_page < scenario.page_count; ++physical_page) {
              StatusOr<llfs::PageRefCount> actual =
                  scenario.page_allocator->get_ref_count(llfs::PhysicalPageId{physical_page});

              ASSERT_TRUE(actual.ok()) << BATT_INSPECT(actual);
              EXPECT_EQ(actual->ref_count, expected[physical_page]);

              // Verify ref counts are inside the test range.
              //
              if (physical_page + 2 >= scenario.page_count) {
                // Slow page.
                //
                EXPECT_EQ(actual->ref_count, 2);

              } else {
                // Fast page.
                //
                EXPECT_GE(actual->ref_count, 2);
                EXPECT_LE(actual->ref_count, kMaxTestRefCount);
              }
            }

            // Create a new random update.
            //
            ref_count_updates.clear();

            // Generate a random bitset to select the pages for update; never update two of the
            // pages, to test that "slow" pages are correctly refreshed.
            //
            u64 update_set =
                scenario.sim.pick_int(1, (usize{1} << (i32)(scenario.page_count - 2)) - 1);

            while (update_set != 0) {
              const usize physical_page = batt::log2_floor(update_set);
              i32 delta = -1;
              if (expected[physical_page] < kMaxTestRefCount &&
                  (expected[physical_page] == 2 || scenario.sim.pick_int(0, 1) == 1)) {
                delta = 1;
              }

              ref_count_updates.emplace_back(llfs::PageRefCount{
                  .page_id = scenario.page_ids->make_page_id(physical_page, /*generation=*/0),
                  .ref_count = delta,
              });

              expected[physical_page] += delta;
              update_set &= ~(u64{1} << physical_page);
            }

            ASSERT_GT(ref_count_updates.size(), 0);
          }
        }
      });
}

// Test Plan:
//  1. calculate log size
//  2. recover from empty log
//     a. verify all ref counts are 0
//     b. allocate_page should succeed without blocking until no more pages
//     c. deallocate_page should unblock an allocator; allow page to be reallocated (same
//        generation).
//  3. try to update pages without attaching - fail
//  -. simulate workload with hot pages and cold pages; run for long enough so that log rolls over
//     several times
//     - verify that the hot page ref count updates don't cause the cold page ref counts to be lost
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
