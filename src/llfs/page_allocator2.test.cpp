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

#include <batteries/small_vec.hpp>

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
  static constexpr usize kTestUserCount = 3;
  static constexpr usize kTestMaxAttachments = kTestUserCount + 1;
  static constexpr usize kTestPageCount = 32;
  static constexpr usize kMaxUpdatesPerUser = 5;

  struct Scenario;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  class PageSet
  {
   public:
    using Self = PageSet;

    static u64 mask(usize i) noexcept
    {
      BATT_CHECK_LT(i, 64);
      return u64{1} << i;
    }

    static PageSet pick_random(usize page_count, llfs::StorageSimulation& sim)
    {
      return PageSet{sim.pick_int(1, Self::mask(page_count) - 1)};
    }

    //----- --- -- -  -  -   -

    explicit PageSet(u64 bitset = 0) noexcept : bitset_{bitset}
    {
    }

    PageSet(const PageSet&) = default;
    PageSet& operator=(const PageSet&) = default;

    bool empty() const noexcept
    {
      return this->bitset_ == 0;
    }

    usize size() const noexcept
    {
      return batt::bit_count(this->bitset_);
    }

    void insert(usize i) noexcept
    {
      this->bitset_ |= Self::mask(i);
    }

    void erase(usize i) noexcept
    {
      this->bitset_ &= ~Self::mask(i);
    }

    bool contains(usize i) noexcept
    {
      return (this->bitset_ & Self::mask(i)) != 0;
    }

    usize pop() noexcept
    {
      BATT_CHECK(!this->empty());
      const usize i = batt::log2_floor(this->bitset_);
      this->erase(i);
      return i;
    }

    PageSet intersect_with(const PageSet& other) const noexcept
    {
      return PageSet{this->bitset_ & other.bitset_};
    }

    PageSet union_with(const PageSet& other) const noexcept
    {
      return PageSet{this->bitset_ | other.bitset_};
    }

   private:
    u64 bitset_;
  };

  struct SimulatedUserMessage {
    usize sender_i;

    llfs::PageId page_id;
  };

  struct SimulatedUserSlot {
    u64 sim_step;

    PageSet page_set;

    batt::SmallVec<llfs::PageRefCount, kTestPageCount> updates;

    bool committed = false;

    llfs::SlotReadLock allocator_lock;
  };

  struct SimulatedUser {
    usize index;

    boost::uuids::uuid user_id = llfs::random_uuid();

    batt::SmallVec<llfs::PageRefCount, kTestPageCount> root_refs;

    batt::SmallVec<i32, kTestPageCount> lock_count;

    batt::SmallVec<SimulatedUserSlot, kMaxUpdatesPerUser> slots;

    batt::SmallVec<llfs::PageId, kTestPageCount> dead_pages;

    std::deque<SimulatedUserMessage> inbox;

    Status status;

    std::shared_ptr<batt::Task> task;

    bool attached = false;

    bool done = false;

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    PageSet root_pages() const noexcept
    {
      PageSet result;
      for (usize i = 0; i < root_refs.size(); ++i) {
        if (this->root_refs[i].ref_count != 0) {
          result.insert(i);
        }
      }
      return result;
    }

    void initialize(Scenario& scenario);

    void recover(Scenario& scenario);

    void take_action(Scenario& scenario);

    void send_message(Scenario& scenario);

    void read_message(Scenario& scenario);

    void update_refs(Scenario& scenario);

    void recycle_pages(Scenario& scenario);

    void new_pages(Scenario& scenario);

    void commit_slot(Scenario& scenario, llfs::slot_offset_type user_slot);

    void set_error(const Status& e)
    {
      this->status = e;
    }

    usize root_count() const noexcept
    {
      usize total = 0;
      for (const llfs::PageRefCount& prc : this->root_refs) {
        if (prc.ref_count != 0) {
          total += 1;
        }
      }
      return total;
    }

    usize nth_root_ref(usize n) const noexcept
    {
      usize seen = 0;
      for (usize i = 0; i < this->root_refs.size(); ++i) {
        if (this->root_refs[i].ref_count != 0) {
          if (seen == n) {
            return i;
          }
          ++seen;
        }
      }
      BATT_PANIC() << "n value is out of bounds!" << BATT_INSPECT(n)
                   << BATT_INSPECT_RANGE(this->root_refs);
      BATT_UNREACHABLE();
    }

    template <typename T>
    [[nodiscard]] bool require_ok(const StatusOr<T>& s)
    {
      if (s.ok()) {
        return true;
      }
      this->set_error(s.status());
      return false;
    }

    [[nodiscard]] bool require_ok(const Status& s)
    {
      if (s.ok()) {
        return true;
      }
      this->set_error(s);
      return false;
    }

    bool ok() const noexcept
    {
      return this->status.ok();
    }

    void crash() noexcept
    {
      for (SimulatedUserSlot& slot : this->slots) {
        slot.allocator_lock.clear();
      }
    }
  };

  enum struct CommitAssumption {
    kCommitted,
    kNotCommitted,
  };

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

    llfs::PageCount page_count{kTestPageCount};

    llfs::page_device_id_int page_device_id = 7;

    llfs::Optional<llfs::PageIdFactory> page_ids;

    llfs::MaxAttachments max_attachments{kTestMaxAttachments};

    boost::uuids::uuid user_id = llfs::random_uuid();

    std::unique_ptr<PageAllocator> page_allocator;

    std::function<void(Scenario&)> test_body;

    std::array<SimulatedUser, kTestUserCount> users;

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

    usize find_user(SimulatedUser* user) const noexcept
    {
      isize i = user - this->users.data();

      BATT_CHECK_GE(i, 0);
      BATT_CHECK_LT(i, this->users.size());

      return i;
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
    template <typename VecT>
    void add_new_pages(usize min_pages, usize max_pages, i32 min_ref_count, i32 max_ref_count,
                       VecT* ref_count_updates)
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

    bool verify_slots(const SimulatedUserSlot** first, const SimulatedUserSlot** last,
                      Optional<CommitAssumption> next_assumption = None,
                      batt::SmallVec<llfs::PageRefCount, kTestPageCount> expected = {}) const
    {
      if (expected.size() != this->page_count) {
        BATT_CHECK(expected.empty());

        expected.resize(this->page_count);
        for (usize physical_page = 0; physical_page < expected.size(); ++physical_page) {
          expected[physical_page].page_id =
              this->page_ids->make_page_id(physical_page, /*generation=*/0);
          expected[physical_page].ref_count = 0;
        }
      }

      for (; first != last; ++first) {
        if (!(*first)->committed) {
          if (next_assumption == None) {
            return this->verify_slots(first, last, CommitAssumption::kCommitted, expected) ||
                   this->verify_slots(first, last, CommitAssumption::kNotCommitted, expected);
          }
          CommitAssumption assumption = *next_assumption;
          next_assumption = None;
          if (assumption == CommitAssumption::kNotCommitted) {
            continue;
          }
        }
        for (const llfs::PageRefCount& update : (*first)->updates) {
          const usize physical_page = this->page_ids->get_physical_page(update.page_id);

          if (expected[physical_page].page_id != update.page_id) {
            return false;
          }

          const usize generation = this->page_ids->get_generation(update.page_id);
          if (update.ref_count == llfs::kRefCount_1_to_0) {
            if (expected[physical_page].ref_count != 1) {
              return false;
            }
            expected[physical_page].ref_count = 0;
            expected[physical_page].page_id =
                this->page_ids->make_page_id(physical_page, generation + 1);

          } else {
            if (expected[physical_page].ref_count == 1) {
              return false;
            }

            const i32 new_ref_count = expected[physical_page].ref_count + update.ref_count;
            if (new_ref_count < 0) {
              return false;
            }

            expected[physical_page].ref_count = new_ref_count;
          }
        }
      }

      batt::SmallVec<llfs::PageRefCount, kTestPageCount> actual;

      for (usize physical_page = 0; physical_page < expected.size(); ++physical_page) {
        actual.emplace_back(batt::ok_result_or_panic(
            this->page_allocator->get_ref_count(llfs::PhysicalPageId{physical_page})));
      }

      if (actual != expected) {
        LLFS_LOG_INFO() << BATT_INSPECT(expected.size()) << BATT_INSPECT(actual.size()) << std::endl
                        << BATT_INSPECT_RANGE(expected) << std::endl
                        << BATT_INSPECT_RANGE(actual);

        if (actual.size() == expected.size()) {
          for (usize i = 0; i < expected.size(); ++i) {
            if (expected[i] != actual[i]) {
              LLFS_LOG_INFO() << "[" << i << "]" << BATT_INSPECT(expected[i])
                              << BATT_INSPECT(actual[i]);
            }
          }
        }
        return false;
      }

      return true;
    }
  };
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator2SimTest::SimulatedUser::initialize(Scenario& scenario)
{
  const usize user_i = scenario.find_user(this);

  this->index = user_i;

  LLFS_VLOG(1) << "Initializing user " << user_i;

  this->root_refs.resize(scenario.page_count, llfs::PageRefCount{
                                                  .page_id = llfs::PageId{},
                                                  .ref_count = 0,
                                              });

  this->lock_count.resize(scenario.page_count, 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator2SimTest::SimulatedUser::recover(Scenario& scenario)
{
  const bool was_attached = this->attached;
  if (!this->attached) {
    LLFS_VLOG(1) << "User " << this->index << " not attached; attaching...";

    StatusOr<llfs::slot_offset_type> attach_slot =
        scenario.page_allocator->attach_user(this->user_id);

    if (!this->require_ok(attach_slot)) {
      return;
    }

    Status sync_status = scenario.page_allocator->sync(*attach_slot);
    if (!this->require_ok(sync_status)) {
      return;
    }

    this->attached = true;
  }

  {
    StatusOr<std::map<llfs::slot_offset_type, llfs::SlotReadLock>> recovered_txns =
        scenario.page_allocator->get_pending_recovery(this->user_id);

    if (!recovered_txns.ok()) {
      ASSERT_FALSE(was_attached);

    } else {
      LLFS_VLOG(1) << "User " << this->index << " recovered " << recovered_txns->size()
                   << " pending txns";

      for (auto& [slot_offset, slot_read_lock] : *recovered_txns) {
        ASSERT_LT(slot_offset, this->slots.size());
        this->slots[slot_offset].committed = true;
      }

      for (llfs::slot_offset_type user_slot = 0; user_slot < this->slots.size(); ++user_slot) {
        SimulatedUserSlot& slot = this->slots[user_slot];
        if (!slot.committed) {
          ASSERT_NO_FATAL_FAILURE(this->commit_slot(scenario, user_slot));
          EXPECT_TRUE(slot.committed);
        }
      }

      Status notify_status = scenario.page_allocator->notify_user_recovered(this->user_id);
      if (!notify_status.ok()) {
        this->set_error(notify_status);
      }
    }
  }

  LLFS_VLOG(1) << "User " << this->index << " finished recovery";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator2SimTest::SimulatedUser::take_action(Scenario& scenario)
{
  LLFS_VLOG(1) << "User " << this->index << ": take_action";

  // Allocate a slot for updates on this step.
  //
  const llfs::slot_offset_type slot_offset = this->slots.size();
  if (slot_offset == kMaxUpdatesPerUser) {
    this->done = true;
    return;
  }
  BATT_CHECK_LT(slot_offset, kMaxUpdatesPerUser);

  SimulatedUserSlot& slot = this->slots.emplace_back();
  slot.sim_step = scenario.sim.current_step();

  for (usize retry = 0; retry < 2; ++retry) {
    const bool second_try = (retry != 0);

    // Send a message to some other user.
    //
    if (scenario.users.size() >= 2 && this->root_count() > 0 &&
        (second_try || scenario.sim.pick_branch())) {
      ASSERT_NO_FATAL_FAILURE(this->send_message(scenario));
    }

    // Process incoming messages, if there are any.
    //
    if (!this->inbox.empty() && (second_try || scenario.sim.pick_branch())) {
      ASSERT_NO_FATAL_FAILURE(this->read_message(scenario));
    }

    // Update existing counts.
    //
    if (this->root_count() > 0 && (second_try || scenario.sim.pick_branch())) {
      ASSERT_NO_FATAL_FAILURE(this->update_refs(scenario));
    }

    // Recycle dead pages.
    //
    if (!this->dead_pages.empty() && (second_try || scenario.sim.pick_branch())) {
      ASSERT_NO_FATAL_FAILURE(this->recycle_pages(scenario));
    }

    // Allocate some new pages.
    //
    if (second_try || scenario.sim.pick_branch()) {
      ASSERT_NO_FATAL_FAILURE(this->new_pages(scenario));
    }

    if (!slot.updates.empty()) {
      break;
    }
  }

  // Check to make sure we can proceed.
  //
  if (slot.updates.empty()) {
    slot.committed = true;
    return;
  }
  if (!this->ok()) {
    return;
  }

  // Apply the updates.
  //
  ASSERT_NO_FATAL_FAILURE(this->commit_slot(scenario, slot_offset));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator2SimTest::SimulatedUser::send_message(Scenario& scenario)
{
  LLFS_VLOG(1) << "User " << this->index << ": send_message";

  const usize n_roots = this->root_count();
  const usize physical_page_to_send = this->nth_root_ref(scenario.sim.pick_int(0, n_roots - 1));
  usize receiver_i = scenario.sim.pick_int(0, scenario.users.size() - 2);
  if (receiver_i >= this->index) {
    ++receiver_i;
  }
  SimulatedUser& receiver = scenario.users[receiver_i];
  const llfs::PageRefCount& root_ref = this->root_refs[physical_page_to_send];
  BATT_CHECK_GT(root_ref.ref_count, 0);

  LLFS_VLOG(1) << " -- sending " << root_ref << ": " << this->index << " -> " << receiver_i;

  this->lock_count[physical_page_to_send] += 1;
  receiver.inbox.emplace_back(SimulatedUserMessage{
      .sender_i = this->index,
      .page_id = root_ref.page_id,
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator2SimTest::SimulatedUser::read_message(Scenario& scenario)
{
  LLFS_VLOG(1) << "User " << this->index << ": read_message";

  BATT_CHECK(!this->inbox.empty());
  const SimulatedUserMessage message = this->inbox.front();
  this->inbox.pop_front();

  LLFS_VLOG(1) << " -- received" << BATT_INSPECT(message.page_id);

  BATT_CHECK(!this->slots.empty());
  SimulatedUserSlot& slot = this->slots.back();

  // Add the sent page to the updates for this slot.
  //
  const usize i = scenario.page_ids->get_physical_page(message.page_id);
  if (!slot.page_set.contains(i)) {
    slot.page_set.insert(i);
    slot.updates.emplace_back(llfs::PageRefCount{
        .page_id = message.page_id,
        .ref_count = 1,
    });
  }

  // Add the page to this user's root ref set.
  //
  const usize physical_page = scenario.page_ids->get_physical_page(message.page_id);
  llfs::PageRefCount& prc = this->root_refs[physical_page];
  prc.page_id = message.page_id;
  prc.ref_count += 1;

  // Release the lock in the sender's context (this is roughly equivalent to a SlotReadLock in
  // the sender's volume root log).
  //
  SimulatedUser& sender = scenario.users[message.sender_i];
  BATT_CHECK_GT(sender.lock_count[physical_page], 0);
  sender.lock_count[physical_page] -= 1;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator2SimTest::SimulatedUser::update_refs(Scenario& scenario)
{
  LLFS_VLOG(1) << "User " << this->index << ": update_refs";

  BATT_CHECK(!this->slots.empty());
  SimulatedUserSlot& slot = this->slots.back();

  PageSet update_set = PageSet::pick_random(scenario.page_count, scenario.sim)  //
                           .intersect_with(this->root_pages());

  LLFS_VLOG(1) << " --" << BATT_INSPECT(update_set.size());

  while (!update_set.empty()) {
    const usize physical_page = update_set.pop();
    if (slot.page_set.contains(physical_page)) {
      continue;
    }
    slot.page_set.insert(physical_page);

    llfs::PageRefCount& prc = this->root_refs[physical_page];
    BATT_CHECK_NE(prc.ref_count, 0);

    const i32 min_new_count = (this->lock_count[physical_page] > 0) ? 1 : 0;
    i32 new_count = scenario.sim.pick_int(min_new_count, prc.ref_count);
    if (new_count == prc.ref_count) {
      new_count += 1;
    }
    slot.updates.emplace_back(llfs::PageRefCount{
        .page_id = prc.page_id,
        .ref_count = new_count - prc.ref_count,
    });
    prc.ref_count = new_count;
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//

void PageAllocator2SimTest::SimulatedUser::recycle_pages(Scenario& scenario)
{
  LLFS_VLOG(1) << "User " << this->index << ": recycle_pages; dead_pages["
               << this->dead_pages.size() << "] == " << batt::dump_range(this->dead_pages);

  BATT_CHECK(!this->slots.empty());
  SimulatedUserSlot& slot = this->slots.back();

  batt::SmallVec<llfs::PageId, kTestPageCount> local_dead_pages;
  std::swap(this->dead_pages, local_dead_pages);

  for (llfs::PageId dead_page_id : local_dead_pages) {
    const usize physical_page = scenario.page_ids->get_physical_page(dead_page_id);
    BATT_CHECK_EQ(slot.page_set.contains(physical_page), false);
    slot.page_set.insert(physical_page);

    slot.updates.emplace_back(llfs::PageRefCount{
        .page_id = dead_page_id,
        .ref_count = llfs::kRefCount_1_to_0,
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator2SimTest::SimulatedUser::new_pages(Scenario& scenario)
{
  LLFS_VLOG(1) << "User " << this->index << ": new_pages";

  BATT_CHECK(!this->slots.empty());
  SimulatedUserSlot& slot = this->slots.back();

  const usize n_pages = scenario.sim.pick_int(1, 5);
  LLFS_VLOG(1) << " --" << BATT_INSPECT(n_pages);

  for (usize i = 0; i < n_pages; ++i) {
    StatusOr<llfs::PageId> new_page_id =
        scenario.page_allocator->allocate_page(batt::WaitForResource::kFalse);

    if (!new_page_id.ok()) {
      if (new_page_id.status() != batt::StatusCode::kResourceExhausted) {
        this->set_error(new_page_id.status());
      } else {
        LLFS_VLOG(1) << " -- out of pages (skipping new_pages)";
      }
      return;
    }

    const usize physical_page = scenario.page_ids->get_physical_page(*new_page_id);

    llfs::PageRefCount& root_ref = this->root_refs[physical_page];
    root_ref.page_id = *new_page_id;
    root_ref.ref_count = 1;

    BATT_CHECK_EQ(slot.page_set.contains(physical_page), false);
    slot.page_set.insert(physical_page);

    slot.updates.emplace_back(llfs::PageRefCount{
        .page_id = *new_page_id,
        .ref_count = 2,
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator2SimTest::SimulatedUser::commit_slot(Scenario& scenario,
                                                       llfs::slot_offset_type user_slot)
{
  BATT_CHECK_LT(user_slot, this->slots.size());
  SimulatedUserSlot& slot = this->slots[user_slot];

  LLFS_VLOG(1) << "User " << this->index << ": commit_slot; updates[" << slot.updates.size()
               << "] == " << batt::dump_range(slot.updates);

  StatusOr<llfs::SlotReadLock> update_slot = scenario.page_allocator->update_page_ref_counts(
      this->user_id, user_slot, llfs::as_seq(slot.updates),
      /*garbage_collect_fn=*/[this](llfs::PageId dead_page_id) {
        this->dead_pages.emplace_back(dead_page_id);
      });

  if (!this->require_ok(update_slot)) {
    return;
  }

  slot.allocator_lock = std::move(*update_slot);

  Status sync_status = scenario.page_allocator->sync(slot.allocator_lock.slot_range().upper_bound);
  if (!this->require_ok(sync_status)) {
    return;
  }

  slot.committed = true;
  slot.allocator_lock.clear();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

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
      .run(batt::StaticType<Scenario>{}, [](Scenario& scenario) {
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
      .run(batt::StaticType<Scenario>{}, [](Scenario& scenario) {
        ASSERT_NO_FATAL_FAILURE(scenario.recover());
        ASSERT_NO_FATAL_FAILURE(scenario.attach_user(scenario.user_id));

        batt::SmallVec<llfs::PageRefCount, kTestPageCount> ref_count_updates;
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
      .run(batt::StaticType<Scenario>{}, [](Scenario& scenario) {
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
          batt::SmallVec<llfs::PageRefCount, kTestPageCount> ref_count_updates;
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
      .run(batt::StaticType<Scenario>{}, [](Scenario& scenario) {
        ASSERT_GT(scenario.page_count, 2);
        ASSERT_LE(scenario.page_count, 64);

        constexpr i32 kMaxTestRefCount = 5000;

        // Keep track of expected page values.
        //
        batt::SmallVec<i32, kTestPageCount> expected(scenario.page_count, 2);
        llfs::slot_offset_type user_slot = 0;

        //+++++++++++-+-+--+----- --- -- -  -  -   -
        {
          // Create an object to test and attach our user id.
          //
          ASSERT_NO_FATAL_FAILURE(scenario.recover());
          ASSERT_NO_FATAL_FAILURE(scenario.attach_user(scenario.user_id));

          // Allocate all the pages.
          //
          batt::SmallVec<llfs::PageRefCount, kTestPageCount> ref_count_updates;
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
            PageSet update_set = PageSet::pick_random(scenario.page_count - 2, scenario.sim);

            while (!update_set.empty()) {
              const usize physical_page = update_set.pop();
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
            }

            ASSERT_GT(ref_count_updates.size(), 0);
          }
        }
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
TEST_F(PageAllocator2SimTest, MultiUserUpdates)
{
  llfs::testing::ScenarioRunner{}  //
      .n_seeds(1)
      .n_updates(1)
      .n_threads(1)
      .run(batt::StaticType<Scenario>{}, [](Scenario& scenario) {
        (void)scenario;

        // Plan:
        //  - spawn N user tasks, that each:
        //    1. attach to the page allocator
        //    2. choose one of the following actions (if available):
        //       - (try to) allocate 0 or more page(s), add 0 or more pages that are already known
        //          by that user, write an update
        //       - pin a page and share its id with another user
        //       - drop a reference to an unpinned page
        //       - process a message
        //

        ASSERT_NO_FATAL_FAILURE(scenario.recover());

        for (SimulatedUser& user : scenario.users) {
          ASSERT_NO_FATAL_FAILURE(user.initialize(scenario));
        }

        for (SimulatedUser& user : scenario.users) {
          user.task = std::make_shared<batt::Task>(
              scenario.sim.schedule_task(),

              // Task main:
              //
              [&scenario, p_user = &user] {
                ASSERT_NO_FATAL_FAILURE(p_user->recover(scenario));

                LLFS_LOG_INFO() << BATT_INSPECT(p_user->done) << BATT_INSPECT(p_user->status);

                while (!p_user->done && p_user->ok()) {
                  batt::Task::yield();
                  ASSERT_NO_FATAL_FAILURE(p_user->take_action(scenario));
                }
              },

              /*name=*/batt::to_string("simulated_user_task_", user.index));
        }

        for (SimulatedUser& user : scenario.users) {
          user.task->join();
        }

        scenario.page_allocator->halt();
        scenario.page_allocator->join();

        std::vector<const SimulatedUserSlot*> all_slots;
        for (const SimulatedUser& user : scenario.users) {
          for (const SimulatedUserSlot& slot : user.slots) {
            all_slots.emplace_back(&slot);
          }
        }

        std::sort(all_slots.begin(), all_slots.end(),
                  [](const SimulatedUserSlot* a, const SimulatedUserSlot* b) {
                    return a->sim_step < b->sim_step;
                  });

        const bool verified =
            scenario.verify_slots(all_slots.data(), all_slots.data() + all_slots.size());

        BATT_CHECK(verified);
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
