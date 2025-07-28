//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache.hpp>
//

#include <llfs/committable_page_cache_job.hpp>
#include <llfs/ioring_page_file_device.hpp>
#include <llfs/memory_log_device.hpp>
#include <llfs/metrics.hpp>
#include <llfs/new_page_view.hpp>
#include <llfs/page_cache_job.hpp>
#include <llfs/sharded_page_view.hpp>
#include <llfs/status_code.hpp>

#include <boost/range/irange.hpp>
#include <boost/uuid/random_generator.hpp>  // TODO [tastolfi 2021-04-05] remove me

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize get_page_size(const PageDeviceEntry* entry)
{
  return entry ? get_page_size(entry->arena) : 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageCache::PageDeleterImpl::PageDeleterImpl(PageCache& page_cache) noexcept
    : page_cache_{page_cache}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCache::PageDeleterImpl::delete_pages(const Slice<const PageToRecycle>& to_delete,
                                                PageRecycler& recycler,
                                                slot_offset_type caller_slot,
                                                batt::Grant& recycle_grant,
                                                i32 recycle_depth) /*override*/
{
  if (to_delete.empty()) {
    return OkStatus();
  }
  BATT_DEBUG_INFO("delete_pages()" << BATT_INSPECT_RANGE(to_delete) << BATT_INSPECT(recycle_depth));

  const boost::uuids::uuid& caller_uuid = recycler.uuid();

  JobCommitParams params{
      .caller_uuid = &caller_uuid,
      .caller_slot = caller_slot,
      .recycler = as_ref(recycler),
      .recycle_grant = &recycle_grant,
      .recycle_depth = recycle_depth,
  };

  // We must drop the page from the storage device and decrement ref counts
  // as an atomic transaction, so create a PageCacheJob.
  //
  std::unique_ptr<PageCacheJob> job = this->page_cache_.new_job();

  // Add the page to the job's delete list; this will load the page to verify
  // we can trace its refs.
  //
  LLFS_VLOG(1) << "[PageDeleterImpl::delete_pages] deleting: "
               << batt::dump_range(to_delete, batt::Pretty::True);

  for (const PageToRecycle& next_page : to_delete) {
    BATT_CHECK_EQ(next_page.depth, params.recycle_depth);
    Status pre_delete_status = job->delete_page(next_page.page_id);
    BATT_REQUIRE_OK(pre_delete_status);
  }

  // Commit the job.
  //
  Status job_status = commit(std::move(job), params, Caller::PageRecycler_recycle_task_main);
  BATT_REQUIRE_OK(job_status);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<std::shared_ptr<PageCache>> PageCache::make_shared(
    std::vector<PageArena>&& storage_pool, const PageCacheOptions& options)
{
  initialize_status_codes();

  std::shared_ptr<PageCache> page_cache{new PageCache(std::move(storage_pool), options)};

  return page_cache;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCache::PageCache(std::vector<PageArena>&& storage_pool,
                     const PageCacheOptions& options) noexcept
    : options_{options}
    , metrics_{}
    , cache_slot_pool_{PageCacheSlot::Pool::make_new(
          options.cache_slot_count, options.max_cache_size_bytes, "PageCacheSlot_Pool")}
    , page_readers_{std::make_shared<batt::Mutex<PageLayoutReaderMap>>()}
{
#if 0
  this->cache_slot_pool_by_page_size_log2_.fill(nullptr);
#endif

  // Find the maximum page device id value.
  //
  page_device_id_int max_page_device_id = 0;
  for (const PageArena& arena : storage_pool) {
    max_page_device_id = std::max(max_page_device_id, arena.device().get_id());
  }

  // Add sharded page view devices to storage pool.
  //
  std::vector<PageArena> sharded_view_arenas;
  std::unordered_map<page_device_id_int, std::vector<page_device_id_int>> sharded_view_mappings;
  for (const std::pair<PageSize, PageSize>& sharded_view : options.sharded_views) {
    // For every sharded view type requested (page_size -> shard_size), we find all devices whose
    // page size matches, and create sharded views for each of them.
    //
    for (PageArena& arena : storage_pool) {
      // Is this the right sized page?
      //
      if (get_page_size(arena) == sharded_view.first) {
        // Increase the max_page_device_id to allocate a new identifier for the sharded view
        // device.
        //
        const page_device_id_int sharded_view_id = max_page_device_id + 1;

        // Warning: not all PageDevice types currently support sharded views!
        //
        std::unique_ptr<PageDevice> sharded_view_device =
            arena.device().make_sharded_view(sharded_view_id, sharded_view.second);

        if (sharded_view_device != nullptr) {
          max_page_device_id = sharded_view_id;

          // Add a sharded view mapping; we will use this to link devices to their sharded views
          // later.
          //
          sharded_view_mappings[arena.id()].emplace_back(sharded_view_id);

          // Create a PageDevice and PageArena for the sharded view.
          //
          sharded_view_arenas.emplace_back(PageArena{
              std::move(sharded_view_device),
              nullptr,
          });
        }
      }
    }
  }
  storage_pool.insert(storage_pool.end(),  //
                      std::make_move_iterator(sharded_view_arenas.begin()),
                      std::make_move_iterator(sharded_view_arenas.end()));

  // Populate this->page_devices_.
  //
  this->page_devices_.resize(max_page_device_id + 1);
  for (PageArena& arena : storage_pool) {
    const page_device_id_int device_id = arena.device().get_id();
    const auto page_size_log2 = batt::log2_ceil(arena.device().page_size());

    BATT_CHECK_EQ(PageSize{1} << page_size_log2, arena.device().page_size())
        << "Page sizes must be powers of 2!";

    BATT_CHECK_LT(page_size_log2, kMaxPageSizeLog2);

#if 0
    // Create a slot pool for this page size if we haven't already done so.
    //
    if (!this->cache_slot_pool_by_page_size_log2_[page_size_log2]) {
      this->cache_slot_pool_by_page_size_log2_[page_size_log2] = PageCacheSlot::Pool::make_new(
          /*n_slots=*/SlotCount{this->options_.max_cached_pages_per_size_log2[page_size_log2]},
          /*name=*/batt::to_string("size_", u64{1} << page_size_log2));
    }
#endif

    BATT_CHECK_LT(device_id, this->page_devices_.size());
    BATT_CHECK_EQ(this->page_devices_[device_id], nullptr)
        << "Duplicate entries found for the same device id!" << BATT_INSPECT(device_id);

    this->page_devices_[device_id] = std::make_unique<PageDeviceEntry>(  //
        std::move(arena),                                                //
        batt::make_copy(this->cache_slot_pool_)
#if 0
        batt::make_copy(this->cache_slot_pool_by_page_size_log2_[page_size_log2])  //
#endif
    );

    PageDeviceEntry& device_entry = *this->page_devices_[device_id];

    if (!device_entry.arena.has_allocator()) {
      device_entry.can_alloc = false;
    }

    // We will sort these later.
    //
    this->page_devices_by_page_size_.emplace_back(this->page_devices_[device_id].get());
  }
  BATT_CHECK_EQ(this->page_devices_by_page_size_.size(), storage_pool.size());

  // Sort the storage pool by page size (MUST be first).
  //
  std::sort(this->page_devices_by_page_size_.begin(), this->page_devices_by_page_size_.end(),
            PageSizeOrder{});

  // Index the storage pool into groups of arenas by page size.
  //
  for (usize size_log2 = 6; size_log2 < kMaxPageSizeLog2; ++size_log2) {
    auto iter_pair = std::equal_range(this->page_devices_by_page_size_.begin(),
                                      this->page_devices_by_page_size_.end(),
                                      PageSize{u32{1} << size_log2}, PageSizeOrder{});

    this->page_devices_by_page_size_log2_[size_log2] =
        as_slice(this->page_devices_by_page_size_.data() +
                     std::distance(this->page_devices_by_page_size_.begin(), iter_pair.first),
                 as_range(iter_pair).size());
  }

  // Cross-link all page devices to their sharded view entries.
  //
  for (const auto& [device_id, sharded_view_ids] : sharded_view_mappings) {
    BATT_CHECK_LT(device_id, this->page_devices_.size());
    PageDeviceEntry& device_entry = *this->page_devices_[device_id];
    for (page_device_id_int sharded_view_id : sharded_view_ids) {
      PageDeviceEntry& sharded_view_entry = *this->page_devices_[sharded_view_id];
      const i32 shard_size_log2 = batt::log2_ceil(get_page_size(sharded_view_entry.arena));

      // Sanity checks.
      //
      BATT_CHECK(!sharded_view_entry.can_alloc);
      BATT_CHECK_GT(shard_size_log2, 8);
      BATT_CHECK_LT(shard_size_log2, 32);
      BATT_CHECK_EQ(device_entry.sharded_views[shard_size_log2], nullptr);

      device_entry.sharded_views[shard_size_log2] = std::addressof(sharded_view_entry);
    }
  }

  // Register metrics.
  //
  const auto metric_name = [this](std::string_view property) {
    return batt::to_string("PageCache_", property);
  };

#define ADD_METRIC_(n) global_metric_registry().add(metric_name(#n), this->metrics_.n)

  ADD_METRIC_(total_bytes_written);
  ADD_METRIC_(used_bytes_written);
  ADD_METRIC_(node_write_count);
  ADD_METRIC_(leaf_write_count);
  // ADD_METRIC_(page_read_latency);
  // ADD_METRIC_(page_write_latency);
  ADD_METRIC_(pipeline_wait_latency);
  ADD_METRIC_(update_ref_counts_latency);
  ADD_METRIC_(ref_count_sync_latency);

#undef ADD_METRIC_
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCache::~PageCache() noexcept
{
  this->close();
  this->join();

  global_metric_registry()  //
      .remove(this->metrics_.total_bytes_written)
      .remove(this->metrics_.used_bytes_written)
      .remove(this->metrics_.node_write_count)
      .remove(this->metrics_.leaf_write_count)
      //.remove(this->metrics_.page_read_latency)
      //.remove(this->metrics_.page_write_latency)
      .remove(this->metrics_.pipeline_wait_latency)
      .remove(this->metrics_.update_ref_counts_latency)
      .remove(this->metrics_.ref_count_sync_latency);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageCacheOptions& PageCache::options() const
{
  return this->options_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCache::assign_filter_device(PageSize src_page_size, PageSize filter_page_size) noexcept
{
  // Find and cache the PageDeviceEntry for the filter device and leaf device.
  //
  llfs::Slice<llfs::PageDeviceEntry* const> src_entries =
      this->devices_with_page_size(src_page_size);

  llfs::Slice<llfs::PageDeviceEntry* const> filter_entries =
      this->devices_with_page_size(filter_page_size);

  if (src_entries.size() == 0 || filter_entries.size() == 0) {
    return batt::StatusCode::kNotFound;
  }

  llfs::PageDeviceEntry* found_src_entry = nullptr;
  llfs::PageDeviceEntry* found_filter_entry = nullptr;

  // Search through all src-page-size devices and all filter-page-size devices, looking for a pair
  // that have the same page count.  Panic unless this is the *only* such pair.
  //
  for (llfs::PageDeviceEntry* src_entry : src_entries) {
    const llfs::PageCount src_page_count =
        src_entry->arena.device().page_ids().get_physical_page_count();

    for (llfs::PageDeviceEntry* filter_entry : filter_entries) {
      const llfs::PageCount filter_page_count =
          filter_entry->arena.device().page_ids().get_physical_page_count();

      if (src_page_count == filter_page_count) {
        if (found_src_entry != nullptr || found_filter_entry != nullptr) {
          return batt::StatusCode::kUnavailable;
        }

        found_src_entry = src_entry;
        found_filter_entry = filter_entry;
        //
        // IMPORTANT: we do not exit the loops early because we want to verify that this is the only
        // match!
      }
    }
  }

  if (found_src_entry == nullptr || found_filter_entry == nullptr) {
    return batt::StatusCode::kNotFound;
  }

  BATT_CHECK_EQ(found_src_entry->filter_device_entry, nullptr);
  BATT_CHECK_EQ(found_src_entry->is_filter_device_for, nullptr);
  BATT_CHECK_EQ(found_filter_entry->filter_device_entry, nullptr);
  BATT_CHECK_EQ(found_filter_entry->is_filter_device_for, nullptr);

  found_src_entry->filter_device_entry = found_filter_entry;
  found_src_entry->filter_device_id = found_filter_entry->arena.device().get_id();

  found_filter_entry->is_filter_device_for = found_src_entry;
  found_filter_entry->is_filter_device_for_id = found_src_entry->arena.device().get_id();
  found_filter_entry->can_alloc = false;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<PageId> PageCache::page_shard_id_for(PageId full_page_id,
                                              const Interval<usize>& shard_range)
{
  const usize shard_size = shard_range.size();

  BATT_CHECK_EQ(batt::bit_count(shard_size), 1) << "shard sizes must be powers of 2";
  BATT_CHECK_EQ(shard_range.lower_bound & (shard_size - 1), 0)
      << "shard offsets must be aligned to the shard size";

  PageDeviceEntry* const src_entry = this->get_device_for_page(full_page_id);

  const i32 shard_size_log2 = batt::log2_ceil(shard_size);
  const i32 shards_per_page_log2 = src_entry->page_size_log2 - shard_size_log2;

  BATT_CHECK_GT(src_entry->page_size_log2, shard_size_log2);

  PageDeviceEntry* const dst_entry = src_entry->sharded_views[shard_size_log2];
  if (!dst_entry) {
    return None;
  }

  const u64 shard_index_in_page = shard_range.lower_bound >> shard_size_log2;

  return full_page_id  //
      .set_device_id_shifted(dst_entry->device_id_shifted)
      .set_address((full_page_id.address() << shards_per_page_log2) | shard_index_in_page);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCache::register_page_layout(const PageLayoutId& layout_id, const PageReader& reader)
{
  LLFS_LOG_WARNING() << "PageCache::register_page_layout is DEPRECATED; please use "
                        "PageCache::register_page_reader";

  return this->page_readers_->lock()
      ->emplace(layout_id,
                PageReaderFromFile{
                    .page_reader = reader,
                    .file = __FILE__,
                    .line = __LINE__,
                })
      .second;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::Status PageCache::register_page_reader(const PageLayoutId& layout_id, const char* file,
                                             int line, const PageReader& reader)
{
  auto locked = this->page_readers_->lock();
  const auto [iter, was_inserted] = locked->emplace(layout_id, PageReaderFromFile{
                                                                   .page_reader = reader,
                                                                   .file = file,
                                                                   .line = line,
                                                               });

  if (!was_inserted && (iter->second.file != file || iter->second.line != line)) {
    return ::llfs::make_status(StatusCode::kPageReaderConflict);
  }

  BATT_CHECK_EQ(iter->second.file, file);
  BATT_CHECK_EQ(iter->second.line, line);

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::close()
{
  for (const std::unique_ptr<PageDeviceEntry>& entry : this->page_devices_) {
    if (entry) {
      entry->arena.halt();
    }
  }
  this->cache_slot_pool_->halt();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::join()
{
  for (const std::unique_ptr<PageDeviceEntry>& entry : this->page_devices_) {
    if (entry) {
      entry->arena.join();
    }
  }
  this->cache_slot_pool_->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<PageCacheJob> PageCache::new_job()
{
  return std::make_unique<PageCacheJob>(this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::allocate_page_of_size(PageSize size,
                                                      batt::WaitForResource wait_for_resource,
                                                      LruPriority lru_priority, u64 callers,
                                                      u64 job_id,
                                                      const batt::CancelToken& cancel_token)
{
  const PageSizeLog2 size_log2 = log2_ceil(size);
  BATT_CHECK_EQ(usize{1} << size_log2, size) << "size must be a power of 2";

  return this->allocate_page_of_size_log2(size_log2, wait_for_resource, lru_priority,
                                          callers | Caller::PageCache_allocate_page_of_size, job_id,
                                          std::move(cancel_token));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::allocate_page_of_size_log2(PageSizeLog2 size_log2,
                                                           batt::WaitForResource wait_for_resource,
                                                           LruPriority lru_priority,
                                                           u64 callers [[maybe_unused]],
                                                           u64 job_id [[maybe_unused]],
                                                           const batt::CancelToken& cancel_token)
{
  BATT_CHECK_LT(size_log2, kMaxPageSizeLog2);

  PageSize page_size{u32{1} << size_log2};

  LatencyTimer alloc_timer{this->metrics_.allocate_page_alloc_latency};

  Slice<PageDeviceEntry* const> device_entries = this->devices_with_page_size_log2(size_log2);

  // TODO [tastolfi 2021-09-08] If the caller wants to wait, which device should we wait on?  First
  // available? Random?  Round-Robin?
  //
  for (auto wait_arg : {batt::WaitForResource::kFalse, batt::WaitForResource::kTrue}) {
    for (PageDeviceEntry* device_entry : device_entries) {
      if (!device_entry->can_alloc) {
        continue;
      }
      PageArena& arena = device_entry->arena;
      StatusOr<PageId> page_id = arena.allocator().allocate_page(wait_arg, cancel_token);
      if (!page_id.ok()) {
        if (page_id.status() == batt::StatusCode::kResourceExhausted) {
          const u64 page_size = u64{1} << size_log2;
          LLFS_LOG_INFO_FIRST_N(1)  //
              << "Failed to allocate page (pool is empty): " << BATT_INSPECT(page_size);
        }
        continue;
      }

      BATT_CHECK_EQ(PageIdFactory::get_device_id(*page_id), arena.id());

      LLFS_VLOG(1) << "allocated page " << *page_id;

#if LLFS_TRACK_NEW_PAGE_EVENTS
      this->track_new_page_event(NewPageTracker{
          .ts = 0,
          .job_id = job_id,
          .page_id = *page_id,
          .callers = callers,
          .event_id = (int)NewPageTracker::Event::kAllocate,
      });
#endif  // LLFS_TRACK_NEW_PAGE_EVENTS

      return this->pin_allocated_page_to_cache(device_entry, page_size, *page_id, lru_priority);
    }

    if (wait_for_resource == batt::WaitForResource::kFalse) {
      break;
    }
  }

  LLFS_LOG_WARNING() << "No arena with free space could be found;" << BATT_INSPECT(page_size)
                     << BATT_INSPECT(wait_for_resource);

  return Status{batt::StatusCode::kUnavailable};  // TODO [tastolfi 2021-10-20]
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::allocate_filter_page_for(PageId page_id, LruPriority lru_priority)
{
  Optional<PageId> filter_page_id = this->filter_page_id_for(page_id);
  if (!filter_page_id) {
    return {batt::StatusCode::kUnavailable};
  }

  PageDeviceEntry* filter_device_entry = this->get_device_for_page(*filter_page_id);
  BATT_CHECK_NOT_NULLPTR(filter_device_entry);

  PageDevice* filter_page_device = std::addressof(filter_device_entry->arena.device());

  return this->pin_allocated_page_to_cache(filter_device_entry, filter_page_device->page_size(),
                                           *filter_page_id, lru_priority);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::async_write_filter_page(const PinnedPage& new_filter_page,
                                        PageDevice::WriteHandler&& handler)
{
  PageDeviceEntry* filter_device_entry = this->get_device_for_page(new_filter_page.page_id());

  filter_device_entry->arena.device().write(new_filter_page.get_page_buffer(), std::move(handler));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::pin_allocated_page_to_cache(PageDeviceEntry* device_entry,
                                                            PageSize page_size, PageId page_id,
                                                            LruPriority lru_priority)
{
  PageArena& arena = device_entry->arena;

  StatusOr<std::shared_ptr<PageBuffer>> new_page_buffer = arena.device().prepare(page_id);
  BATT_REQUIRE_OK(new_page_buffer);

  NewPageView* p_new_page_view = nullptr;

  // PageDevice::prepare must be thread-safe.
  //
  StatusOr<PageCacheSlot::PinnedRef> pinned_slot = device_entry->cache.find_or_insert(
      page_id, page_size, lru_priority,
      /*initialize=*/
      [&new_page_buffer, &p_new_page_view](const PageCacheSlot::PinnedRef& pinned_slot) {
        // Create a NewPageView object as a placeholder so we can insert the new page into the
        // cache.
        //
        std::shared_ptr<NewPageView> new_page_view =
            std::make_shared<NewPageView>(std::move(*new_page_buffer));

        // Save a pointer to the NewPageView to create the PinnedPage below.
        //
        p_new_page_view = new_page_view.get();

        // Access the slot's Latch and set it.
        //
        batt::Latch<std::shared_ptr<const PageView>>* latch = pinned_slot.value();
        latch->set_value(std::move(new_page_view));
      });

  BATT_REQUIRE_OK(pinned_slot);
  BATT_CHECK_NOT_NULLPTR(p_new_page_view);

  return PinnedPage{p_new_page_view, std::move(*pinned_slot)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::deallocate_page(PageId page_id, u64 callers, u64 job_id)
{
  LLFS_VLOG(1) << "deallocated page " << std::hex << page_id;

#if LLFS_TRACK_NEW_PAGE_EVENTS
  this->track_new_page_event(NewPageTracker{
      .ts = 0,
      .job_id = job_id,
      .page_id = page_id,
      .callers = callers,
      .event_id = (int)NewPageTracker::Event::kDeallocate,
  });
#endif  // LLFS_TRACK_NEW_PAGE_EVENTS

  this->arena_for_page_id(page_id).allocator().deallocate_page(page_id);
  this->purge(page_id, callers | Caller::PageCache_deallocate_page, job_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCache::attach(const boost::uuids::uuid& user_id, slot_offset_type slot_offset)
{
  bool success = false;
  std::vector<const PageArena*> attached_arenas;

  auto detach_on_failure = batt::finally([&] {
    if (success) {
      return;
    }
    for (const PageArena* p_arena : attached_arenas) {
      auto detach_status = p_arena->allocator().detach_user(user_id, slot_offset);
      if (!detach_status.ok()) {
        LLFS_LOG_ERROR() << "Failed to detach after failed attachement: "
                         << BATT_INSPECT(p_arena->id());
      }
    }
  });

  for (PageDeviceEntry* entry : this->all_devices()) {
    BATT_CHECK_NOT_NULLPTR(entry);
    auto arena_status = entry->arena.allocator().attach_user(user_id, slot_offset);
    BATT_REQUIRE_OK(arena_status);
    attached_arenas.emplace_back(&entry->arena);
  }

  success = true;

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageCache::detach(const boost::uuids::uuid& user_id, slot_offset_type slot_offset)
{
  for (PageDeviceEntry* entry : this->all_devices()) {
    BATT_CHECK_NOT_NULLPTR(entry);
    auto arena_status = entry->arena.allocator().detach_user(user_id, slot_offset);
    BATT_REQUIRE_OK(arena_status);
  }
  //
  // TODO [tastolfi 2021-04-02] - handle partial failure (?)

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::prefetch_hint(PageId page_id)
{
  (void)this->find_page_in_cache(page_id, PageLoadOptions{}  //
                                              .required_layout(None)
                                              .ok_if_not_found(true));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const std::vector<std::unique_ptr<PageDeviceEntry>>& PageCache::devices_by_id() const
{
  return this->page_devices_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<PageDeviceEntry* const> PageCache::all_devices() const
{
  return as_slice(this->page_devices_by_page_size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<PageDeviceEntry* const> PageCache::devices_with_page_size(usize size) const
{
  const usize size_log2 = batt::log2_ceil(size);
  BATT_CHECK_EQ(size, usize{1} << size_log2) << "page size must be a power of 2";

  return this->devices_with_page_size_log2(size_log2);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Slice<PageDeviceEntry* const> PageCache::devices_with_page_size_log2(usize size_log2) const
{
  BATT_CHECK_LT(size_log2, kMaxPageSizeLog2);

  return this->page_devices_by_page_size_log2_[size_log2];
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageArena& PageCache::arena_for_page_id(PageId page_id) const
{
  return this->arena_for_device_id(PageIdFactory::get_device_id(page_id));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const PageArena& PageCache::arena_for_device_id(page_device_id_int device_id_val) const
{
  BATT_CHECK_LT(device_id_val, this->page_devices_.size())
      << "the specified page_id's device is not in the storage pool for this cache";

  BATT_CHECK_NOT_NULLPTR(this->page_devices_[device_id_val]);

  return this->page_devices_[device_id_val]->arena;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::async_write_new_page(PinnedPage&& pinned_page,
                                     PageDevice::WriteHandler&& handler) noexcept
{
  const PageId page_id = pinned_page.page_id();
  const auto device_id_val = PageIdFactory::get_device_id(page_id);

  BATT_CHECK_LT(device_id_val, this->page_devices_.size())
      << "the specified page_id's device is not in the storage pool for this cache";

  BATT_CHECK_NOT_NULLPTR(this->page_devices_[device_id_val]);

  PageDeviceEntry& entry = *this->page_devices_[device_id_val];

  entry.arena.device().write(pinned_page.get_page_buffer(), std::move(handler));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::purge(PageId page_id, u64 callers [[maybe_unused]], u64 job_id [[maybe_unused]])
{
  if (page_id.is_valid()) {
#if LLFS_TRACK_NEW_PAGE_EVENTS
    this->track_new_page_event(NewPageTracker{
        .ts = 0,
        .job_id = job_id,
        .page_id = page_id,
        .callers = callers,
        .event_id = (int)NewPageTracker::Event::kPurge,
    });
#endif  // LLFS_TRACK_NEW_PAGE_EVENTS

    PageDeviceEntry* const entry = this->get_device_for_page(page_id);
    BATT_CHECK_NOT_NULLPTR(entry);

    entry->cache.erase(page_id);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::try_pin_cached_page(PageId page_id,
                                                    const PageLoadOptions& options) /*override*/
{
  if (bool_from(options.pin_page_to_job(), /*default_value=*/false)) {
    return Status{batt::StatusCode::kUnimplemented};
  }

  if (!page_id) {
    return ::llfs::make_status(StatusCode::kPageIdInvalid);
  }

  PageDeviceEntry* const entry = this->get_device_for_page(page_id);
  BATT_CHECK_NOT_NULLPTR(entry);

  BATT_ASSIGN_OK_RESULT(PageCacheSlot::PinnedRef pinned_ref,
                        entry->cache.try_find(page_id, options.lru_priority()));

  BATT_ASSIGN_OK_RESULT(std::shared_ptr<const PageView> loaded, pinned_ref->poll());

  if (options.required_layout() &&
      *options.required_layout() != ShardedPageView::page_layout_id()) {
    BATT_REQUIRE_OK(require_page_layout(loaded->page_buffer(), options.required_layout()));
  }

  return PinnedPage{loaded.get(), std::move(pinned_ref)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PinnedPage> PageCache::load_page(PageId page_id, const PageLoadOptions& options)
{
  if (bool_from(options.pin_page_to_job(), /*default_value=*/false)) {
    return Status{batt::StatusCode::kUnimplemented};
  }

  this->metrics_.get_count.add(1);

  if (!page_id) {
    return ::llfs::make_status(StatusCode::kPageIdInvalid);
  }

  BATT_ASSIGN_OK_RESULT(PageCacheSlot::PinnedRef pinned_slot,  //
                        this->find_page_in_cache(page_id, options));

  StatusOr<std::shared_ptr<const PageView>> loaded = pinned_slot->await();
  if (!loaded.ok()) {
    if (!options.ok_if_not_found()) {
      LLFS_LOG_ERROR() << loaded.status() << std::endl << boost::stacktrace::stacktrace{};
    }
    return loaded.status();
  }

  BATT_CHECK_EQ(loaded->get() != nullptr, bool{pinned_slot});

  return PinnedPage{loaded->get(), std::move(pinned_slot)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PageCache::find_page_in_cache(PageId page_id, const PageLoadOptions& options)
    -> batt::StatusOr<PageCacheSlot::PinnedRef>
{
  if (!page_id) {
    return PageCacheSlot::PinnedRef{};
  }

  PageDeviceEntry* const entry = this->get_device_for_page(page_id);
  BATT_CHECK_NOT_NULLPTR(entry);

  return entry->cache.find_or_insert(
      page_id, entry->page_size(), options.lru_priority(),
      [this, entry, &options](const PageCacheSlot::PinnedRef& pinned_slot) {
        entry->cache.metrics().miss_count.add(1);
        this->async_load_page_into_slot(pinned_slot, options.required_layout(),
                                        options.ok_if_not_found());
      });
}

struct AsyncLoadPageOp : batt::RefCounted<AsyncLoadPageOp> {
  PageCache* page_cache = nullptr;
  Optional<PageLayoutId> required_layout;
  OkIfNotFound ok_if_not_found;
  PageCacheSlot::PinnedRef pinned_slot;
  Optional<std::chrono::steady_clock::time_point> start_time;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::async_load_page_into_slot(const PageCacheSlot::PinnedRef& pinned_slot,
                                          const Optional<PageLayoutId>& required_layout,
                                          OkIfNotFound ok_if_not_found)
{
  const PageId page_id = pinned_slot.key();

  PageDeviceEntry* const entry = this->get_device_for_page(page_id);
  BATT_CHECK_NOT_NULLPTR(entry);

  boost::intrusive_ptr<AsyncLoadPageOp> op{new AsyncLoadPageOp{}};

  op->page_cache = this;
  op->required_layout = required_layout;
  op->ok_if_not_found = ok_if_not_found;
  op->pinned_slot = pinned_slot;
  if (batt::sample_metric_at_rate(batt::Every2ToTheConst<9>{})) {
    op->start_time = std::chrono::steady_clock::now();
  }

  entry->arena.device().read(
      page_id,
      /*read_handler=*/[op = std::move(op)]  //
      (StatusOr<std::shared_ptr<const PageBuffer>>&& result) mutable {
        const PageId page_id = op->pinned_slot.key();
        auto* p_metrics = &op->page_cache->metrics_;
        auto page_readers = op->page_cache->page_readers_;

        BATT_DEBUG_INFO("PageCache::find_page_in_cache - read handler");

        auto cleanup = batt::finally([&] {
          op.reset();
        });

        batt::Latch<std::shared_ptr<const PageView>>* latch = op->pinned_slot.value();
        BATT_CHECK_NOT_NULLPTR(latch);

        if (!result.ok()) {
          if (!op->ok_if_not_found) {
#if LLFS_TRACK_NEW_PAGE_EVENTS
            LLFS_LOG_WARNING() << "recent events for" << BATT_INSPECT(page_id)
                               << BATT_INSPECT(ok_if_not_found) << " (now=" << this->history_end_
                               << "):"
                               << batt::dump_range(
                                      this->find_new_page_events(page_id) | seq::collect_vec(),
                                      batt::Pretty::True);
#endif  // LLFS_TRACK_NEW_PAGE_EVENTS
          }
          latch->set_value(result.status());
          return;
        }

        // Page read succeeded!  Find the right typed reader.
        //
        std::shared_ptr<const PageBuffer>& page_data = *result;
        const PageSize read_page_size = get_page_size(page_data);
        StatusOr<std::shared_ptr<const PageView>> page_view;

        if (op->start_time) {
          usize page_size_log2 = batt::log2_ceil(read_page_size);
          p_metrics->page_read_latency[page_size_log2].update(*op->start_time);
        }
        p_metrics->total_bytes_read.add(read_page_size);

        if (op->required_layout && *op->required_layout == ShardedPageView::page_layout_id()) {
          page_view = std::make_shared<ShardedPageView>(std::move(page_data));

        } else {
          Status layout_status = require_page_layout(*page_data, op->required_layout);
          if (!layout_status.ok()) {
            latch->set_value(layout_status);
            return;
          }
          PageLayoutId layout_id = get_page_header(*page_data).layout_id;

          PageReader reader_for_layout;
          {
            auto locked = page_readers->lock();
            auto iter = locked->find(layout_id);
            if (iter == locked->end()) {
              LLFS_LOG_ERROR() << "Unknown page layout: "
                               << batt::c_str_literal(
                                      std::string_view{(const char*)&layout_id, sizeof(layout_id)})
                               << BATT_INSPECT(page_id);
              latch->set_value(make_status(StatusCode::kNoReaderForPageViewType));
              return;
            }
            reader_for_layout = iter->second.page_reader;
          }
          // ^^ Release the page_readers mutex ASAP

          page_view = reader_for_layout(std::move(page_data));
        }

        if (page_view.ok()) {
          BATT_CHECK_EQ(page_view->use_count(), 1u);
        }
        latch->set_value(std::move(page_view));
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCache::page_might_contain_key(PageId /*page_id*/, const KeyView& /*key*/) const
{
  return true;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
#if LLFS_TRACK_NEW_PAGE_EVENTS

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCache::track_new_page_event(const NewPageTracker& tracker)
{
  const isize i = this->history_end_.fetch_add(1);
  BATT_CHECK_GE(i, 0);
  auto& slot = this->history_[i % this->history_.size()];
  slot = tracker;
  slot.ts = i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<NewPageTracker> PageCache::find_new_page_events(PageId page_id) const
{
  const isize n = this->history_end_.load();
  return batt::as_seq(boost::irange(isize{0}, isize(this->history_.size())))  //
         | seq::map([this, n](isize i) {
             isize j = n - i;
             if (j < 0) {
               j += this->history_.size();
               BATT_CHECK_GE(j, 0);
             }
             return this->history_[j % this->history_.size()];
           })  //
         | seq::filter(
               [n, page_id, page_id_factory = this->arena_for_page_id(page_id).device().page_ids()](
                   const NewPageTracker& t) {
                 return t.ts < n &&
                        page_id_factory.get_device_id(t.page_id) ==
                            page_id_factory.get_device_id(page_id) &&
                        page_id_factory.get_physical_page(t.page_id) ==
                            page_id_factory.get_physical_page(page_id);
               })  //
         | seq::boxed();
}

#endif  // LLFS_TRACK_NEW_PAGE_EVENTS

}  // namespace llfs
