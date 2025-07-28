//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_PAGE_FILE_DEVICE_HPP
#define LLFS_IORING_PAGE_FILE_DEVICE_HPP

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/file_offset_ptr.hpp>
#include <llfs/ioring.hpp>
#include <llfs/page_device.hpp>
#include <llfs/page_device_config.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class IoRingPageFileDevice : public PageDevice
{
 public:
  struct PhysicalLayout {
    using Self = PhysicalLayout;

    static Self from_packed_config(const FileOffsetPtr<PackedPageDeviceConfig>& config);

    //+++++++++++-+-+--+----- --- -- -  -  -   -

    PageSize page_size;
    PageCount page_count;
    FileOffset page_0_offset;
    u16 page_size_log2;
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit IoRingPageFileDevice(IoRing::File&& file,
                                const FileOffsetPtr<PackedPageDeviceConfig>& config) noexcept;

  /** \brief Creates a read-only view of the page device; the constructed object does *not* own the
   * passed file, so the caller must make sure the file lives at least as long as `this`.
   */
  explicit IoRingPageFileDevice(page_device_id_int device_id, IoRing::File* file,
                                const PhysicalLayout& physical_layout) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRing::File& file()
  {
    return this->file_;
  }

  const PhysicalLayout& layout() const
  {
    return this->physical_layout_;
  }

  std::unique_ptr<PageDevice> make_sharded_view(page_device_id_int device_id,
                                                PageSize shard_size) override;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  IoRingPageFileDevice* as_io_ring_page_file_device() override
  {
    return this;
  }

  PageIdFactory page_ids() override;

  PageSize page_size() override;

  StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId page_id) override;

  void write(std::shared_ptr<const PageBuffer>&& page_buffer, WriteHandler&& handler) override;

  void read(PageId id, ReadHandler&& handler) override;

  void drop(PageId id, WriteHandler&& handler) override;

 private:
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  StatusOr<u64> get_physical_page(PageId page_id) const;

  StatusOr<i64> get_file_offset_of_page(PageId page_id) const;

  void write_some(i64 page_offset_in_file, std::shared_ptr<const PageBuffer>&& page_buffer,
                  ConstBuffer remaining_data, WriteHandler&& handler);

  void read_some(PageId page_id, i64 page_offset_in_file, std::shared_ptr<PageBuffer>&& page_buffer,
                 usize page_buffer_size, usize n_read_so_far, ReadHandler&& handler);

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const bool is_read_only_;
  const bool is_sharded_view_;

  // The backing file for this page device.  Could be a flat file or a raw block device.
  //
  Optional<IoRing::File> owned_file_;

  // Access to the file is done via this reference.
  //
  IoRing::File& file_;

  // The layout for this device.
  //
  PhysicalLayout physical_layout_;

  // Used to construct and parse PageIds for this device.
  //
  PageIdFactory page_ids_;
};

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
#endif  // LLFS_IORING_PAGE_FILE_DEVICE_HPP
