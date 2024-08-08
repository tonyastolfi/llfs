//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_PAGE_REF_COUNT_HPP
#define LLFS_PACKED_PAGE_REF_COUNT_HPP

#include <llfs/config.hpp>
//
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/page_ref_count.hpp>
#include <llfs/unpack_cast.hpp>

namespace llfs {

struct PackedPageRefCount {
  PackedPageId page_id;
  little_i32 ref_count;

  PageRefCount as_page_ref_count() const noexcept
  {
    return PageRefCount{
        .page_id = this->page_id.unpack(),
        .ref_count = this->ref_count,
    };
  }

  PageRefCount unpack() const noexcept
  {
    return this->as_page_ref_count();
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedPageRefCount), 12);

LLFS_DEFINE_PACKED_TYPE_FOR(PageRefCount, PackedPageRefCount);
LLFS_DEFINE_PACKED_TYPE_FOR(PackedPageRefCount, PackedPageRefCount);

LLFS_IS_SELF_CONTAINED_PACKED_TYPE(PackedPageRefCount, true)

static_assert(is_self_contained_packed_type<PackedPageRefCount>());

std::ostream& operator<<(std::ostream& out, const PackedPageRefCount& t);

inline usize packed_sizeof(const PackedPageRefCount&)
{
  return packed_sizeof(batt::StaticType<PackedPageRefCount>{});
}

template <typename Dst>
[[nodiscard]] bool pack_object_to(const PageRefCount& prc, PackedPageRefCount* packed, Dst*)
{
  packed->page_id = PackedPageId::from(prc.page_id);
  packed->ref_count = prc.ref_count;
  return true;
}

template <typename Dst>
[[nodiscard]] bool pack_object_to(const PackedPageRefCount& from, PackedPageRefCount* to, Dst*)
{
  *to = from;
  return true;
}

inline Status validate_packed_value(const PackedPageRefCount& pprc, const void* buffer_data,
                                    usize buffer_size)
{
  return validate_packed_struct(pprc, buffer_data, buffer_size);
}

/** \brief Returns the size (in bytes) of a packed page ref count slot (assuming PackedPageRefCount
 * is a member of the top-level event variant for the target log).
 */
usize packed_sizeof_page_ref_count_slot() noexcept;

}  // namespace llfs

#endif  // LLFS_PACKED_PAGE_REF_COUNT_HPP
