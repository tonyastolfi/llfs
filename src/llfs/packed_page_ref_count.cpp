//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/packed_page_ref_count.hpp>
//

#include <llfs/slot_writer.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedPageRefCount& t)
{
  out << "PackedPageRefCount{.page_id=" << t.page_id << ", .ref_count=";

  if (t.ref_count == kRefCount_1_to_0) {
    out << "kRefCount_1_to_0";
  } else {
    out << std::setw(0) << std::dec << t.ref_count.value();
  }

  return out << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof_page_ref_count_slot() noexcept
{
  constexpr usize kSize = packed_sizeof_slot_with_payload_size(sizeof(PackedPageRefCount));
  return kSize;
}

}  // namespace llfs
