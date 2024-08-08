//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_DEFINE_PACKED_TYPE_HPP
#define LLFS_DEFINE_PACKED_TYPE_HPP

#include <batteries/status.hpp>
#include <batteries/type_traits.hpp>
#include <batteries/utility.hpp>

#include <boost/preprocessor/cat.hpp>

#include <type_traits>

namespace llfs {

template <typename T>
struct Use_macro_LLFS_DEFINE_PACKED_TYPE_FOR_to_define_the_packed_representation_of_type {
};

template <typename T>
struct DefinePackedTypeFor {
  using type =
      llfs::Use_macro_LLFS_DEFINE_PACKED_TYPE_FOR_to_define_the_packed_representation_of_type<T>;
};

template <typename T>
using PackedTypeFor =
    typename decltype(llfs_packed_type_for(batt::StaticType<std::decay_t<T>>{}))::type;

class DataReader;

template <typename T>
using UnpackedTypeFor = batt::RemoveStatusOr<decltype(unpack_object(std::declval<const T&>(),
                                                                    std::declval<DataReader*>()))>;

template <typename T>
inline constexpr bool is_self_contained_packed_type(batt::StaticType<T> static_type = {})
{
  return llfs_is_self_contained_packed_type(static_type);
}

}  // namespace llfs

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

#define LLFS_DEFINE_PACKED_TYPE_FOR(type, packed_type)                                             \
  [[maybe_unused]] inline ::batt::StaticType<packed_type> llfs_packed_type_for(                    \
      ::batt::StaticType<type>)                                                                    \
  {                                                                                                \
    return {};                                                                                     \
  }                                                                                                \
  [[maybe_unused]] static inline constexpr int BOOST_PP_CAT(                                       \
      Suppress_Warning_About_Extra_Semicolon_After_LLFS_DEFINE_PACKED_TYPE_FOR_,                   \
      BOOST_PP_CAT(__COUNTER__, BOOST_PP_CAT(_, __LINE__))) = 0

#define LLFS_IS_SELF_CONTAINED_PACKED_TYPE(type, value)                                            \
  [[maybe_unused]] inline constexpr bool llfs_is_self_contained_packed_type(                       \
      ::batt::StaticType<type>)                                                                    \
  {                                                                                                \
    return (value);                                                                                \
  }

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

namespace batt {

template <typename T>
inline StaticType<typename ::llfs::DefinePackedTypeFor<std::decay_t<T>>::type> llfs_packed_type_for(
    StaticType<T>)
{
  return {};
}

inline StaticType<void> llfs_packed_type_for(StaticType<void>)
{
  return {};
}

template <typename T>
inline constexpr bool llfs_is_self_contained_packed_type(StaticType<T> = {})
{
  return false;
}

}  // namespace batt

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

#endif  // LLFS_DEFINE_PACKED_TYPE_HPP
