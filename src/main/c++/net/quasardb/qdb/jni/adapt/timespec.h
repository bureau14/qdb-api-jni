#pragma once

#include "../env.h"
#include "../guard/local_ref.h"
#include "../object.h"
#include "../object_array.h"
#include "../primitive_array.h"
#include "../util/unzip_view.hpp"
#include <qdb/ts.h>
#include <cassert>
#include <jni.h>

namespace qdb::jni::adapt::timespec
{

inline constexpr qdb_timespec_t _make_timespec_t(jlong sec, jlong nsec)
{
    static_assert(sizeof(qdb_time_t) >= sizeof(jlong));
    static_assert(0 >= qdb_min_time);

    // Sanity check: we don't support timestamps < 0, which would be before
    // epoch. If we have one, it implies some conversion went wrong.
    assert(sec >= 0 || sec == qdb_min_time || sec == qdb_max_time);
    assert(nsec >= 0 || nsec == qdb_min_time || nsec == qdb_max_time);

    return qdb_timespec_t{sec, nsec};
}

inline qdb_timespec_t to_qdb(qdb::jni::env & env, jobject input)
{
    jclass object_class = jni::object::get_class(env, input);
    jfieldID sec_field  = jni::introspect::lookup_field(env, object_class, "sec", "J");
    jfieldID nsec_field = jni::introspect::lookup_field(env, object_class, "nsec", "J");

    return _make_timespec_t(env.instance().GetLongField(input, sec_field),
        env.instance().GetLongField(input, nsec_field));
}

inline jni::guard::local_ref<jobject> to_java(qdb::jni::env & env, qdb_timespec_t const & input)
{
    return jni::object::create(
        env, "net/quasardb/qdb/ts/Timespec", "(JJ)V", input.tv_sec, input.tv_nsec);
}

}; // namespace qdb::jni::adapt::timespec

namespace qdb::jni::adapt::timespecs
{

inline std::vector<qdb_timespec_t> to_qdb(qdb::jni::env & env, jobject input)
{
    jni::guard::primitive_array<qdb_int_t> secs =
        jni::primitive_array::from_field<qdb_int_t>(env, input, "sec", "[J");

    jni::guard::primitive_array<qdb_int_t> nsecs =
        jni::primitive_array::from_field<qdb_int_t>(env, input, "nsec", "[J");

    assert(secs.size() == nsecs.size());

    auto zipped = ranges::zip_view(secs.to_range(), nsecs.to_range());

    auto callback = [](auto iter) -> qdb_timespec_t {
        return timespec::_make_timespec_t(std::get<0>(iter), std::get<1>(iter));
    };

    std::vector<qdb_timespec_t> ret{secs.size()};
    std::transform(zipped.begin(), zipped.end(), ret.begin(), callback);
    return ret;
}

template <ranges::input_range R>
inline jni::guard::local_ref<jobject> to_java(qdb::jni::env & env, R const & input)
{
    static_assert(std::is_same<ranges::range_value_t<R>, qdb_timespec_t>::value);

    // We have a range of timespecs, which are structs of tv_sec / tv_nsec. Let's split
    // that into two ranges of tv_sec / tv_nsec.

    auto xform = [](qdb_timespec_t const & x) {
        return std::make_pair(x.tv_sec, x.tv_nsec);
    };

    auto range_of_pairs         = input | ranges::views::transform(xform);
    auto const && [secs, nsecs] = jni::util::make_unzip_views(range_of_pairs);

    auto secs_  = jni::primitive_array::from_range<qdb_int_t>(env, secs);
    auto nsecs_ = jni::primitive_array::from_range<qdb_int_t>(env, nsecs);

    return jni::object::create(
        env, "net/quasardb/qdb/ts/Timespecs", "([J[J)V", secs_.release(), nsecs_.release());
}

}; // namespace qdb::jni::adapt::timespecs
