#pragma once

#include "../byte_buffer.h"
#include "../util/unzip_view.hpp"
#include "timespec.h"
#include "value.h"
#include "value_traits.h"
#include <qdb/query.h>
#include "detail/series.h"
#include <cassert>
#include <functional>
#include <iostream>
#include <jni.h>

namespace qdb::jni::adapt::series
{

template <ranges::input_range R>
inline decltype(auto) as_range(R const & input)
{
    return input;
}

template <typename T>
inline decltype(auto) as_range(T const & input)
{
    return input.to_range();
}

/**
 * Point-based array conversion. Responsible for:
 *
 *  - taking low-level JNI value types for the timestamps and the points
 *
 *  - converting the points from the JNI type to our own adapter types
 *    (e.g. jobjectArray -> jni::object_array)
 *
 *  - interleaving the timestamps and points together in a single array
 *    (e.g. taking two vectors of qdb_timespec_t and qdb_int_t and converting
 *     them into a single qdb_ts_int64_point vector).
 *
 * No modifications _should_ be necessary to implement additional types. To
 * implement additional types, detail::xform_series and _xform_input should be
 * used.
 *
 * The `template <typename From>` is used to look up the value traits through
 * value_traits<From>. `From` is used as a tag here, we do not use `From`
 * directly, but rather `value_traits<From>::point_type`, for example.
 */
template <typename From>
inline void to_qdb(qdb::jni::env & env,
    std::vector<qdb_timespec_t> const & timestamps,
    typename value_traits<From>::jarray_type values,
    typename std::vector<typename value_traits<From>::point_type>::iterator dst)
{
    using point_type = typename value_traits<From>::point_type;

    // convert the basic JNI type  to an adapter class of ours.
    //
    // for example:
    //
    //  * jobjectArray -> jni::object_array
    //  * jlongArray   -> jni::guard::array_critical
    //  * jobject      -> std::vector<qdb_timespec_t>
    //
    // Most importantly, it provides us a way to iterate these values.
    //
    auto values_ = detail::xform_input<From>(env, values);
    assert(timestamps.size() == values_.size());

    auto zipped = ranges::zip_view(timestamps, as_range(values_));

    auto callback = [&env](auto iter) -> point_type {
        point_type ret{std::get<0>(iter)};
        detail::xform_series<From>(env, std::get<1>(iter), ret);
        return ret;
    };

    // Now that everything is put into place, transform everything.
    std::transform(zipped.begin(), zipped.end(), dst, callback);
}

template <typename From>
inline std::vector<typename value_traits<From>::point_type> to_qdb(qdb::jni::env & env,
    std::vector<qdb_timespec_t> const & timestamps,
    typename value_traits<From>::jarray_type values)
{
    using point_type = typename value_traits<From>::point_type;
    std::vector<point_type> out{timestamps.size()};

    to_qdb<From>(env, timestamps, values, std::begin(out));

    return out;
}

template <typename From, ranges::input_range R>
requires(std::is_same<ranges::range_value_t<R>, typename value_traits<From>::point_type>::value)
    jni::guard::local_ref<jobject> to_java(qdb::jni::env & env, R const & input)
{
    using point_type  = typename value_traits<From>::point_type;
    using value_type  = typename value_traits<From>::value_type;
    using jarray_type = typename value_traits<From>::jarray_type;

    // QuasarDB C API provides the points as an array of structs. Our Java representation
    // is a struct with two arrays (timespecs + point_type).
    //
    // We'll first unzip these data structures into two separate arrays.
    auto xform = [](point_type const & x) -> std::tuple<qdb_timespec_t, value_type> {
        return std::make_pair(x.timestamp, detail::get_value<From>(x));
    };

    auto range_of_pairs = input | ranges::views::transform(xform);

    auto const && [timestamps, values] = jni::util::make_unzip_views(range_of_pairs);

    jni::guard::local_ref<jobject> timestamps_ = adapt::timespecs::to_java(env, timestamps);
    jni::guard::local_ref<jarray_type> values_ = detail::xform_output<From>(env, values);

    return detail::create<From>(env, std::move(timestamps_), std::move(values_));
}

}; // namespace qdb::jni::adapt::series
