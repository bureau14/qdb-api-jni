#pragma once

#include "../../byte_buffer.h"
#include "../../debug.h"
#include "../../primitive_array.h"
#include "../../signature.h"
#include "../timespec.h"
#include "../value_traits.h"
#include <jni.h>
#include <sstream>

namespace qdb::jni::adapt::series::detail
{

template <typename From>
using point_type = typename value_traits<From>::point_type;

template <typename From>
using jarray_type = typename value_traits<From>::jarray_type;

/**
 * xform_series is responsible for transforming the value-part of a ts_point.
 * Default xform_series implementation works for simple values.
 *
 * At moment of writing works for:
 *
 *  - qdb_int_t
 *  - double
 *  - qdb_timespec_t
 */
template <typename From, typename T>
inline void xform_series(qdb::jni::env & /* env */, T v, point_type<From> & out)
{
    out.value = v;
}

template <>
inline void xform_series<qdb_blob_t>(qdb::jni::env & env, jobject v, qdb_ts_blob_point & out)
{
    qdb_blob_t tmp = jni::byte_buffer::to_qdb(env, v);

    out.content        = tmp.content;
    out.content_length = tmp.content_length;
}

template <>
inline void xform_series<qdb_string_t>(qdb::jni::env & env, jobject v, qdb_ts_string_point & out)
{
    jni::string::get_chars_utf8(env, v).as_qdb(out);
}

template <typename From>
inline decltype(auto) get_value(point_type<From> const & input)
{
    return input.value;
}

template <>
inline decltype(auto) get_value<qdb_string_t>(point_type<qdb_string_t> const & input)
{
    // Not copied!
    return qdb_string_t{input.content, input.content_length};
}

template <>
inline decltype(auto) get_value<qdb_blob_t>(point_type<qdb_blob_t> const & input)
{
    return qdb_blob_t{input.content, input.content_length};
}

/**
 * xform_input is responsible for converting/casting/wrapping the input JNI
 * object into something that's c++-iterable.
 */
template <typename From>
inline decltype(auto) xform_input(
    qdb::jni::env & env, typename value_traits<From>::jarray_type input)
{
    return jni::make_primitive_array<From>(env, input);
}

template <>
inline decltype(auto) xform_input<qdb_blob_t>(qdb::jni::env & env, jobjectArray input)
{
    return jni::object_array{env, input};
}

template <>
inline decltype(auto) xform_input<qdb_string_t>(qdb::jni::env & env, jobjectArray input)
{
    return jni::object_array{env, input};
}

template <>
inline decltype(auto) xform_input<qdb_timespec_t>(qdb::jni::env & env, jobject input)
{
    return timespecs::to_qdb(env, input);
}

template <typename From>
struct xform_util
{
    static constexpr char const * data_class =
        jni::signature::points_subtype_of<jarray_type<From>>();

    template <ranges::input_range R>
    static inline jni::guard::local_ref<jarray_type<From>> output(
        qdb::jni::env & env, R const & input)
    {
        return jni::primitive_array::from_range<From>(env, input);
    };

    static inline std::string data_constructor()
    {
        std::stringstream ss;
        ss << "("
           << "Lnet/quasardb/qdb/ts/Timespecs"
           << ";" << jni::signature::of_type<jarray_type<From>>() << ")V";
        return ss.str();
    }
};

template <>
struct xform_util<qdb_timespec_t>
{
    static constexpr char const * data_class = "net/quasardb/qdb/ts/Series$TimestampData";

    static inline std::string data_constructor()
    {
        return "(Lnet/quasardb/qdb/ts/Timespecs;Lnet/quasardb/qdb/ts/Timespecs;)V";
    }

    template <ranges::input_range R>
    static inline jni::guard::local_ref<jobject> output(qdb::jni::env & env, R const & input)
    {
        return adapt::timespecs::to_java(env, input);
    };
};

template <>
struct xform_util<qdb_string_t>
{
    static constexpr char const * data_class = "net/quasardb/qdb/ts/Series$StringData";

    static inline std::string data_constructor()
    {
        return "(Lnet/quasardb/qdb/ts/Timespecs;[Ljava/lang/String;)V";
    }

    template <ranges::input_range R>
    static inline jni::guard::local_ref<jobjectArray> output(qdb::jni::env & env, R const & input)
    {
        static_assert(std::is_same<ranges::range_value_t<R>, qdb_string_t>::value);

        // Convert all qdb_string_t's to jstrings
        auto xform = [&env](qdb_string_t const & x) {
            return jni::string::create_utf8(env, x).release();
        };

        auto input_ = input | ranges::views::transform(xform);

        // Then create an object array out of it
        return jni::guard::local_ref{
            env, jni::make_object_array(env, "java/lang/String", input_).release()};
    };
};

template <>
struct xform_util<qdb_blob_t>
{
    static constexpr char const * data_class = "net/quasardb/qdb/ts/Series$BlobData";

    static inline std::string data_constructor()
    {
        return "(Lnet/quasardb/qdb/ts/Timespecs;[Ljava/nio/ByteBuffer;)V";
    }

    template <ranges::input_range R>
    static inline jni::guard::local_ref<jobjectArray> output(qdb::jni::env & env, R const & input)
    {
        static_assert(std::is_same<ranges::range_value_t<R>, qdb_blob_t>::value);

        // Convert all qdb_blob_t's to bytebuffers (jobjects)
        auto xform = [&env](qdb_blob_t const & x) {
            return jni::byte_buffer::create_copy(env, x).release();
        };

        auto input_ = input | ranges::views::transform(xform);

        // Then create an object array out of it
        return jni::guard::local_ref{
            env, jni::make_object_array(env, "java/nio/ByteBuffer", input_).release()};
    };
};

template <typename From, ranges::input_range R>
inline decltype(auto) xform_output(qdb::jni::env & env, R const & input)
{
    return xform_util<From>::template output(env, input);
}

template <typename From>
inline jni::guard::local_ref<jobject> create(qdb::jni::env & env,
    jni::guard::local_ref<jobject> && timestamps,
    jni::guard::local_ref<jarray_type<From>> && values)
{
    return jni::object::create(env, xform_util<From>::data_class,
        xform_util<From>::data_constructor().c_str(), timestamps.release(), values.release());
}

}; // namespace qdb::jni::adapt::series::detail
