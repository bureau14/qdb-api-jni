#pragma once

#include "env.h"
#include "introspect.h"
#include "object.h"
#include <qdb/client.h> // for qdb_string_t
#include "adapt/value_traits.h"
#include "guard/local_ref.h"
#include "guard/primitive_array.h"
#include <range/v3/range.hpp>
#include <iostream>
#include <jni.h>

namespace qdb::jni::primitive_array::detail
{
template <typename From>
struct util
{
    template <ranges::range R>
    static jni::guard::local_ref<typename adapt::value_traits<From>::jarray_type> from_range(
        qdb::jni::env & env, R const & input);
};

template <>
struct util<qdb_int_t>
{
    template <ranges::range R>
    static jni::guard::local_ref<jlongArray> from_range(qdb::jni::env & env, R const & input)
    {
        // NOTE(leon): this is a hot code path, what is the performance impact
        //             of using a range as input?
        // TODO(leon): ensure R is a contiguous_range?
        auto ret = env.instance().NewLongArray(ranges::size(input));

        jlong * xs = env.instance().GetLongArrayElements(ret, 0);
        ranges::copy(input, xs);
        env.instance().ReleaseLongArrayElements(ret, xs, 0);
        return jni::guard::local_ref{env, ret};
    }
};

template <>
struct util<double>
{
    template <ranges::range R>
    static jni::guard::local_ref<jdoubleArray> from_range(qdb::jni::env & env, R const & input)
    {
        // NOTE(leon): this is a hot code path, what is the performance impact
        //             of using a range as input?
        // TODO(leon): ensure R is a contiguous_range?
        auto ret     = env.instance().NewDoubleArray(ranges::size(input));
        jdouble * xs = env.instance().GetDoubleArrayElements(ret, 0);
        ranges::copy(input, xs);
        env.instance().ReleaseDoubleArrayElements(ret, xs, 0);
        return jni::guard::local_ref{env, ret};
    }
};

}; // namespace qdb::jni::primitive_array::detail

namespace qdb::jni
{

template <typename T>
inline jni::guard::primitive_array<T> make_primitive_array(qdb::jni::env & env, jarray arr)
{
    jint len = env.instance().GetArrayLength(arr);
    T * xs   = static_cast<T *>(env.instance().GetPrimitiveArrayCritical(arr, 0));
    assert(xs != NULL);
    return guard::primitive_array<T>(env, arr, xs, len);
}

}; // namespace qdb::jni

namespace qdb::jni::primitive_array
{

template <typename T>
inline jni::guard::primitive_array<T> from_field(
    qdb::jni::env & env, jobject object, char const * alias, char const * signature)
{
    jni::guard::local_ref<jarray> xs =
        jni::object::from_field<jarray>(env, object, alias, signature);
    return make_primitive_array<T>(env, xs.release());
}

template <typename From, ranges::range R>
jni::guard::local_ref<typename adapt::value_traits<From>::jarray_type> from_range(
    qdb::jni::env & env, R const & input)
{
    return detail::util<From>::template from_range<R>(env, input);
}

template <typename T, ranges::range R>
inline R to_range(jni::guard::primitive_array<T> const & xs)
{
    return xs.to_range();
}

}; // namespace qdb::jni::primitive_array
