#pragma once

#include "../byte_buffer.h"
#include "../guard/local_ref.h"
#include "../object.h"
#include "../string.h"
#include "timespec.h"
#include "value_traits.h"
#include <qdb/query.h>
#include <jni.h>

namespace qdb::jni::adapt::value::detail
{

template <typename ValueType>
inline qdb::jni::guard::local_ref<jobject> to_java_impl(qdb::jni::env & env, ValueType value);

template <>
inline qdb::jni::guard::local_ref<jobject> to_java_impl<qdb_int_t>(
    qdb::jni::env & env, qdb_int_t value)
{
    return jni::object::call_static_method(
        env, "net/quasardb/qdb/ts/Value", "createInt64", "(J)Lnet/quasardb/qdb/ts/Value;", value);
}

template <>
inline qdb::jni::guard::local_ref<jobject> to_java_impl<double>(qdb::jni::env & env, double value)
{
    return jni::object::call_static_method(
        env, "net/quasardb/qdb/ts/Value", "createDouble", "(D)Lnet/quasardb/qdb/ts/Value;", value);
}

template <>
inline qdb::jni::guard::local_ref<jobject> to_java_impl<qdb_timespec_t>(
    qdb::jni::env & env, qdb_timespec_t value)
{
    return jni::object::call_static_method(env, "net/quasardb/qdb/ts/Value", "createTimestamp",
        "(Lnet/quasardb/qdb/ts/Timespec;)Lnet/quasardb/qdb/ts/Value;",
        jni::adapt::timespec::to_java(env, value).release());
}

template <>
inline qdb::jni::guard::local_ref<jobject> to_java_impl<qdb_string_t>(
    qdb::jni::env & env, qdb_string_t value)
{
    return jni::object::call_static_method(env, "net/quasardb/qdb/ts/Value", "createString",
        "(Ljava/lang/String;)Lnet/quasardb/qdb/ts/Value;",
        jni::string::create_utf8(env, value).release());
}

template <>
inline qdb::jni::guard::local_ref<jobject> to_java_impl<qdb_blob_t>(
    qdb::jni::env & env, qdb_blob_t value)
{
    return jni::object::call_static_method(env, "net/quasardb/qdb/ts/Value", "createSafeBlob",
        "(Ljava/nio/ByteBuffer;)Lnet/quasardb/qdb/ts/Value;",
        jni::byte_buffer::create_copy(env, value).release());
}
} // namespace qdb::jni::adapt::value::detail

namespace qdb::jni::adapt::value
{

inline jni::guard::local_ref<jobject> _create_null(qdb::jni::env & env)
{
    return jni::object::call_static_method(
        env, "net/quasardb/qdb/ts/Value", "createNull", "()Lnet/quasardb/qdb/ts/Value;");
}

template <typename ValueType>
inline qdb::jni::guard::local_ref<jobject> to_java(qdb::jni::env & env, ValueType value)
{
    if (value_traits<ValueType>::is_null(value))
    {
        return _create_null(env);
    }

    return detail::to_java_impl(env, value);
}

jni::guard::local_ref<jobject> from_native(qdb::jni::env & env, qdb_point_result_t const & input);

jni::guard::local_ref<jobject> _from_native_int64(
    qdb::jni::env & env, qdb_point_result_t const & input);

jni::guard::local_ref<jobject> _from_native_count(
    qdb::jni::env & env, qdb_point_result_t const & input);

jni::guard::local_ref<jobject> _from_native_double(
    qdb::jni::env & env, qdb_point_result_t const & input);

jni::guard::local_ref<jobject> _from_native_timestamp(
    qdb::jni::env & env, qdb_point_result_t const & input);

jni::guard::local_ref<jobject> _from_native_blob(
    qdb::jni::env & env, qdb_point_result_t const & input);

jni::guard::local_ref<jobject> _from_native_string(
    qdb::jni::env & env, qdb_point_result_t const & input);

jni::guard::local_ref<jobject> _from_native_null(qdb::jni::env & env);
}; // namespace qdb::jni::adapt::value

namespace qdb::jni::adapt::value::type
{

inline jni::guard::local_ref<jobject> to_java(qdb::jni::env & env, qdb_ts_column_type_t type)
{
    static_assert(sizeof(type) <= sizeof(jint));
    return jni::object::call_static_method(env, "net/quasardb/qdb/ts/Value$Type", "fromInt",
        "(I)Lnet/quasardb/qdb/ts/Value$Type;", static_cast<jint>(type));
}

}; // namespace qdb::jni::adapt::value::type
