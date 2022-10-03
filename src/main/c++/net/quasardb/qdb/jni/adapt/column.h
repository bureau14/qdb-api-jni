#pragma once

#include "../env.h"
#include "../object.h"
#include "../object_array.h"
#include "../primitives.h"
#include "../string.h"
#include <qdb/ts.h>
#include <cassert>
#include <iostream>
#include <jni.h>
#include <vector>

namespace qdb::jni::adapt::column
{

template <typename T = qdb_ts_column_info_ex_t>
jni::guard::local_ref<jobject> to_java(jni::env & env, T const & x);

template <typename T>
T to_qdb(qdb::jni::env & env, qdb_handle_t handle, jobject input);

inline qdb_ts_column_type_t _column_type_from_type_enum(qdb::jni::env & env, jobject input)
{
    jclass object_class  = jni::object::get_class(env, input);
    jfieldID value_field = jni::introspect::lookup_field(env, object_class, "value", "I");

    return jni::primitives::get_int_as<qdb_ts_column_type_t>(env, input, value_field);
}
}; // namespace qdb::jni::adapt::column

namespace qdb::jni::adapt::columns
{

template <typename T, ranges::input_range R>
inline std::vector<T> to_qdb(jni::env & env, qdb_handle_t handle, R const & xs)
{
    auto view = ranges::views::transform(xs, [&env, &handle](jobject x) -> T {
        return column::to_qdb<T>(env, handle, x);
    });
    return std::vector<T>{view.begin(), view.end()};
}

template <typename T, ranges::input_range R>
jni::object_array to_java(jni::env & env, R const & xs)
{
    auto callback = [&env](T x) -> jobject {
        return column::to_java(env, x).release();
    };

    return jni::make_object_array(
        env, "net/quasardb/qdb/ts/Column", ranges::views::transform(xs, callback));
}

template <typename T>
inline jni::object_array to_java(jni::env & env, T * xs, std::size_t n)
{
    return to_java<T>(env, ranges::views::counted(xs, n));
}

}; // namespace qdb::jni::adapt::columns
