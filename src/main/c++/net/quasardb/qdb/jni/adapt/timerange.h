#pragma once

#include "../env.h"
#include "../object.h"
#include "../object_array.h"
#include "timespec.h"
#include <qdb/ts.h>
#include <range/v3/range.hpp>
#include <jni.h>

namespace qdb::jni::adapt::timerange
{

inline qdb_ts_range_t to_qdb(qdb::jni::env & env, jobject input)
{
    jclass object_class = jni::object::get_class(env, input);
    jfieldID begin_field =
        jni::introspect::lookup_field(env, object_class, "begin", "Lnet/quasardb/qdb/ts/Timespec;");
    jfieldID end_field =
        jni::introspect::lookup_field(env, object_class, "end", "Lnet/quasardb/qdb/ts/Timespec;");

    return qdb_ts_range_t{
        adapt::timespec::to_qdb(env, env.instance().GetObjectField(input, begin_field)),
        adapt::timespec::to_qdb(env, env.instance().GetObjectField(input, end_field))};
}

inline jni::guard::local_ref<jobject> to_java(qdb::jni::env & env, qdb_ts_range_t const & input)
{
    auto begin = adapt::timespec::to_java(env, input.begin);
    auto end   = adapt::timespec::to_java(env, input.end);

    return jni::object::create(env, "net/quasardb/qdb/ts/TimeRange",
        "(Lnet/quasardb/qdb/ts/Timespec;Lnet/quasardb/qdb/ts/Timespec;)V", begin.release(),
        end.release());
}

template <ranges::input_range R, typename OutputIterator>
inline void to_qdb(jni::env & env, R const & xs, OutputIterator out)
{
    auto view = ranges::views::transform(xs, [&env](jobject x) -> qdb_ts_range_t {
        return qdb::jni::adapt::timerange::to_qdb(env, x);
    });

    ranges::copy(ranges::begin(view), ranges::end(view), out);
}

template <ranges::input_range R>
inline std::vector<qdb_ts_range_t> to_qdb(jni::env & env, R const & xs)
{
    std::vector<qdb_ts_range_t> ret{ranges::size(xs)};
    to_qdb<R>(env, xs, ret.begin());
    return ret;
}

}; // namespace qdb::jni::adapt::timerange
