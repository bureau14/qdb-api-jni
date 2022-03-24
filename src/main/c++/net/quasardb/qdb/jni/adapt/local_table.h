#pragma once

#include "../exception.h"
#include "../guard/local_ref.h"
#include "../object.h"
#include "../object_array.h"
#include "timespec.h"
#include <qdb/ts.h>
#include <jni.h>
#include <vector>

namespace qdb::jni::adapt::local_table
{
jni::guard::local_ref<jobject> _read_value(qdb::jni::env & env,
    qdb_handle_t handle,
    qdb_local_table_t localTable,
    std::size_t idx,
    qdb_ts_column_type_t type);

template <ranges::input_range R>
inline jni::object_array _read_values(
    qdb::jni::env & env, qdb_handle_t handle, qdb_local_table_t localTable, R const & columns)
{
    // Maybe hack, maybe not; more elegant would be to zip the columns with a
    // range of (0, 1, 2, 3, .... columns.size()-1), but we don't have a zip
    // range transform yet.
    std::size_t i = 0;
    auto callback = [&env, &handle, &localTable, &i](
                        qdb_ts_column_info_ex_t const & column) -> jobject {
        return local_table::_read_value(env, handle, localTable, i++, column.type).release();
    };

    return jni::make_object_array(
        env, "net/quasardb/qdb/ts/Value", ranges::views::transform(columns, callback));
}

template <ranges::input_range R>
inline qdb::jni::guard::local_ref<jobject> next_row(
    qdb::jni::env & env, qdb_handle_t handle, qdb_local_table_t localTable, R const & columns)
{
    qdb_timespec_t row_ts;
    qdb_error_t err = qdb_ts_table_next_row(localTable, &row_ts);

    if (err == qdb_e_iterator_end)
    {
        return jni::guard::local_ref<jobject>{env};
    }

    jni::exception::throw_if_error(handle, err);

    jni::object_array vals = _read_values(env, handle, localTable, columns);

    return jni::object::create(env, "net/quasardb/qdb/ts/WritableRow",
        "(Lnet/quasardb/qdb/ts/Timespec;[Lnet/quasardb/qdb/ts/Value;)V",
        jni::adapt::timespec::to_java(env, row_ts).release(), vals.release());
}

}; // namespace qdb::jni::adapt::local_table
