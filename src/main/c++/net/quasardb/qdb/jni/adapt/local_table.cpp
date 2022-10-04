
#include "local_table.h"
#include "../exception.h"
#include "value.h"
#include "value_traits.h"

template <qdb_ts_column_type_t ColumnType>
struct value_handler
{
    qdb::jni::guard::local_ref<jobject> operator()(
        qdb::jni::env & env, qdb_handle_t handle, qdb_local_table_t localTable, std::size_t idx);
};

template <>
struct value_handler<qdb_ts_column_double>
{
    qdb::jni::guard::local_ref<jobject> operator()(
        qdb::jni::env & env, qdb_handle_t handle, qdb_local_table_t localTable, std::size_t idx)
    {
        double value;
        qdb::jni::exception::throw_if_error(handle, qdb_ts_row_get_double(localTable, idx, &value));

        return qdb::jni::adapt::value::to_java(env, handle, value);
    }
};

template <>
struct value_handler<qdb_ts_column_blob>
{
    qdb::jni::guard::local_ref<jobject> operator()(
        qdb::jni::env & env, qdb_handle_t handle, qdb_local_table_t localTable, std::size_t idx)
    {
        qdb_blob_t value;
        qdb::jni::exception::throw_if_error(handle,
            qdb_ts_row_get_blob(localTable, idx, &(value.content), &(value.content_length)));

        return qdb::jni::adapt::value::to_java(env, handle, value);
    }
};

template <>
struct value_handler<qdb_ts_column_int64>
{
    qdb::jni::guard::local_ref<jobject> operator()(
        qdb::jni::env & env, qdb_handle_t handle, qdb_local_table_t localTable, std::size_t idx)
    {
        qdb_int_t value;
        qdb::jni::exception::throw_if_error(handle, qdb_ts_row_get_int64(localTable, idx, &value));

        return qdb::jni::adapt::value::to_java(env, handle, value);
    }
};

template <>
struct value_handler<qdb_ts_column_timestamp>
{
    qdb::jni::guard::local_ref<jobject> operator()(
        qdb::jni::env & env, qdb_handle_t handle, qdb_local_table_t localTable, std::size_t idx)
    {
        qdb_timespec_t value;
        qdb::jni::exception::throw_if_error(
            handle, qdb_ts_row_get_timestamp(localTable, idx, &value));

        return qdb::jni::adapt::value::to_java(env, handle, value);
    }
};

template <>
struct value_handler<qdb_ts_column_string>
{
    qdb::jni::guard::local_ref<jobject> operator()(
        qdb::jni::env & env, qdb_handle_t handle, qdb_local_table_t localTable, std::size_t idx)
    {
        qdb_string_t value;
        qdb::jni::exception::throw_if_error(
            handle, qdb_ts_row_get_string(localTable, idx, &(value.data), &(value.length)));

        return qdb::jni::adapt::value::to_java(env, handle, value);
    }
};

template <>
struct value_handler<qdb_ts_column_symbol>
{
    qdb::jni::guard::local_ref<jobject> operator()(
        qdb::jni::env & env, qdb_handle_t handle, qdb_local_table_t localTable, std::size_t idx)
    {
        return value_handler<qdb_ts_column_string>()(env, handle, localTable, idx);
    }
};

template <class... Args>
constexpr decltype(auto) _dispatch(qdb_ts_column_type_t type, Args &&... args)
{
    switch (type)
    {
#define CASE(x) \
    case x:     \
        return value_handler<x>()(args...);

        CASE(qdb_ts_column_double)
        CASE(qdb_ts_column_int64)
        CASE(qdb_ts_column_blob)
        CASE(qdb_ts_column_string)
        CASE(qdb_ts_column_timestamp)
        CASE(qdb_ts_column_symbol)

    default:
        throw new qdb::jni::exception(qdb_e_incompatible_type,
            "Incompatible type while dispatching local table value handler");
    }
}

/* static */ qdb::jni::guard::local_ref<jobject> qdb::jni::adapt::local_table::_read_value(
    qdb::jni::env & env,
    qdb_handle_t handle,
    qdb_local_table_t localTable,
    std::size_t idx,
    qdb_ts_column_type_t type)
{
    return _dispatch(type, env, handle, localTable, idx);
}
