#include "../../adapt/point.h"
#include "../../adapt/timerange.h"
#include "../../exception.h"
#include "../../guard/qdb_resource.h"
#include "../../object_array.h"
#include "../../string.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/ts.h>
#include <algorithm>
#include <cstring>
#include <vector>

namespace jni = qdb::jni;

namespace
{
constexpr char const * table_column_name = "$table";

template <typename From>
using point_type_t = typename jni::adapt::value_traits<From>::point_type;

template <typename From>
using jarray_type_t = typename jni::adapt::value_traits<From>::jarray_type;

inline bool column_types_match(qdb_ts_column_type_t expected, qdb_ts_column_type_t actual) noexcept
{
    return expected == actual
           || (expected == qdb_ts_column_string && actual == qdb_ts_column_symbol);
}

inline qdb_ts_column_type_t expected_column_type(qdb_ts_column_type_t value_type) noexcept
{
    return value_type;
}

inline qdb_exp_batch_push_column_t const * find_requested_column(
    qdb_bulk_reader_table_data_t const & table_data,
    qdb::jni::guard::string_utf8 const & column_name)
{
    qdb_exp_batch_push_column_t const * result = nullptr;

    for (qdb_size_t idx = 0; idx < table_data.column_count; ++idx)
    {
        auto const & candidate = table_data.columns[idx];
        if (candidate.name == nullptr)
        {
            continue;
        }

        if (std::strcmp(candidate.name, column_name.get()) == 0)
        {
            return &candidate;
        }
    }

    return result;
}

inline qdb::jni::guard::local_ref<jobject> empty_points(
    qdb::jni::env & env, qdb_handle_t handle, qdb_ts_column_type_t value_type)
{
    std::vector<qdb_timespec_t> timestamps;

    switch (value_type)
    {
    case qdb_ts_column_double:
    {
        std::vector<double> values;
        auto timestamps_ = jni::adapt::timespecs::to_java(env, timestamps);
        auto values_     = jni::adapt::point::detail::xform_output<double>(env, handle, values);
        return jni::adapt::point::detail::create<double>(
            env, std::move(timestamps_), std::move(values_));
    }
    case qdb_ts_column_int64:
    {
        std::vector<qdb_int_t> values;
        auto timestamps_ = jni::adapt::timespecs::to_java(env, timestamps);
        auto values_     = jni::adapt::point::detail::xform_output<qdb_int_t>(env, handle, values);
        return jni::adapt::point::detail::create<qdb_int_t>(
            env, std::move(timestamps_), std::move(values_));
    }
    case qdb_ts_column_timestamp:
    {
        std::vector<qdb_timespec_t> values;
        auto timestamps_ = jni::adapt::timespecs::to_java(env, timestamps);
        auto values_ = jni::adapt::point::detail::xform_output<qdb_timespec_t>(env, handle, values);
        return jni::adapt::point::detail::create<qdb_timespec_t>(
            env, std::move(timestamps_), std::move(values_));
    }
    case qdb_ts_column_blob:
    {
        std::vector<qdb_blob_t> values;
        auto timestamps_ = jni::adapt::timespecs::to_java(env, timestamps);
        auto values_     = jni::adapt::point::detail::xform_output<qdb_blob_t>(env, handle, values);
        return jni::adapt::point::detail::create<qdb_blob_t>(
            env, std::move(timestamps_), std::move(values_));
    }
    case qdb_ts_column_string:
    case qdb_ts_column_symbol:
    {
        std::vector<qdb_string_t> values;
        auto timestamps_ = jni::adapt::timespecs::to_java(env, timestamps);
        auto values_ = jni::adapt::point::detail::xform_output<qdb_string_t>(env, handle, values);
        return jni::adapt::point::detail::create<qdb_string_t>(
            env, std::move(timestamps_), std::move(values_));
    }
    default:
        throw jni::exception(qdb_e_incompatible_type, "Unrecognized value type");
    }
}

template <typename From>
inline void release_points(qdb_handle_t /* handle */, std::vector<point_type_t<From>> const &)
{}

template <>
inline void release_points<qdb_string_t>(
    qdb_handle_t handle, std::vector<point_type_t<qdb_string_t>> const & points)
{
    for (auto const & point : points)
    {
        if (point.content != nullptr)
        {
            qdb_release(handle, point.content);
        }
    }
}

template <typename From>
struct exp_batch_column_builder
{
    using point_type = point_type_t<From>;
    using value_type = typename jni::adapt::value_traits<From>::value_type;

    std::vector<value_type> values;

    explicit exp_batch_column_builder(std::vector<point_type> const & points)
        : values(points.size())
    {
        std::transform(points.begin(), points.end(), values.begin(), [](point_type const & point) {
            return point.value;
        });
    }

    void bind(qdb_exp_batch_push_column_t & column)
    {
        if constexpr (std::is_same_v<value_type, double>)
        {
            column.data.doubles = values.data();
        }
        else if constexpr (std::is_same_v<value_type, qdb_int_t>)
        {
            column.data.ints = values.data();
        }
        else if constexpr (std::is_same_v<value_type, qdb_timespec_t>)
        {
            column.data.timestamps = values.data();
        }
    }
};

template <>
struct exp_batch_column_builder<qdb_blob_t>
{
    std::vector<qdb_blob_t> values;

    explicit exp_batch_column_builder(std::vector<point_type_t<qdb_blob_t>> const & points)
        : values(points.size())
    {
        std::transform(points.begin(), points.end(), values.begin(), [](auto const & point) {
            return qdb_blob_t{point.content, point.content_length};
        });
    }

    void bind(qdb_exp_batch_push_column_t & column)
    {
        column.data.blobs = values.data();
    }
};

template <>
struct exp_batch_column_builder<qdb_string_t>
{
    std::vector<qdb_string_t> values;

    explicit exp_batch_column_builder(std::vector<point_type_t<qdb_string_t>> const & points)
        : values(points.size())
    {
        std::transform(points.begin(), points.end(), values.begin(), [](auto const & point) {
            return qdb_string_t{point.content, point.content_length};
        });
    }

    void bind(qdb_exp_batch_push_column_t & column)
    {
        column.data.strings = values.data();
    }
};

template <typename From>
inline jint insert_points(JNIEnv * jniEnv,
    jlong handle,
    jstring table,
    jstring column,
    jobject timestamps,
    jarray_type_t<From> values)
{
    qdb::jni::env env(jniEnv);
    qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

    try
    {
        auto table_  = qdb::jni::string::get_chars_utf8(env, handle_, table);
        auto column_ = qdb::jni::string::get_chars_utf8(env, handle_, column);

        std::vector<qdb_timespec_t> timestamps_ = jni::adapt::timespecs::to_qdb(env, timestamps);
        std::vector<point_type_t<From>> points =
            jni::adapt::point::to_qdb<From>(env, handle_, timestamps_, values);
        struct points_guard_t
        {
            qdb_handle_t handle;
            std::vector<point_type_t<From>> const & points;

            ~points_guard_t()
            {
                release_points<From>(handle, points);
            }
        } points_guard{handle_, points};

        qdb_exp_batch_push_column_t column_data{};
        column_data.name      = column_.get();
        column_data.data_type = jni::adapt::value_traits<From>::column_type;

        exp_batch_column_builder<From> builder{points};
        builder.bind(column_data);

        qdb_exp_batch_push_table_t table_data{};
        table_data.name                  = table_.get();
        table_data.data.row_count        = timestamps_.size();
        table_data.data.column_count     = 1;
        table_data.data.timestamps       = timestamps_.data();
        table_data.data.columns          = &column_data;
        table_data.truncate_ranges       = nullptr;
        table_data.truncate_range_count  = 0;
        table_data.deduplication_mode    = qdb_exp_batch_deduplication_mode_disabled;
        table_data.where_duplicate       = nullptr;
        table_data.where_duplicate_count = 0;
        table_data.creation              = qdb_exp_batch_dont_create;

        qdb_exp_batch_options_t options{};
        options.mode       = qdb_exp_batch_push_transactional;
        options.push_flags = qdb_exp_batch_push_flag_none;

        qdb_error_t err =
            qdb_exp_batch_push_with_options(handle_, &options, &table_data, nullptr, 1);
        return jni::exception::throw_if_error(handle_, err);
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

template <typename From, typename TimestampRange, typename ValueRange>
inline qdb::jni::guard::local_ref<jobject> make_points_data(qdb::jni::env & env,
    qdb_handle_t handle,
    TimestampRange const & timestamps,
    ValueRange const & values)
{
    auto timestamps_ = jni::adapt::timespecs::to_java(env, timestamps);
    auto values_     = jni::adapt::point::detail::xform_output<From>(env, handle, values);

    return jni::adapt::point::detail::create<From>(env, std::move(timestamps_), std::move(values_));
}

inline qdb::jni::guard::local_ref<jobject> get_points(JNIEnv * jniEnv,
    jlong handle,
    jstring table,
    jstring column,
    qdb_ts_column_type_t value_type,
    jobjectArray ranges)
{
    qdb::jni::env env(jniEnv);
    qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

    try
    {
        auto table_  = qdb::jni::string::get_chars_utf8(env, handle_, table);
        auto column_ = qdb::jni::string::get_chars_utf8(env, handle_, column);

        std::vector<qdb_ts_range_t> ranges_ =
            jni::adapt::timerange::to_qdb(env, jni::object_array(env, ranges));

        const char * columns[]           = {column_.get()};
        qdb_bulk_reader_table_t tables[] = {{table_.get(), ranges_.data(), ranges_.size()}};

        jni::guard::qdb_resource<qdb_reader_handle_t> reader{handle_};
        jni::exception::throw_if_error(
            handle_, qdb_bulk_reader_fetch(handle_, columns, 1, tables, 1, &reader));

        jni::guard::qdb_resource<qdb_bulk_reader_table_data_t *> data{handle_};
        jni::exception::throw_if_error(handle_, qdb_bulk_reader_get_data(reader, &data, 0));

        if (data.get() == nullptr)
        {
            return empty_points(env, handle_, value_type);
        }

        auto const & table_data = data.get()[0];

        auto const * column_data = find_requested_column(table_data, column_);
        if (column_data == nullptr)
        {
            throw jni::exception(
                qdb_e_uninitialized, "Bulk reader did not return the requested column");
        }

        if (!column_types_match(expected_column_type(value_type), column_data->data_type))
        {
            throw jni::exception(
                qdb_e_incompatible_type, "Unexpected column type returned by bulk reader");
        }

        auto timestamps = ranges::views::counted(
            table_data.timestamps, static_cast<std::size_t>(table_data.row_count));

        switch (column_data->data_type)
        {
        case qdb_ts_column_double:
            return make_points_data<double>(env, handle_, timestamps,
                ranges::views::counted(
                    column_data->data.doubles, static_cast<std::size_t>(table_data.row_count)));

        case qdb_ts_column_int64:
            return make_points_data<qdb_int_t>(env, handle_, timestamps,
                ranges::views::counted(
                    column_data->data.ints, static_cast<std::size_t>(table_data.row_count)));

        case qdb_ts_column_timestamp:
            return make_points_data<qdb_timespec_t>(env, handle_, timestamps,
                ranges::views::counted(
                    column_data->data.timestamps, static_cast<std::size_t>(table_data.row_count)));

        case qdb_ts_column_blob:
            return make_points_data<qdb_blob_t>(env, handle_, timestamps,
                ranges::views::counted(
                    column_data->data.blobs, static_cast<std::size_t>(table_data.row_count)));

        case qdb_ts_column_symbol:
        case qdb_ts_column_string:
            return make_points_data<qdb_string_t>(env, handle_, timestamps,
                ranges::views::counted(
                    column_data->data.strings, static_cast<std::size_t>(table_data.row_count)));

        default:
            throw jni::exception(
                qdb_e_incompatible_type, "Unrecognized column type returned by bulk reader");
        }
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return jni::guard::local_ref<jobject>{env};
    }
}
} // namespace

JNIEXPORT jobject JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1point_1get_1ranges(JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jstring table,
    jstring column,
    jint value_type,
    jobjectArray ranges)
{
    return get_points(
        jniEnv, handle, table, column, static_cast<qdb_ts_column_type_t>(value_type), ranges);
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1point_1insert(JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jstring table,
    jstring column,
    jobject timestamps,
    jint value_type,
    jobject values)
{
    qdb_ts_column_type_t value_type_ = static_cast<qdb_ts_column_type_t>(value_type);

    switch (value_type_)
    {
    case jni::adapt::value_traits<double>::column_type:
        return insert_points<double>(
            jniEnv, handle, table, column, timestamps, reinterpret_cast<jdoubleArray>(values));

    case jni::adapt::value_traits<qdb_int_t>::column_type:
        return insert_points<qdb_int_t>(
            jniEnv, handle, table, column, timestamps, reinterpret_cast<jlongArray>(values));

    case jni::adapt::value_traits<qdb_timespec_t>::column_type:
        return insert_points<qdb_timespec_t>(
            jniEnv, handle, table, column, timestamps, reinterpret_cast<jobject>(values));

    case jni::adapt::value_traits<qdb_blob_t>::column_type:
        return insert_points<qdb_blob_t>(
            jniEnv, handle, table, column, timestamps, reinterpret_cast<jobjectArray>(values));

    case jni::adapt::value_traits<qdb_string_t>::column_type:
        return insert_points<qdb_string_t>(
            jniEnv, handle, table, column, timestamps, reinterpret_cast<jobjectArray>(values));

    default:
    {
        qdb::jni::env env(jniEnv);
        jni::exception e{qdb_e_incompatible_type, "Unrecognized value type"};
        e.throw_new(env);
        return e.error();
    }
    }
}
