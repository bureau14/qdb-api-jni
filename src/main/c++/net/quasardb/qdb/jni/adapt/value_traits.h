#pragma once

#include <qdb/client.h>
#include <qdb/ts.h>
#include <cassert>
#include <cmath>

namespace qdb
{
namespace jni
{
namespace adapt
{

template <typename ValueType>
struct value_traits_base
{
    using is_trivial = typename std::is_trivial<ValueType>::type;
    using value_type = ValueType;
};

template <typename ValueType>
struct value_traits : public value_traits_base<ValueType>
{
    typedef ValueType value_type;
    typedef jarray jarray_type;

    inline static bool is_null(value_type x);
};

template <>
struct value_traits<qdb_int_t> : public value_traits_base<qdb_int_t>
{
    static constexpr qdb_int_t null_value             = 0x8000000000000000ll;
    static constexpr qdb_ts_column_type_t column_type = qdb_ts_column_int64;

    using point_type  = qdb_ts_int64_point;
    using jtype       = jlong;
    using jarray_type = jlongArray;

    inline static bool is_null(qdb_int_t x)
    {
        return x == null_value;
    }
};

template <>
struct value_traits<double> : public value_traits_base<double>
{
    static constexpr qdb_ts_column_type_t column_type = qdb_ts_column_double;

    using point_type  = qdb_ts_double_point;
    using jtype       = jdouble;
    using jarray_type = jdoubleArray;

    inline static bool is_null(double x)
    {
        return std::isnan(x);
    }
};

template <>
struct value_traits<qdb_timespec_t> : public value_traits_base<qdb_timespec_t>
{
    static constexpr qdb_ts_column_type_t column_type = qdb_ts_column_timestamp;
    using point_type                                  = qdb_ts_timestamp_point;
    using jtype                                       = jobject;
    using jarray_type                                 = jobject;

    inline static bool is_null(qdb_timespec_t x)
    {
        return x.tv_sec == qdb_min_time && x.tv_nsec == qdb_min_time;
    }
};

template <>
struct value_traits<qdb_string_t> : public value_traits_base<qdb_string_t>
{
    // can be either symbol or string, but we use string, as we're repurposing
    // value enums as column enums, because the QDB API doesn't have a notion of
    // a 'value type'
    static constexpr qdb_ts_column_type_t column_type = qdb_ts_column_string;
    using point_type                                  = qdb_ts_string_point;
    using jtype                                       = jobject;
    using jarray_type                                 = jobjectArray;

    inline static bool is_null(qdb_string_t x)
    {
        assert((x.length == 0) == (x.data == nullptr));
        return x.length == 0;
    }
};

template <>
struct value_traits<qdb_blob_t> : public value_traits_base<qdb_blob_t>
{
    static constexpr qdb_ts_column_type_t column_type = qdb_ts_column_blob;
    using point_type                                  = qdb_ts_blob_point;
    using jtype                                       = jobject;
    using jarray_type                                 = jobjectArray;

    inline static bool is_null(qdb_blob_t x)
    {
        assert((x.content_length == 0) == (x.content == nullptr));
        return x.content_length == 0;
    }
};

}; // namespace adapt
}; // namespace jni
}; // namespace qdb
