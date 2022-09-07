#include "../../adapt/point.h"
#include "../../adapt/timerange.h"
#include "../../exception.h"
#include "../../guard/qdb_resource.h"
#include "../../object_array.h"
#include "../../string.h"
#include "net_quasardb_qdb_jni_qdb.h"

namespace jni = qdb::jni;

/**
 * Utility struct that handles the boilerplate of adapting all input values
 * before dispatching to the jni::adapt::point::to_qdb.
 */
template <typename From>
struct points_inserter
{
    // Low level 'point' type, a pair of timespec/value, e.g.
    // qdb_ts_double_point.
    using point_type = typename jni::adapt::value_traits<From>::point_type;

    // How JNI represents the array that input values are represented as, e.g.
    // jdoubleArray for doubles and jobjectArray for strings.
    using jarray_type = typename jni::adapt::value_traits<From>::jarray_type;

    using CallbackSignature = qdb_error_t(
        qdb_handle_t, char const *, char const *, point_type const *, qdb_size_t);

    /**
     * @param f This is our callback function that handles the actual
     * invocation. Its signature must match that of CallbackSignature, and is
     * expected to be a reference of one of the `qdb_ts_double_insert` etc
     * functions.
     */
    template <typename Callable>
    inline qdb_error_t operator()(JNIEnv * jniEnv,
        jlong handle,
        jstring table,
        jstring column,
        jobject timestamps,
        jarray_type values,
        Callable f)
    {
        static_assert(std::is_convertible_v<Callable &&, std::function<CallbackSignature>>,
            "Callback function signature doesn't match expected type");

        qdb::jni::env env(jniEnv);
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        try
        {
            auto table_  = qdb::jni::string::get_chars_utf8(env, table);
            auto column_ = qdb::jni::string::get_chars_utf8(env, column);

            std::vector<qdb_timespec_t> timestamps_ =
                jni::adapt::timespecs::to_qdb(env, timestamps);

            std::vector<point_type> xs = jni::adapt::point::to_qdb<From>(env, timestamps_, values);

            /**
             * All data is in the correct shape now, invoke the actual insertion
             * function. If you're getting compilation errors here, it means
             * that the callback function doesn't match the expected signature.
             */

            qdb_error_t e = jni::exception::throw_if_error(
                handle_, f(handle_, table_.get(), column_.get(), xs.data(), xs.size()));
            return e;
        }
        catch (jni::exception const & e)
        {
            e.throw_new(env);
            return e.error();
        }
    }
};

/**
 * Utility struct that handles the boilerplate of dispatching to
 * the jni::adapt::point::to_java function.
 */
template <typename From>
struct points_retriever
{
    using point_type        = typename jni::adapt::value_traits<From>::point_type;
    using jarray_type       = typename jni::adapt::value_traits<From>::jarray_type;
    using CallbackSignature = qdb_error_t(qdb_handle_t /* handle */,
        char const * /* alias */,
        char const * /* column */,
        qdb_ts_range_t const * /* ranges */,
        qdb_size_t /* range_count */,
        point_type ** /* points */,
        qdb_size_t * /* point_count */
    );

    template <typename Callable>
    inline jobject operator()(JNIEnv * jniEnv,
        jlong handle,
        jstring table,
        jstring column,
        jobjectArray ranges,
        Callable f)
    {
        static_assert(std::is_convertible_v<Callable &&, std::function<CallbackSignature>>,
            "Callback function signature doesn't match expected type");

        qdb::jni::env env(jniEnv);
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        try
        {
            auto table_  = qdb::jni::string::get_chars_utf8(env, table);
            auto column_ = qdb::jni::string::get_chars_utf8(env, column);

            std::vector<qdb_ts_range_t> ranges_ =
                jni::adapt::timerange::to_qdb(env, jni::object_array(env, ranges));

            jni::guard::qdb_resource<point_type *> xs{handle_};
            qdb_size_t n{0};

            jni::exception::throw_if_error(handle_,
                f(handle_, table_.get(), column_.get(), ranges_.data(), ranges_.size(), &xs, &n));

            assert(xs != nullptr);

            return jni::adapt::point::to_java<From>(env, ranges::views::counted(xs.get(), n))
                .release();
        }
        catch (jni::exception const & e)
        {
            e.throw_new(env);
            return nullptr;
        }
    }
};

/**
 * JNI export function. Sole purpose is to dispatch to `points_retriever`, no
 * conversions or actual logic should be done in this function.
 */
JNIEXPORT jobject JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1point_1get_1ranges(JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jstring table,
    jstring column,
    jint value_type,
    jobjectArray ranges)
{
    qdb_ts_column_type_t value_type_ = static_cast<qdb_ts_column_type_t>(value_type);

    switch (value_type_)
    {
#define CASE(x, f)                                                                         \
    case jni::adapt::value_traits<x>::column_type:                                         \
    {                                                                                      \
        using value_type_t = typename jni::adapt::value_traits<x>::value_type;             \
                                                                                           \
        static_assert(std::is_same<x, value_type_t>());                                    \
                                                                                           \
        return points_retriever<value_type_t>()(jniEnv, handle, table, column, ranges, f); \
    };

        CASE(double, qdb_ts_double_get_ranges);
        CASE(qdb_int_t, qdb_ts_int64_get_ranges);
        CASE(qdb_timespec_t, qdb_ts_timestamp_get_ranges);
        CASE(qdb_blob_t, qdb_ts_blob_get_ranges);
        CASE(qdb_string_t, qdb_ts_string_get_ranges);

#undef CASE
    default:
        throw new jni::exception(qdb_e_incompatible_type, "Unrecognized value type");
    };
}

/**
 * JNI export function. Sole purpose is to dispatch to `points_inserter`, no
 * conversions or actual logic should be done in this function.
 */
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
#define CASE(x, f)                                                              \
    case jni::adapt::value_traits<x>::column_type:                              \
    {                                                                           \
        using jarray_type  = typename jni::adapt::value_traits<x>::jarray_type; \
        using value_type_t = typename jni::adapt::value_traits<x>::value_type;  \
                                                                                \
        static_assert(std::is_same<x, value_type_t>());                         \
                                                                                \
        jarray_type values_ = reinterpret_cast<jarray_type>(values);            \
        return points_inserter<value_type_t>()(                                 \
            jniEnv, handle, table, column, timestamps, values_, f);             \
    };

        CASE(double, qdb_ts_double_insert);
        CASE(qdb_int_t, qdb_ts_int64_insert);
        CASE(qdb_timespec_t, qdb_ts_timestamp_insert);
        CASE(qdb_blob_t, qdb_ts_blob_insert);
        CASE(qdb_string_t, qdb_ts_string_insert);

#undef CASE
    default:
        throw new jni::exception(qdb_e_incompatible_type, "Unrecognized value type");
    };
}
