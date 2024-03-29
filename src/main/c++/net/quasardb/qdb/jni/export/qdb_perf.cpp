#include "../debug.h"
#include "../env.h"
#include "../exception.h"
#include "../guard/local_ref.h"
#include "../introspect.h"
#include "../object.h"
#include "../string.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/perf.h>

namespace jni = qdb::jni;

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_enable_1performance_1trace(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong handle)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        return jni::exception::throw_if_error(handle_, qdb_perf_enable_client_tracking(handle_));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_disable_1performance_1trace(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong handle)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        jni::exception::throw_if_error(handle_, qdb_perf_clear_all_profiles(handle_));

        return jni::exception::throw_if_error(handle_, qdb_perf_disable_client_tracking(handle_));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

jni::guard::local_ref<jobject> native_to_trace(jni::env & env,
    qdb_handle_t handle,
    qdb_perf_profile_t const & profile,
    jclass traceClass,
    jmethodID traceConstructor,
    jclass measurementClass,
    jmethodID measurementConstructor)
{
    jni::guard::local_ref<jobject> output(jni::object::create(env, traceClass, traceConstructor,
        jni::string::create(env, handle, profile.name).release()));

    for (qdb_size_t i = 0; i < profile.count; ++i)
    {
        qdb_perf_measurement_t const & m = profile.measurements[i];

        jni::guard::local_ref<jobject> measurement(jni::object::create(
            env, measurementClass, measurementConstructor, (jint)(0), (jlong)(m.elapsed)));
    }

    return output;
}

jni::guard::local_ref<jstring> native_to_label(
    jni::env & env, qdb_handle_t handle, enum qdb_perf_label_t l)
{
    qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

    switch (l)
    {
    case qdb_pl_undefined:
        return jni::string::create_utf8(env, handle, "undefined");
    case qdb_pl_accepted:
        return jni::string::create_utf8(env, handle, "accepted");
    case qdb_pl_received:
        return jni::string::create_utf8(env, handle, "received");
    case qdb_pl_secured:
        return jni::string::create_utf8(env, handle, "secured");
    case qdb_pl_deserialization_starts:
        return jni::string::create_utf8(env, handle, "deserialization_starts");
    case qdb_pl_deserialization_ends:
        return jni::string::create_utf8(env, handle, "deserialization_ends");
    case qdb_pl_entering_chord:
        return jni::string::create_utf8(env, handle, "entering_chord");
    case qdb_pl_processing_starts:
        return jni::string::create_utf8(env, handle, "processing_starts");
    case qdb_pl_dispatch:
        return jni::string::create_utf8(env, handle, "dispatch");
    case qdb_pl_serialization_starts:
        return jni::string::create_utf8(env, handle, "serialization_starts");
    case qdb_pl_serialization_ends:
        return jni::string::create_utf8(env, handle, "serialization_ends");
    case qdb_pl_processing_ends:
        return jni::string::create_utf8(env, handle, "processing_ends");
    case qdb_pl_replying:
        return jni::string::create_utf8(env, handle, "replying");
    case qdb_pl_replied:
        return jni::string::create_utf8(env, handle, "replied");
    case qdb_pl_entry_writing_starts:
        return jni::string::create_utf8(env, handle, "entry_writing_starts");
    case qdb_pl_entry_writing_ends:
        return jni::string::create_utf8(env, handle, "entry_writing_ends");
    case qdb_pl_content_reading_starts:
        return jni::string::create_utf8(env, handle, "content_reading_starts");
    case qdb_pl_content_reading_ends:
        return jni::string::create_utf8(env, handle, "content_reading_ends");
    case qdb_pl_content_writing_starts:
        return jni::string::create_utf8(env, handle, "content_writing_starts");
    case qdb_pl_content_writing_ends:
        return jni::string::create_utf8(env, handle, "content_writing_ends");
    case qdb_pl_directory_reading_starts:
        return jni::string::create_utf8(env, handle, "directory_reading_starts");
    case qdb_pl_directory_reading_ends:
        return jni::string::create_utf8(env, handle, "directory_reading_ends");
    case qdb_pl_directory_writing_starts:
        return jni::string::create_utf8(env, handle, "directory_writing_starts");
    case qdb_pl_directory_writing_ends:
        return jni::string::create_utf8(env, handle, "directory_writing_ends");
    case qdb_pl_entry_trimming_starts:
        return jni::string::create_utf8(env, handle, "entry_trimming_starts");
    case qdb_pl_entry_trimming_ends:
        return jni::string::create_utf8(env, handle, "entry_trimming_ends");
    case qdb_pl_ts_evaluating_starts:
        return jni::string::create_utf8(env, handle, "ts_evaluating_starts");
    case qdb_pl_ts_evaluating_ends:
        return jni::string::create_utf8(env, handle, "ts_evaluating_ends");
    case qdb_pl_ts_bucket_updating_starts:
        return jni::string::create_utf8(env, handle, "ts_bucket_updating_starts");
    case qdb_pl_ts_bucket_updating_ends:
        return jni::string::create_utf8(env, handle, "ts_bucket_updating_ends");
    case qdb_pl_affix_search_starts:
        return jni::string::create_utf8(env, handle, "affix_search_starts");
    case qdb_pl_affix_search_ends:
        return jni::string::create_utf8(env, handle, "affix_search_ends");
    case qdb_pl_eviction_starts:
        return jni::string::create_utf8(env, handle, "eviction_starts");
    case qdb_pl_eviction_ends:
        return jni::string::create_utf8(env, handle, "eviction_ends");
    case qdb_pl_time_vector_tracker_reading_starts:
        return jni::string::create_utf8(env, handle, "time_vector_tracker_reading_starts");
    case qdb_pl_time_vector_tracker_reading_ends:
        return jni::string::create_utf8(env, handle, "time_vector_tracker_reading_ends");
    case qdb_pl_bucket_reading_starts:
        return jni::string::create_utf8(env, handle, "bucket_reading_starts");
    case qdb_pl_bucket_reading_ends:
        return jni::string::create_utf8(env, handle, "bucket_reading_ends");
    case qdb_pl_entries_directory_reading_starts:
        return jni::string::create_utf8(env, handle, "entries_directory_reading_starts");
    case qdb_pl_entries_directory_reading_ends:
        return jni::string::create_utf8(env, handle, "entries_directory_reading_ends");
    case qdb_pl_acl_reading_starts:
        return jni::string::create_utf8(env, handle, "acl_reading_starts");
    case qdb_pl_acl_reading_ends:
        return jni::string::create_utf8(env, handle, "acl_reading_ends");
    case qdb_pl_time_vector_reading_starts:
        return jni::string::create_utf8(env, handle, "time_vector_reading_starts");
    case qdb_pl_time_vector_reading_ends:
        return jni::string::create_utf8(env, handle, "time_vector_reading_ends");
    case qdb_pl_unknown:
        return jni::string::create_utf8(env, handle, "unknown");
    }

    return jni::string::create_utf8(env, handle, "uncategorized");
}

jni::guard::local_ref<jobjectArray> native_to_measurements(jni::env & env,
    qdb_handle_t handle,
    qdb_perf_profile_t const & profile,
    jclass measurementClass,
    jmethodID measurementConstructor)
{
    jni::guard::local_ref<jobjectArray> output(
        jni::object::create_array(env, profile.count, measurementClass));

    for (qdb_size_t i = 0; i < profile.count; ++i)
    {
        qdb_perf_measurement_t const & m = profile.measurements[i];

        // jni::debug::println(env, std::string("elapsed: ") +
        // std::to_string(m.elapsed)); jni::debug::println(env,
        // std::string("label: ") + std::to_string(m.label));

        env.instance().SetObjectArrayElement(output, (jsize)i,
            jni::object::create(env, measurementClass, measurementConstructor,
                native_to_label(env, handle, m.label).release(), (jlong)(m.elapsed))
                .release());
    }

    return output;
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_get_1performance_1traces(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong handle, jobject output)
{
    jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        qdb_perf_profile_t * profiles;
        qdb_size_t profiles_count;

        jni::exception::throw_if_error(
            handle_, qdb_perf_get_profiles(handle_, &profiles, &profiles_count));

        //! Initialy class + functions once, cache them accross all operations
        jclass trace_class =
            jni::introspect::lookup_class(env, "net/quasardb/qdb/PerformanceTrace$Trace");

        jmethodID trace_constructor = jni::introspect::lookup_method(env, trace_class, "<init>",
            "(Ljava/lang/String;[Lnet/quasardb/qdb/"
            "PerformanceTrace$Measurement;)V");

        jclass measurement_class =
            jni::introspect::lookup_class(env, "net/quasardb/qdb/PerformanceTrace$Measurement");

        jmethodID measurement_constructor = jni::introspect::lookup_method(
            env, measurement_class, "<init>", "(Ljava/lang/String;J)V");

        //! Create array for all available profiles
        jni::guard::local_ref<jobjectArray> xs(
            jni::object::create_array(env, profiles_count, trace_class));
        // For each profile, convert to Trace object and add to array
        for (qdb_size_t i = 0; i < profiles_count; ++i)
        {
            env.instance().SetObjectArrayElement(xs, (jsize)i,
                jni::object::create(env, trace_class, trace_constructor,
                    jni::string::create_utf8(env, handle_, profiles[i].name.data).release(),

                    native_to_measurements(
                        env, handle_, profiles[i], measurement_class, measurement_constructor)
                        .release()));
        }

        //! And return output by swapping the reference
        setReferenceValue(env, output, xs.release());

        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_clear_1performance_1traces(
    JNIEnv * /* jniEnv */, jclass /* thisClass */, jlong handle)
{
    return qdb_perf_clear_all_profiles((qdb_handle_t)handle);
}
