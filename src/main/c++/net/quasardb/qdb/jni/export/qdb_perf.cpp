#include <qdb/perf.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../guard/local_ref.h"
#include "../util/helpers.h"
#include "../string.h"
#include "../introspect.h"
#include "../debug.h"
#include "../object.h"
#include "../env.h"

namespace jni = qdb::jni;

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_enable_1performance_1trace(JNIEnv * jniEnv, jclass /* thisClass */, jlong handle) {
  return qdb_perf_enable_client_tracking((qdb_handle_t)handle);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_disable_1performance_1trace(JNIEnv * jniEnv, jclass /* thisClass */, jlong handle) {
  qdb_error_t err;

  err = qdb_perf_clear_all_profiles((qdb_handle_t)handle);
  if (QDB_FAILURE(err)) {
    return err;
  }

  return qdb_perf_disable_client_tracking((qdb_handle_t)handle);
}

jni::guard::local_ref<jobject>
native_to_trace(jni::env & env, qdb_perf_profile_t const & profile, jclass traceClass, jmethodID traceConstructor, jclass measurementClass, jmethodID measurementConstructor) {
  jni::guard::local_ref<jobject> output (jni::object::create(env,
                                                             traceClass,
                                                             traceConstructor,
                                                             jni::string::create(env, profile.name).release()));

  for (qdb_size_t i = 0; i < profile.count; ++i) {
    qdb_perf_measurement_t const & m = profile.measurements[i];

    jni::guard::local_ref<jobject> measurement(jni::object::create(env,
                                                                   measurementClass,
                                                                   measurementConstructor,
                                                                   (jint)(0),
                                                                   (jlong)(m.elapsed)));

  }


  return std::move(output);
}

jni::guard::local_ref<jstring>
native_to_label(jni::env & env, enum qdb_perf_label_t l) {
  switch (l) {
  case qdb_pl_undefined:
    return jni::string::create_utf8(env, "undefined");
  case qdb_pl_accepted:
    return jni::string::create_utf8(env, "accepted");
  case qdb_pl_received:
    return jni::string::create_utf8(env, "received");
  case qdb_pl_secured:
    return jni::string::create_utf8(env, "secured");
  case qdb_pl_deserialization_starts:
    return jni::string::create_utf8(env, "deserialization_starts");
  case qdb_pl_deserialization_ends:
    return jni::string::create_utf8(env, "deserialization_ends");
  case qdb_pl_entering_chord:
    return jni::string::create_utf8(env, "entering_chord");
  case qdb_pl_processing_starts:
    return jni::string::create_utf8(env, "processing_starts");
  case qdb_pl_dispatch:
    return jni::string::create_utf8(env, "dispatch");
  case qdb_pl_serialization_starts:
    return jni::string::create_utf8(env, "serialization_starts");
  case qdb_pl_serialization_ends:
    return jni::string::create_utf8(env, "serialization_ends");
  case qdb_pl_processing_ends:
    return jni::string::create_utf8(env, "processing_ends");
  case qdb_pl_replying:
    return jni::string::create_utf8(env, "replying");
  case qdb_pl_replied:
    return jni::string::create_utf8(env, "replied");
  case qdb_pl_entry_writing_starts:
    return jni::string::create_utf8(env, "entry_writing_starts");
  case qdb_pl_entry_writing_ends:
    return jni::string::create_utf8(env, "entry_writing_ends");
  case qdb_pl_content_reading_starts:
    return jni::string::create_utf8(env, "content_reading_starts");
  case qdb_pl_content_reading_ends:
    return jni::string::create_utf8(env, "content_reading_ends");
  case qdb_pl_content_writing_starts:
    return jni::string::create_utf8(env, "content_writing_starts");
  case qdb_pl_content_writing_ends:
    return jni::string::create_utf8(env, "content_writing_ends");
  case qdb_pl_directory_reading_starts:
    return jni::string::create_utf8(env, "directory_reading_starts");
  case qdb_pl_directory_reading_ends:
    return jni::string::create_utf8(env, "directory_reading_ends");
  case qdb_pl_directory_writing_starts:
    return jni::string::create_utf8(env, "directory_writing_starts");
  case qdb_pl_directory_writing_ends:
    return jni::string::create_utf8(env, "directory_writing_ends");
  case qdb_pl_entry_trimming_starts:
    return jni::string::create_utf8(env, "entry_trimming_starts");
  case qdb_pl_entry_trimming_ends:
    return jni::string::create_utf8(env, "entry_trimming_ends");
  case qdb_pl_ts_evaluating_starts:
    return jni::string::create_utf8(env, "ts_evaluating_starts");
  case qdb_pl_ts_evaluating_ends:
    return jni::string::create_utf8(env, "ts_evaluating_ends");
  case qdb_pl_ts_bucket_updating_starts:
    return jni::string::create_utf8(env, "ts_bucket_updating_starts");
  case qdb_pl_ts_bucket_updating_ends:
    return jni::string::create_utf8(env, "ts_bucket_updating_ends");
  case qdb_pl_affix_search_starts:
    return jni::string::create_utf8(env, "affix_search_starts");
  case qdb_pl_affix_search_ends:
    return jni::string::create_utf8(env, "affix_search_ends");
  case qdb_pl_unknown:
    return jni::string::create_utf8(env, "unknown");
  }

  return jni::string::create_utf8(env, "uncategorized");

}

jni::guard::local_ref<jobjectArray>
native_to_measurements(jni::env & env, qdb_perf_profile_t const & profile, jclass measurementClass, jmethodID measurementConstructor) {
  jni::guard::local_ref<jobjectArray> output (jni::object::create_array(env,
                                                                        profile.count,
                                                                        measurementClass));

  for (qdb_size_t i = 0; i < profile.count; ++i) {
    qdb_perf_measurement_t const & m = profile.measurements[i];

    // jni::debug::println(env, std::string("elapsed: ") + std::to_string(m.elapsed));
    // jni::debug::println(env, std::string("label: ") + std::to_string(m.label));

    env.instance().SetObjectArrayElement(output,
                                         (jsize)i,
                                         jni::object::create(env,
                                                             measurementClass,
                                                             measurementConstructor,
                                                             native_to_label(env, m.label).release(),
                                                             (jlong)(m.elapsed)).release());

  }


  return std::move(output);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1performance_1traces(JNIEnv * jniEnv, jclass /* thisClass */, jlong handle, jobject output) {
  jni::env env(jniEnv);

  qdb_error_t err;

  qdb_perf_profile_t * profiles;
  qdb_size_t profiles_count;

  err = qdb_perf_get_profiles((qdb_handle_t)handle, &profiles, &profiles_count);
  if (QDB_FAILURE(err)) {
    return err;
  }

  //! Initialy class + functions once, cache them accross all operations
  jclass trace_class =
    jni::introspect::lookup_class(env, "net/quasardb/qdb/PerformanceTrace$Trace");

  jmethodID trace_constructor =
    jni::introspect::lookup_method(env, trace_class,
                                   "<init>",
                                   "(Ljava/lang/String;[Lnet/quasardb/qdb/PerformanceTrace$Measurement;)V");

  jclass measurement_class =
    jni::introspect::lookup_class(env, "net/quasardb/qdb/PerformanceTrace$Measurement");

  jmethodID measurement_constructor =
    jni::introspect::lookup_method(env, measurement_class,
                                   "<init>",
                                   "(Ljava/lang/String;J)V");

  //! Create array for all available profiles
  jni::guard::local_ref<jobjectArray> xs (jni::object::create_array(env,
                                                                    profiles_count,
                                                                    trace_class));
  // For each profile, convert to Trace object and add to array
  for (qdb_size_t i = 0; i < profiles_count; ++i) {
    env.instance().SetObjectArrayElement(xs,
                                         (jsize)i,
                                         jni::object::create(env,
                                                             trace_class,
                                                             trace_constructor,
                                                             jni::string::create_utf8(env, profiles[i].name.data).release(),

                                                             native_to_measurements(env,
                                                                                    profiles[i],
                                                                                    measurement_class,
                                                                                    measurement_constructor).release()));
  }

  //! And return output by swapping the reference
  setReferenceValue(env, output, xs.release());

  return qdb_e_ok;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_clear_1performance_1traces(JNIEnv * jniEnv, jclass /* thisClass */, jlong handle) {
  return qdb_perf_clear_all_profiles((qdb_handle_t)handle);
}
