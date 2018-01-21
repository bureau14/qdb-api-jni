#include <cassert>
#include "../util/qdb_jni.h"
#include "../ts_helpers.h"

#include "qdb_value.h"

/* static */ jobject
qdb::value::from_native(JNIEnv * env, qdb_point_result_t const & input) {
  // :TODO: cache!
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jmethodID constructor = qdb::jni::lookup_staticMethodID(env, valueClass,
                                                          "createNull",
                                                          "()Lnet/quasardb/qdb/ts/Value;");

  switch (input.type) {

  case qdb_query_result_int64:
    return _from_native_int64(env, input);
    break;

  case qdb_query_result_double:
    return _from_native_double(env, input);
    break;

  case qdb_query_result_timestamp:
    return _from_native_timestamp(env, input);
    break;

  case qdb_query_result_blob:
    return _from_native_blob(env, input);
    break;

  case qdb_query_result_none:
    return _from_native_null(env);
    break;

  };

  return _from_native_null(env);
}

/* static */ jobject
qdb::value::_from_native_int64(JNIEnv * env, qdb_point_result_t const & input) {
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jmethodID constructor = qdb::jni::lookup_staticMethodID(env, valueClass,
                                                          "createInt64",
                                                          "(J)Lnet/quasardb/qdb/ts/Value;");

  return env->CallStaticObjectMethod(valueClass, constructor, input.payload.int64_.value);
}

/* static */ jobject
qdb::value::_from_native_double(JNIEnv * env, qdb_point_result_t const & input) {
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jmethodID constructor = qdb::jni::lookup_staticMethodID(env, valueClass,
                                                          "createDouble",
                                                          "(D)Lnet/quasardb/qdb/ts/Value;");

  return env->CallStaticObjectMethod(valueClass, constructor, input.payload.double_.value);
}

/* static */ jobject
qdb::value::_from_native_timestamp(JNIEnv * env, qdb_point_result_t const & input) {
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jmethodID constructor = qdb::jni::lookup_staticMethodID(env, valueClass,
                                                          "createTimestamp",
                                                          "(Lnet/quasardb/qdb/ts/Timespec;)Lnet/quasardb/qdb/ts/Value;");

  jobject timestamp;
  nativeToTimespec(env, input.payload.timestamp.value, &timestamp);

  return env->CallStaticObjectMethod(valueClass, constructor, timestamp);
}

/* static */ jobject
qdb::value::_from_native_blob(JNIEnv * env, qdb_point_result_t const & input) {
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jmethodID constructor = qdb::jni::lookup_staticMethodID(env, valueClass,
                                                          "createSafeBlob",
                                                          "(Ljava/nio/ByteBuffer;)Lnet/quasardb/qdb/ts/Value;");

  jobject byteBuffer;
  nativeToByteBuffer(env,
                     input.payload.blob.content,
                     input.payload.blob.content_length,
                     &byteBuffer);
  assert(byteBuffer != NULL);

  jobject tmp = env->CallStaticObjectMethod(valueClass, constructor, byteBuffer);
  env->DeleteLocalRef(byteBuffer);
  return tmp;
}

/* static */ jobject
qdb::value::_from_native_null(JNIEnv * env) {
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jmethodID constructor = qdb::jni::lookup_staticMethodID(env, valueClass,
                                                          "createNull",
                                                          "()Lnet/quasardb/qdb/ts/Value;");

  return env->CallStaticObjectMethod(valueClass, constructor);
}
