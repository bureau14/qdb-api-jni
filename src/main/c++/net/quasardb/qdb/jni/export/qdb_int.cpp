#include <qdb/integer.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../env.h"
#include "../string.h"
#include "../util/helpers.h"

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1put(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                       jlong value, jlong expiry) {
  qdb::jni::env env(jniEnv);

  return qdb_int_put((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), value, expiry);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1update(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jlong value, jlong expiry) {
  qdb::jni::env env(jniEnv);

  return qdb_int_update((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), value, expiry);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1get(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                       jobject value) {
  qdb::jni::env env(jniEnv);

  qdb_int_t nativeValue;
  qdb_error_t err = qdb_int_get((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), &nativeValue);
  setLong(env, value, nativeValue);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1add(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                       jlong addend, jobject result) {
  qdb::jni::env env(jniEnv);

  qdb_int_t nativeResult;
  qdb_error_t err =
      qdb_int_add((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), addend, &nativeResult);
  setLong(env, result, nativeResult);
  return err;
}
