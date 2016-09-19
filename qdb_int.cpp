#include "net_quasardb_qdb_jni_qdb.h"

#include "helpers.h"
#include <qdb/integer.h>

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1put(JNIEnv *env, jclass thisClass, jlong handle, jstring alias,
                                       jlong value, jlong expiry) {
  return qdb_int_put((qdb_handle_t)handle, StringUTFChars(env, alias), value, expiry);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1update(JNIEnv *env, jclass thisClass, jlong handle,
                                          jstring alias, jlong value, jlong expiry) {
  return qdb_int_update((qdb_handle_t)handle, StringUTFChars(env, alias), value, expiry);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1get(JNIEnv *env, jclass thisClass, jlong handle, jstring alias,
                                       jobject value) {
  qdb_int_t nativeValue;
  qdb_error_t err = qdb_int_get((qdb_handle_t)handle, StringUTFChars(env, alias), &nativeValue);
  setLong(env, value, nativeValue);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1add(JNIEnv *env, jclass thisClass, jlong handle, jstring alias,
                                       jlong addend, jobject result) {
  qdb_int_t nativeResult;
  qdb_error_t err =
      qdb_int_add((qdb_handle_t)handle, StringUTFChars(env, alias), addend, &nativeResult);
  setLong(env, result, nativeResult);
  return err;
}
