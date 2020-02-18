#include <qdb/integer.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../env.h"
#include "../string.h"
#include "../util/helpers.h"
#include "../exception.h"

namespace jni = qdb::jni;

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1put(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                       jlong value, jlong expiry) {
  qdb::jni::env env(jniEnv);
  try {
    return jni::exception::throw_if_error((qdb_handle_t)handle,
                                          qdb_int_put((qdb_handle_t)handle,
                                                      qdb::jni::string::get_chars_utf8(env, alias),
                                                      value, expiry));
  } catch (jni::exception const & e) {
    e.throw_new(env);
    return e.error();
  }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1update(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jlong value, jlong expiry) {
  qdb::jni::env env(jniEnv);
  try {
    return jni::exception::throw_if_error((qdb_handle_t)handle,
                                          qdb_int_update((qdb_handle_t)handle,
                                                         qdb::jni::string::get_chars_utf8(env,
                                                                                          alias),
                                                         value,
                                                         expiry));
  } catch (jni::exception const & e) {
    e.throw_new(env);
    return e.error();
  }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1get(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                       jobject value) {
  qdb::jni::env env(jniEnv);

  try {
    qdb_int_t nativeValue;
    jni::exception::throw_if_error((qdb_handle_t)handle,
                                   qdb_int_get((qdb_handle_t)handle,
                                               qdb::jni::string::get_chars_utf8(env, alias),
                                               &nativeValue));
    setLong(env, value, nativeValue);
    return qdb_e_ok;
  } catch (jni::exception const & e) {
    e.throw_new(env);
    return e.error();
  }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_int_1add(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                       jlong addend, jobject result) {
  qdb::jni::env env(jniEnv);

  try {
    qdb_int_t nativeResult;
    jni::exception::throw_if_error((qdb_handle_t)handle,
                                   qdb_int_add((qdb_handle_t)handle,
                                               qdb::jni::string::get_chars_utf8(env, alias),
                                               addend,
                                               &nativeResult));
    setLong(env, result, nativeResult);
    return qdb_e_ok;
  } catch (jni::exception const & e) {
    e.throw_new(env);
    return e.error();
  }
}
