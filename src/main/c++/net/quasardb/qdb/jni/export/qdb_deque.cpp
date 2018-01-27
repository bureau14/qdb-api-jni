#include <qdb/deque.h>
#include "net_quasardb_qdb_jni_qdb.h"

#include "../env.h"
#include "../string.h"
#include "../util/helpers.h"

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1size(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jobject size) {
  qdb::jni::env env(jniEnv);

  qdb_size_t nativeSize;
  qdb_error_t err = qdb_deque_size((qdb_handle_t)handle, qdb::jni::string::get_chars(env, alias), &nativeSize);
  setLong(env, size, nativeSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1get_1at(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                             jstring alias, jlong index, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err = qdb_deque_get_at((qdb_handle_t)handle, qdb::jni::string::get_chars(env, alias), index,
                                     &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1set_1at(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                             jstring alias, jlong index, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = env.instance().GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
  return qdb_deque_set_at((qdb_handle_t)handle, qdb::jni::string::get_chars(env, alias), index, contentPtr,
                          contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1push_1front(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                 jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = env.instance().GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
  return qdb_deque_push_front((qdb_handle_t)handle, qdb::jni::string::get_chars(env, alias), contentPtr,
                              contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1push_1back(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = env.instance().GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
  return qdb_deque_push_back((qdb_handle_t)handle, qdb::jni::string::get_chars(env, alias), contentPtr,
                             contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1pop_1front(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err = qdb_deque_pop_front((qdb_handle_t)handle, qdb::jni::string::get_chars(env, alias),
                                        &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1pop_1back(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                               jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err = qdb_deque_pop_back((qdb_handle_t)handle, qdb::jni::string::get_chars(env, alias),
                                       &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1front(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                           jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err =
      qdb_deque_front((qdb_handle_t)handle, qdb::jni::string::get_chars(env, alias), &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1back(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err =
      qdb_deque_back((qdb_handle_t)handle, qdb::jni::string::get_chars(env, alias), &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}
