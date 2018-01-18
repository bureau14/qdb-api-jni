#include "net_quasardb_qdb_jni_qdb.h"

#include "helpers.h"
#include <qdb/deque.h>

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1size(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jobject size) {
  qdb_size_t nativeSize;
  qdb_error_t err = qdb_deque_size((qdb_handle_t)handle, StringUTFChars(env, alias), &nativeSize);
  setLong(env, size, nativeSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1get_1at(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                             jstring alias, jlong index, jobject content) {
  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err = qdb_deque_get_at((qdb_handle_t)handle, StringUTFChars(env, alias), index,
                                     &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1set_1at(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                             jstring alias, jlong index, jobject content) {
  const void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  return qdb_deque_set_at((qdb_handle_t)handle, StringUTFChars(env, alias), index, contentPtr,
                          contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1push_1front(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                                 jstring alias, jobject content) {
  const void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  return qdb_deque_push_front((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr,
                              contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1push_1back(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                                jstring alias, jobject content) {
  const void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  return qdb_deque_push_back((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr,
                             contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1pop_1front(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                                jstring alias, jobject content) {
  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err = qdb_deque_pop_front((qdb_handle_t)handle, StringUTFChars(env, alias),
                                        &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1pop_1back(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                               jstring alias, jobject content) {
  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err = qdb_deque_pop_back((qdb_handle_t)handle, StringUTFChars(env, alias),
                                       &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1front(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                           jstring alias, jobject content) {
  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err =
      qdb_deque_front((qdb_handle_t)handle, StringUTFChars(env, alias), &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_deque_1back(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jobject content) {
  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err =
      qdb_deque_back((qdb_handle_t)handle, StringUTFChars(env, alias), &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}
