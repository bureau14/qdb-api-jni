#include "net_quasardb_qdb_jni_qdb.h"

#include "helpers.h"
#include <qdb/blob.h>

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1compare_1and_1swap(JNIEnv *env, jclass thisClass, jlong handle,
                                                       jstring alias, jobject newContent,
                                                       jobject comparand, jlong expiry,
                                                       jobject originalContent) {
  const void *newContentPtr = env->GetDirectBufferAddress(newContent);
  qdb_size_t newContentSize = (qdb_size_t)env->GetDirectBufferCapacity(newContent);
  const void *comparandPtr = env->GetDirectBufferAddress(comparand);
  qdb_size_t comparandSize = (qdb_size_t)env->GetDirectBufferCapacity(comparand);
  const void *originalContentPtr = NULL;
  qdb_size_t originalContentSize = 0;
  qdb_error_t err = qdb_blob_compare_and_swap(
      (qdb_handle_t)handle, StringUTFChars(env, alias), newContentPtr, newContentSize, comparandPtr,
      comparandSize, expiry, &originalContentPtr, &originalContentSize);
  setByteBuffer(env, originalContent, originalContentPtr, originalContentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1get(JNIEnv *env, jclass thisClass, jlong handle, jstring alias,
                                        jobject content) {
  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err =
      qdb_blob_get((qdb_handle_t)handle, StringUTFChars(env, alias), &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1get_1and_1remove(JNIEnv *env, jclass thisClass, jlong handle,
                                                     jstring alias, jobject content) {
  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err = qdb_blob_get_and_remove((qdb_handle_t)handle, StringUTFChars(env, alias),
                                            &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1get_1and_1update(JNIEnv *env, jclass thisClass, jlong handle,
                                                     jstring alias, jobject newContent,
                                                     jlong expiry, jobject originalContent) {
  const void *newContentPtr = env->GetDirectBufferAddress(newContent);
  qdb_size_t newContentSize = (qdb_size_t)env->GetDirectBufferCapacity(newContent);
  const void *originalContentPtr = NULL;
  qdb_size_t originalContentSize = 0;
  qdb_error_t err =
      qdb_blob_get_and_update((qdb_handle_t)handle, StringUTFChars(env, alias), newContentPtr,
                              newContentSize, expiry, &originalContentPtr, &originalContentSize);
  setByteBuffer(env, originalContent, originalContentPtr, originalContentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1put(JNIEnv *env, jclass thisClass, jlong handle, jstring alias,
                                        jobject content, jlong expiry) {
  const void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  return qdb_blob_put((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr, contentSize,
                      expiry);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1remove_1if(JNIEnv *env, jclass thisClass, jlong handle,
                                               jstring alias, jobject comparand) {
  const void *comparandPtr = env->GetDirectBufferAddress(comparand);
  qdb_size_t comparandSize = (qdb_size_t)env->GetDirectBufferCapacity(comparand);
  return qdb_blob_remove_if((qdb_handle_t)handle, StringUTFChars(env, alias), comparandPtr,
                            comparandSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1update(JNIEnv *env, jclass thisClass, jlong handle,
                                           jstring alias, jobject content, jlong expiry) {
  const void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  return qdb_blob_update((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr, contentSize,
                         expiry);
}
