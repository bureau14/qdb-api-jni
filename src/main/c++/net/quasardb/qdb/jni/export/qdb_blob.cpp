#include <qdb/blob.h>

#include "net_quasardb_qdb_jni_qdb.h"
#include "../env.h"
#include "../util/helpers.h"

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1compare_1and_1swap(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                       jstring alias, jobject newContent,
                                                       jobject comparand, jlong expiry,
                                                       jobject originalContent) {

  qdb::jni::env env(jniEnv);

  const void *newContentPtr = env.instance().GetDirectBufferAddress(newContent);
  qdb_size_t newContentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(newContent);
  const void *comparandPtr = env.instance().GetDirectBufferAddress(comparand);
  qdb_size_t comparandSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(comparand);
  const void *originalContentPtr = NULL;
  qdb_size_t originalContentSize = 0;
  qdb_error_t err = qdb_blob_compare_and_swap(
      (qdb_handle_t)handle, StringUTFChars(env, alias), newContentPtr, newContentSize, comparandPtr,
      comparandSize, expiry, &originalContentPtr, &originalContentSize);
  setByteBuffer(env, originalContent, originalContentPtr, originalContentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1get(JNIEnv *jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                        jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err =
      qdb_blob_get((qdb_handle_t)handle, StringUTFChars(env, alias), &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1get_1and_1remove(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                     jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = NULL;
  qdb_size_t contentSize = 0;
  qdb_error_t err = qdb_blob_get_and_remove((qdb_handle_t)handle, StringUTFChars(env, alias),
                                            &contentPtr, &contentSize);
  setByteBuffer(env, content, contentPtr, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1get_1and_1update(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                     jstring alias, jobject newContent,
                                                     jlong expiry, jobject originalContent) {
  qdb::jni::env env(jniEnv);

  const void *newContentPtr = env.instance().GetDirectBufferAddress(newContent);
  qdb_size_t newContentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(newContent);
  const void *originalContentPtr = NULL;
  qdb_size_t originalContentSize = 0;
  qdb_error_t err =
      qdb_blob_get_and_update((qdb_handle_t)handle, StringUTFChars(env, alias), newContentPtr,
                              newContentSize, expiry, &originalContentPtr, &originalContentSize);
  setByteBuffer(env, originalContent, originalContentPtr, originalContentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1put(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                        jobject content, jlong expiry) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = env.instance().GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
  return qdb_blob_put((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr, contentSize,
                      expiry);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1remove_1if(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                               jstring alias, jobject comparand) {
  qdb::jni::env env(jniEnv);

  const void *comparandPtr = env.instance().GetDirectBufferAddress(comparand);
  qdb_size_t comparandSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(comparand);
  return qdb_blob_remove_if((qdb_handle_t)handle, StringUTFChars(env, alias), comparandPtr,
                            comparandSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1update(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                           jstring alias, jobject content, jlong expiry) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = env.instance().GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
  return qdb_blob_update((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr, contentSize,
                         expiry);
}
