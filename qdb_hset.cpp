#include "net_quasardb_qdb_jni_qdb.h"

#include "helpers.h"
#include <qdb/hset.h>

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_hset_1insert(JNIEnv *env, jclass thisClass, jlong handle,
                                           jstring alias, jobject content) {
  const void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  return qdb_hset_insert((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr, contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_hset_1erase(JNIEnv *env, jclass thisClass, jlong handle,
                                          jstring alias, jobject content) {
  const void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  return qdb_hset_erase((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr, contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_hset_1contains(JNIEnv *env, jclass thisClass, jlong handle,
                                             jstring alias, jobject content) {
  const void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  return qdb_hset_contains((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr,
                           contentSize);
}
