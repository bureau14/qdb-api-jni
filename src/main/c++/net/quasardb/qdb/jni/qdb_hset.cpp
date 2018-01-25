#include "net_quasardb_qdb_jni_qdb.h"

#include "env.h"
#include "helpers.h"
#include <qdb/hset.h>

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_hset_1insert(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                           jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = env.instance().GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
  return qdb_hset_insert((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr, contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_hset_1erase(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = env.instance().GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
  return qdb_hset_erase((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr, contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_hset_1contains(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                             jstring alias, jobject content) {
  qdb::jni::env env(jniEnv);

  const void *contentPtr = env.instance().GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
  return qdb_hset_contains((qdb_handle_t)handle, StringUTFChars(env, alias), contentPtr,
                           contentSize);
}
