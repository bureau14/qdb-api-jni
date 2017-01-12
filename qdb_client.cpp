#include "net_quasardb_qdb_jni_qdb.h"

#include "helpers.h"
#include <qdb/client.h>

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_build(JNIEnv *env, jclass /*thisClass*/) {
  return env->NewStringUTF(qdb_build());
}

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_version(JNIEnv *env, jclass /*thisClass*/) {
  return env->NewStringUTF(qdb_version());
}

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_error_1message(JNIEnv *env, jclass /*thisClass*/, jint err) {
  return env->NewStringUTF(qdb_error((qdb_error_t)err));
}

JNIEXPORT jlong JNICALL
Java_net_quasardb_qdb_jni_qdb_open_1tcp(JNIEnv * /*env*/, jclass /*thisClass*/) {
  return (jlong)qdb_open_tcp();
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_connect(JNIEnv *env, jclass /*thisClass*/, jlong handle, jstring uri) {
  StringUTFChars nativeUri(env, uri);
  return qdb_connect((qdb_handle_t)handle, nativeUri);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_close(JNIEnv * /*env*/, jclass /*thisClass*/, jlong handle) {
  return qdb_close((qdb_handle_t)handle);
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_free_1buffer(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                           jobject buffer) {
  void *ptr = env->GetDirectBufferAddress(buffer);
  qdb_free_buffer((qdb_handle_t)handle, ptr);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_option_1set_1timeout(JNIEnv * /*env*/, jclass /*thisClass*/, jlong handle,
                                                   jint timeout) {
  return qdb_option_set_timeout((qdb_handle_t)handle, timeout);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_purge_1all(JNIEnv * /*env*/, jclass /*thisClass*/, jlong handle,
                                         jint timeout) {
  return qdb_purge_all((qdb_handle_t)handle, timeout);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_trim_1all(JNIEnv * /*env*/, jclass /*thisClass*/, jlong handle, jint timeout) {
  return qdb_trim_all((qdb_handle_t)handle, timeout);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_remove(JNIEnv *env, jclass /*thisClass*/, jlong handle, jstring alias) {
  return qdb_remove((qdb_handle_t)handle, StringUTFChars(env, alias));
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1type(JNIEnv *env, jclass /*thisClass*/, jlong handle, jstring alias,
                                        jobject type) {
  qdb_entry_metadata_t metadata;
  qdb_error_t err = qdb_get_metadata((qdb_handle_t)handle, StringUTFChars(env, alias), &metadata);
  setInteger(env, type, metadata.type);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_expires_1at(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jlong expiry) {
  return qdb_expires_at((qdb_handle_t)handle, StringUTFChars(env, alias), expiry);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1expiry_1time(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                                jstring alias, jobject expiry) {
  qdb_entry_metadata_t metadata;
  qdb_error_t err = qdb_get_metadata((qdb_handle_t)handle, StringUTFChars(env, alias), &metadata);
  setLong(env, expiry, static_cast<qdb_time_t>(metadata.expiry_time.tv_sec) * 1000 +
    static_cast<qdb_time_t>(metadata.expiry_time.tv_nsec / 1000000ull));
  return err;
}
