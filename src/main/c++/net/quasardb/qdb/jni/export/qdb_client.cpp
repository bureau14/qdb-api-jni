#include <qdb/client.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../log.h"
#include "../env.h"
#include "../string.h"
#include "../util/helpers.h"

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_build(JNIEnv * jniEnv, jclass /*thisClass*/) {
  qdb::jni::env env(jniEnv);

  return env.instance().NewStringUTF(qdb_build());
}

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_version(JNIEnv * jniEnv, jclass /*thisClass*/) {
  qdb::jni::env env(jniEnv);

  return env.instance().NewStringUTF(qdb_version());
}

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_error_1message(JNIEnv * jniEnv, jclass /*thisClass*/, jint err) {
  qdb::jni::env env(jniEnv);

  return env.instance().NewStringUTF(qdb_error((qdb_error_t)err));
}

JNIEXPORT jlong JNICALL
Java_net_quasardb_qdb_jni_qdb_open_1tcp(JNIEnv * /*env*/, jclass /*thisClass*/) {
  return (jlong)qdb_open_tcp();
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_connect(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring uri) {
  qdb::jni::env env(jniEnv);

  qdb::jni::log::ensure_callback(env);

  return qdb_connect((qdb_handle_t)handle,
                     qdb::jni::string::get_chars_utf8(env, uri));
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_secure_1connect(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring uri, jobject securityOptions) {
  qdb::jni::env env(jniEnv);

  qdb::jni::log::ensure_callback(env);

  qdb_error_t err;
  jclass objectClass;
  jfieldID userNameField, userPrivateKeyField, clusterPublicKeyField;

  objectClass = env.instance().GetObjectClass(securityOptions);
  userNameField = env.instance().GetFieldID(objectClass, "user_name", "Ljava/lang/String;");
  userPrivateKeyField = env.instance().GetFieldID(objectClass, "user_private_key", "Ljava/lang/String;");
  clusterPublicKeyField = env.instance().GetFieldID(objectClass, "cluster_public_key", "Ljava/lang/String;");

  jstring userName = (jstring)env.instance().GetObjectField(securityOptions, userNameField);
  jstring userPrivateKey = (jstring)env.instance().GetObjectField(securityOptions, userPrivateKeyField);
  jstring clusterPublicKey = (jstring)env.instance().GetObjectField(securityOptions, clusterPublicKeyField);

  err = qdb_option_set_cluster_public_key((qdb_handle_t)handle,
                                          qdb::jni::string::get_chars_utf8(env, clusterPublicKey));
  if (QDB_FAILURE(err)) {
    return err;
  }

  err = qdb_option_set_user_credentials((qdb_handle_t)handle,
                                        qdb::jni::string::get_chars_utf8(env, userName),
                                        qdb::jni::string::get_chars_utf8(env, userPrivateKey));
  if (QDB_FAILURE(err)) {
    return err;
  }

  return qdb_connect((qdb_handle_t)handle,
                     qdb::jni::string::get_chars_utf8(env, uri));
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_close(JNIEnv * /*env*/, jclass /*thisClass*/, jlong handle) {
  return qdb_close((qdb_handle_t)handle);
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_release(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                      jobject buffer) {
  qdb::jni::env env(jniEnv);

  void *ptr = env.instance().GetDirectBufferAddress(buffer);
  qdb_release((qdb_handle_t)handle, ptr);
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
Java_net_quasardb_qdb_jni_qdb_remove(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias) {
  qdb::jni::env env(jniEnv);

  return qdb_remove((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias));
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1type(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                        jobject type) {
  qdb::jni::env env(jniEnv);
  qdb_entry_metadata_t metadata;
  qdb_error_t err = qdb_get_metadata((qdb_handle_t)handle,

                                     (alias == NULL
                                      ? (char const *)(NULL)
                                      : qdb::jni::string::get_chars_utf8(env, alias)), &metadata);
  setInteger(env, type, metadata.type);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1metadata(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias,
                                            jobject meta) {
  qdb::jni::env env(jniEnv);

  void *metaPtr = env.instance().GetDirectBufferAddress(meta);
  qdb_size_t metaSize = (qdb_size_t)env.instance().GetDirectBufferCapacity(meta);
  if (metaSize != sizeof(qdb_entry_metadata_t)) return qdb_e_invalid_argument;

  return qdb_get_metadata((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), (qdb_entry_metadata_t *)metaPtr);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_expires_1at(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jlong expiry) {
  qdb::jni::env env(jniEnv);

  return qdb_expires_at((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), expiry);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1expiry_1time(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                jstring alias, jobject expiry) {
  qdb::jni::env env(jniEnv);

  qdb_entry_metadata_t metadata;
  qdb_error_t err = qdb_get_metadata((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), &metadata);
  setLong(env, expiry, static_cast<qdb_time_t>(metadata.expiry_time.tv_sec) * 1000 +
    static_cast<qdb_time_t>(metadata.expiry_time.tv_nsec / 1000000ull));
  return err;
}
