#include "net_quasardb_qdb_jni_qdb.h"

#include "helpers.h"
#include <qdb/stream.h>

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_stream_1open(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                           jstring alias, jint mode, jobject stream) {
  qdb_stream_t nativeStream;
  qdb_error_t err = qdb_stream_open((qdb_handle_t)handle, StringUTFChars(env, alias),
                                    (qdb_stream_mode_t)mode, &nativeStream);
  setLong(env, stream, (jlong)nativeStream);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_stream_1close(JNIEnv * /*env*/, jclass /*thisClass*/, jlong stream) {
  return qdb_stream_close((qdb_stream_t)stream);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_stream_1read(JNIEnv * env, jclass /*thisClass*/, jlong stream,
                                           jobject content, jobject bytesRead) {
  void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  qdb_error_t err = qdb_stream_read((qdb_stream_t)stream, contentPtr, &contentSize);
  setLong(env, bytesRead, contentSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_stream_1write(JNIEnv * env, jclass /*thisClass*/, jlong stream,
                                            jobject content) {
  const void *contentPtr = env->GetDirectBufferAddress(content);
  qdb_size_t contentSize = (qdb_size_t)env->GetDirectBufferCapacity(content);
  return qdb_stream_write((qdb_stream_t)stream, contentPtr, contentSize);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_stream_1size(JNIEnv * env, jclass /*thisClass*/, jlong stream,
                                           jobject size) {
  qdb_stream_size_t nativeSize = 0;
  qdb_error_t err = qdb_stream_size((qdb_stream_t)stream, &nativeSize);
  setLong(env, size, nativeSize);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_stream_1getpos(JNIEnv * env, jclass /*thisClass*/, jlong stream,
                                             jobject position) {
  qdb_stream_size_t nativePosition = 0;
  qdb_error_t err = qdb_stream_getpos((qdb_stream_t)stream, &nativePosition);
  setLong(env, position, nativePosition);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_stream_1setpos(JNIEnv * /*env*/, jclass /*thisClass*/, jlong stream,
                                             jlong position) {
  return qdb_stream_setpos((qdb_stream_t)stream, position);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_stream_1truncate(JNIEnv * /*env*/, jclass /*thisClass*/, jlong stream,
                                               jlong position) {
  return qdb_stream_truncate((qdb_stream_t)stream, position);
}
