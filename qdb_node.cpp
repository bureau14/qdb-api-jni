#include "net_quasardb_qdb_jni_qdb.h"

#include "helpers.h"
#include <qdb/node.h>

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    node_status
 * Signature: (JLjava/lang/String;Lnet/quasardb/qdb/jni/StringReference;)I
 */
JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_node_1status(JNIEnv *env, jclass thisClass, jlong handle, jstring uri,
                                           jobject content) {
  const char *nativeContent = NULL;
  qdb_size_t contentLength = 0;
  qdb_error_t err = qdb_node_status((qdb_handle_t)handle, StringUTFChars(env, uri), &nativeContent,
                                    &contentLength);
  setString(env, content, nativeContent);
  return err;
}

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    node_config
 * Signature: (JLjava/lang/String;Lnet/quasardb/qdb/jni/StringReference;)I
 */
JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_node_1config(JNIEnv *env, jclass thisClass, jlong handle, jstring uri,
                                           jobject content) {
  const char *nativeContent = NULL;
  qdb_size_t contentLength = 0;
  qdb_error_t err = qdb_node_config((qdb_handle_t)handle, StringUTFChars(env, uri), &nativeContent,
                                    &contentLength);
  setString(env, content, nativeContent);
  return err;
}

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    node_topology
 * Signature: (JLjava/lang/String;Lnet/quasardb/qdb/jni/StringReference;)I
 */
JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_node_1topology(JNIEnv *env, jclass thisClass, jlong handle,
                                             jstring uri, jobject content) {
  const char *nativeContent = NULL;
  qdb_size_t contentLength = 0;
  qdb_error_t err = qdb_node_topology((qdb_handle_t)handle, StringUTFChars(env, uri),
                                      &nativeContent, &contentLength);
  setString(env, content, nativeContent);
  return err;
}

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    node_stop
 * Signature: (JLjava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_node_1stop(JNIEnv *env, jclass thisClass, jlong handle, jstring uri,
                                         jstring reason) {
  const char *nativeContent = NULL;
  qdb_size_t contentLength = 0;
  return qdb_node_stop((qdb_handle_t)handle, StringUTFChars(env, uri), StringUTFChars(env, reason));
}

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    get_location
 * Signature:
 * (JLjava/lang/String;Lnet/quasardb/qdb/jni/StringReference;Lnet/quasardb/qdb/jni/IntReference;)I
 */
JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1location(JNIEnv *env, jclass thisClass, jlong handle,
                                            jstring alias, jobject address, jobject port) {
  qdb_remote_node_t node;
  qdb_error_t err = qdb_get_location((qdb_handle_t)handle, StringUTFChars(env, alias), &node);
  setString(env, address, node.address);
  setInteger(env, address, node.port);
  return err;
}
