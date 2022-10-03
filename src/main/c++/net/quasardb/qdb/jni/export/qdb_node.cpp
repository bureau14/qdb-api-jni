#include "../env.h"
#include "../exception.h"
#include "../string.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/node.h>

namespace jni = qdb::jni;

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    node_status
 * Signature: (JLjava/lang/String;Lnet/quasardb/qdb/jni/StringReference;)I
 */
JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_node_1status(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring uri, jobject content)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_       = reinterpret_cast<qdb_handle_t>(handle);
        const char * nativeContent = NULL;
        qdb_size_t contentLength   = 0;
        jni::exception::throw_if_error(
            handle_, qdb_node_status(handle_, qdb::jni::string::get_chars_utf8(env, handle_, uri),
                         &nativeContent, &contentLength));
        setString(env, content, nativeContent);
        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    node_config
 * Signature: (JLjava/lang/String;Lnet/quasardb/qdb/jni/StringReference;)I
 */
JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_node_1config(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring uri, jobject content)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_       = reinterpret_cast<qdb_handle_t>(handle);
        const char * nativeContent = NULL;
        qdb_size_t contentLength   = 0;
        jni::exception::throw_if_error(
            handle_, qdb_node_config(handle_, qdb::jni::string::get_chars_utf8(env, handle_, uri),
                         &nativeContent, &contentLength));
        setString(env, content, nativeContent);
        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    node_topology
 * Signature: (JLjava/lang/String;Lnet/quasardb/qdb/jni/StringReference;)I
 */
JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_node_1topology(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring uri, jobject content)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_       = reinterpret_cast<qdb_handle_t>(handle);
        const char * nativeContent = NULL;
        qdb_size_t contentLength   = 0;
        jni::exception::throw_if_error(
            handle_, qdb_node_topology(handle_, qdb::jni::string::get_chars_utf8(env, handle_, uri),
                         &nativeContent, &contentLength));
        setString(env, content, nativeContent);
        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    node_stop
 * Signature: (JLjava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_node_1stop(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring uri, jstring reason)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        return jni::exception::throw_if_error(
            handle_, qdb_node_stop(handle_, qdb::jni::string::get_chars_utf8(env, handle_, uri),
                         qdb::jni::string::get_chars_utf8(env, handle_, reason)));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

/*
 * Class:     net_quasardb_qdb_jni_qdb
 * Method:    get_location
 * Signature:
 * (JLjava/lang/String;Lnet/quasardb/qdb/jni/StringReference;Lnet/quasardb/qdb/jni/IntReference;)I
 */
JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_get_1location(JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jstring alias,
    jobject address,
    jobject port)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_remote_node_t node;
        jni::exception::throw_if_error(
            handle_, qdb_get_location(
                         handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias), &node));
        setString(env, address, node.address);
        setInteger(env, port, node.port);
        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}
