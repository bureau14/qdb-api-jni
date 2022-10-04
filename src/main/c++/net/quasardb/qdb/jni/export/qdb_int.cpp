#include "../env.h"
#include "../exception.h"
#include "../string.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/integer.h>

namespace jni = qdb::jni;

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_int_1put(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jlong value, jlong expiry)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        return jni::exception::throw_if_error(
            handle_, qdb_int_put(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias),
                         value, expiry));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_int_1update(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jlong value, jlong expiry)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        return jni::exception::throw_if_error(
            handle_, qdb_int_update(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias),
                         value, expiry));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_int_1get(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jobject value)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_int_t nativeValue;
        jni::exception::throw_if_error(
            handle_, qdb_int_get(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias),
                         &nativeValue));
        setLong(env, value, nativeValue);
        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_int_1add(JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jstring alias,
    jlong addend,
    jobject result)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_int_t nativeResult;
        jni::exception::throw_if_error(
            handle_, qdb_int_add(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias),
                         addend, &nativeResult));
        setLong(env, result, nativeResult);
        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}
