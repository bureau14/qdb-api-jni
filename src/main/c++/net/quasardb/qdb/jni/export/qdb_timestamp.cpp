#include "../adapt/timespec.h"
#include "../env.h"
#include "../exception.h"
#include "../string.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/timestamp.h>

namespace jni = qdb::jni;

JNIEXPORT jobject JNICALL Java_net_quasardb_qdb_jni_qdb_timestamp_1get(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        auto alias_          = qdb::jni::string::get_chars_utf8(env, alias);

        qdb_timespec_t ret;

        jni::exception::throw_if_error(handle_, qdb_timestamp_get(handle_, alias_, &ret));

        return jni::adapt::timespec::to_java(env, ret).release();
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return nullptr;
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_timestamp_1put(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jobject value)

{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_  = reinterpret_cast<qdb_handle_t>(handle);
        auto alias_           = qdb::jni::string::get_chars_utf8(env, alias);
        qdb_timespec_t value_ = jni::adapt::timespec::to_qdb(env, value);

        jni::exception::throw_if_error(handle_, qdb_timestamp_put(handle_, alias_, &value_, -1));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT jboolean JNICALL Java_net_quasardb_qdb_jni_qdb_timestamp_1update(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jobject value)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_  = reinterpret_cast<qdb_handle_t>(handle);
        auto alias_           = qdb::jni::string::get_chars_utf8(env, alias);
        qdb_timespec_t value_ = jni::adapt::timespec::to_qdb(env, value);

        qdb_error_t err = jni::exception::throw_if_error(
            handle_, qdb_timestamp_update(handle_, alias_, &value_, -1));

        return err == qdb_e_ok_created;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return false;
    }
}
