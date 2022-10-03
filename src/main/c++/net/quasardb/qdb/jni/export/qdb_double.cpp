#include "../env.h"
#include "../exception.h"
#include "../string.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/double.h>

namespace jni = qdb::jni;

JNIEXPORT jdouble JNICALL Java_net_quasardb_qdb_jni_qdb_double_1get(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        auto alias_          = qdb::jni::string::get_chars_utf8(env, handle_, alias);

        jdouble ret;

        jni::exception::throw_if_error(handle_, qdb_double_get(handle_, alias_, &ret));

        return ret;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return 0.0;
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_double_1put(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jdouble value)

{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        auto alias_          = qdb::jni::string::get_chars_utf8(env, handle_, alias);

        jni::exception::throw_if_error(handle_, qdb_double_put(handle_, alias_, value, -1));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT jboolean JNICALL Java_net_quasardb_qdb_jni_qdb_double_1update(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jdouble value)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        auto alias_          = qdb::jni::string::get_chars_utf8(env, handle_, alias);

        qdb_error_t err =
            jni::exception::throw_if_error(handle_, qdb_double_update(handle_, alias_, value, -1));

        return err == qdb_e_ok_created;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return false;
    }
}
