#include "../env.h"
#include "../exception.h"
#include "../guard/qdb_resource.h"
#include "../string.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/string.h>

namespace jni = qdb::jni;

JNIEXPORT jstring JNICALL Java_net_quasardb_qdb_jni_qdb_string_1get(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        auto alias_          = qdb::jni::string::get_chars_utf8(env, alias);

        char const * xs{nullptr};
        qdb_size_t n{0};

        jni::exception::throw_if_error(handle_, qdb_string_get(handle_, alias_, &xs, &n));

        return qdb::jni::string::create_utf8(env, xs, n).release();
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return nullptr;
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_string_1put(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jstring content)

{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        auto alias_          = qdb::jni::string::get_chars_utf8(env, alias);
        auto content_        = qdb::jni::string::get_chars_utf8(env, content);

        jni::exception::throw_if_error(
            handle_, qdb_string_put(handle_, alias_, content_, content_.size(), -1));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT jboolean JNICALL Java_net_quasardb_qdb_jni_qdb_string_1update(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jstring content)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        auto alias_          = qdb::jni::string::get_chars_utf8(env, alias);
        auto content_        = qdb::jni::string::get_chars_utf8(env, content);

        qdb_error_t err = jni::exception::throw_if_error(
            handle_, qdb_string_update(handle_, alias_, content_, content_.size(), -1));

        return err == qdb_e_ok_created;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return false;
    }
}
