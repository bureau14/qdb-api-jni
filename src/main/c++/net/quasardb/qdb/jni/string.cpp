#include "string.h"
#include "env.h"
#include "exception.h"
#include "object.h"
#include <qdb/client.h>

/* static */ qdb::jni::guard::string_utf8 qdb::jni::string::get_chars_utf8(
    qdb::jni::env & env, qdb_handle_t handle, jstring str)
{
    assert(str != NULL);

    return qdb::jni::guard::string_utf8(env, handle, str,
        env.instance().GetStringUTFChars(str, NULL), env.instance().GetStringUTFLength(str));
}

/* static */ qdb::jni::guard::local_ref<jstring> qdb::jni::string::create(
    jni::env & env, qdb_handle_t /* handle */, char const * str, jsize len)
{
    return qdb::jni::guard::local_ref<jstring>(
        env, env.instance().NewString((const jchar *)str, len));
}

/* static */ qdb::jni::guard::local_ref<jstring> qdb::jni::string::create_utf8(
    jni::env & env, qdb_handle_t /* handle */, char const * str)
{
    return qdb::jni::guard::local_ref<jstring>(env, env.instance().NewStringUTF(str));
}

/* static */ qdb::jni::guard::local_ref<jstring> qdb::jni::string::create_utf8(
    jni::env & env, qdb_handle_t handle, char const * str, qdb_size_t len)
{
    // quasardb doesn't null-terminate strings, JNI does not accept explicit
    // string length and assumes null-terminated UTF-8 strings. As such we're
    // gonna heap allocate it, just to null-terminate it.

    std::string copy{str, len};

    return qdb::jni::guard::local_ref<jstring>(env, env.instance().NewStringUTF(copy.c_str()));
}

/* static */ qdb::jni::guard::local_ref<jobjectArray> qdb::jni::string::create_array(
    jni::env & env, jsize size)
{
    return jni::object::create_array(env, size, jni::string::lookup_class(env));
}
