#include "../env.h"
#include "../exception.h"
#include "../string.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/tag.h>
#include <stdlib.h>

namespace jni = qdb::jni;

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_attach_1tag(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jstring tag)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        return jni::exception::throw_if_error(
            handle_, qdb_attach_tag(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias),
                         qdb::jni::string::get_chars_utf8(env, handle_, tag)));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_has_1tag(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jstring tag)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        return jni::exception::throw_if_error(
            handle_, qdb_has_tag(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias),
                         qdb::jni::string::get_chars_utf8(env, handle_, tag)));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_detach_1tag(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jstring tag)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        return jni::exception::throw_if_error(
            handle_, qdb_detach_tag(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias),
                         qdb::jni::string::get_chars_utf8(env, handle_, tag)));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_get_1tags(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jobject tags)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        const char ** nativeTags = NULL;
        size_t tagCount          = 0;
        jni::exception::throw_if_error(
            handle_, qdb_get_tags(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias),
                         &nativeTags, &tagCount));
        setStringArray(env, tags, nativeTags, tagCount);
        qdb_release(handle_, nativeTags);
        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1begin(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jobject iterator)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        qdb_const_tag_iterator_t * nativeIterator = new qdb_const_tag_iterator_t;
        qdb_error_t err                           = jni::exception::throw_if_error(
                                      handle_, qdb_tag_iterator_begin(handle_,
                                                   qdb::jni::string::get_chars_utf8(env, handle_, alias), nativeIterator));
        if (QDB_SUCCESS(err))
        {
            setLong(env, iterator, (jlong)nativeIterator);
        }
        else
        {
            delete nativeIterator;
            setLong(env, iterator, 0);
        }
        return err;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1next(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong iterator)
{
    qdb::jni::env env(jniEnv);
    try
    {
        return qdb_tag_iterator_next((qdb_const_tag_iterator_t *)iterator);
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1close(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong iterator)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_error_t err = qdb_tag_iterator_close((qdb_const_tag_iterator_t *)iterator);
        delete (qdb_const_tag_iterator_t *)iterator;
        return err;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jstring JNICALL Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1alias(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong iterator)
{
    qdb::jni::env env(jniEnv);

    try
    {
        if (iterator)
        {
            return env.instance().NewStringUTF(((qdb_const_tag_iterator_t *)iterator)->alias);
        }
        else
        {
            return NULL;
        }
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return NULL;
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1type(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong iterator)
{
    qdb::jni::env env(jniEnv);

    try
    {
        if (iterator)
        {
            return ((qdb_const_tag_iterator_t *)iterator)->type;
        }
        else
        {
            return qdb_entry_uninitialized;
        }
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}
