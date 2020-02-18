#include <qdb/blob.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../env.h"
#include "../exception.h"
#include "../string.h"
#include "../util/helpers.h"

namespace jni = qdb::jni;

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1compare_1and_1swap(JNIEnv *jniEnv,
                                                       jclass /*thisClass*/,
                                                       jlong handle,
                                                       jstring alias,
                                                       jobject newContent,
                                                       jobject comparand,
                                                       jlong expiry,
                                                       jobject originalContent)
{

    qdb::jni::env env(jniEnv);
    try
    {
        const void *newContentPtr =
            env.instance().GetDirectBufferAddress(newContent);
        qdb_size_t newContentSize =
            (qdb_size_t)env.instance().GetDirectBufferCapacity(newContent);
        const void *comparandPtr =
            env.instance().GetDirectBufferAddress(comparand);
        qdb_size_t comparandSize =
            (qdb_size_t)env.instance().GetDirectBufferCapacity(comparand);
        const void *originalContentPtr = NULL;
        qdb_size_t originalContentSize = 0;
        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_blob_compare_and_swap(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, alias), newContentPtr,
                newContentSize, comparandPtr, comparandSize, expiry,
                &originalContentPtr, &originalContentSize));
        setByteBuffer(env, originalContent, originalContentPtr,
                      originalContentSize);
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1get(JNIEnv *jniEnv,
                                        jclass /*thisClass*/,
                                        jlong handle,
                                        jstring alias,
                                        jobject content)
{
    qdb::jni::env env(jniEnv);

    try
    {
        const void *contentPtr = NULL;
        qdb_size_t contentSize = 0;
        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_blob_get((qdb_handle_t)handle,
                         qdb::jni::string::get_chars_utf8(env, alias),
                         &contentPtr, &contentSize));
        setByteBuffer(env, content, contentPtr, contentSize);
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1get_1and_1remove(JNIEnv *jniEnv,
                                                     jclass /*thisClass*/,
                                                     jlong handle,
                                                     jstring alias,
                                                     jobject content)
{
    qdb::jni::env env(jniEnv);

    try
    {
        const void *contentPtr = NULL;
        qdb_size_t contentSize = 0;
        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_blob_get_and_remove(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, alias), &contentPtr,
                &contentSize));
        setByteBuffer(env, content, contentPtr, contentSize);
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1get_1and_1update(JNIEnv *jniEnv,
                                                     jclass /*thisClass*/,
                                                     jlong handle,
                                                     jstring alias,
                                                     jobject newContent,
                                                     jlong expiry,
                                                     jobject originalContent)
{
    qdb::jni::env env(jniEnv);

    try
    {
        const void *newContentPtr =
            env.instance().GetDirectBufferAddress(newContent);
        qdb_size_t newContentSize =
            (qdb_size_t)env.instance().GetDirectBufferCapacity(newContent);
        const void *originalContentPtr = NULL;
        qdb_size_t originalContentSize = 0;
        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_blob_get_and_update(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, alias), newContentPtr,
                newContentSize, expiry, &originalContentPtr,
                &originalContentSize));
        setByteBuffer(env, originalContent, originalContentPtr,
                      originalContentSize);
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1put(JNIEnv *jniEnv,
                                        jclass /*thisClass*/,
                                        jlong handle,
                                        jstring alias,
                                        jobject content,
                                        jlong expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        const void *contentPtr = env.instance().GetDirectBufferAddress(content);
        qdb_size_t contentSize =
            (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_blob_put((qdb_handle_t)handle,
                         qdb::jni::string::get_chars_utf8(env, alias),
                         contentPtr, contentSize, expiry));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1remove_1if(JNIEnv *jniEnv,
                                               jclass /*thisClass*/,
                                               jlong handle,
                                               jstring alias,
                                               jobject comparand)
{
    qdb::jni::env env(jniEnv);

    try
    {
        const void *comparandPtr =
            env.instance().GetDirectBufferAddress(comparand);
        qdb_size_t comparandSize =
            (qdb_size_t)env.instance().GetDirectBufferCapacity(comparand);
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_blob_remove_if((qdb_handle_t)handle,
                               qdb::jni::string::get_chars_utf8(env, alias),
                               comparandPtr, comparandSize));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_blob_1update(JNIEnv *jniEnv,
                                           jclass /*thisClass*/,
                                           jlong handle,
                                           jstring alias,
                                           jobject content,
                                           jlong expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        const void *contentPtr = env.instance().GetDirectBufferAddress(content);
        qdb_size_t contentSize =
            (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_blob_update((qdb_handle_t)handle,
                            qdb::jni::string::get_chars_utf8(env, alias),
                            contentPtr, contentSize, expiry));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}
