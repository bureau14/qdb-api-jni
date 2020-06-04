#include <qdb/client.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../env.h"
#include "../exception.h"
#include "../log.h"
#include "../string.h"
#include "../util/helpers.h"

namespace jni = qdb::jni;

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_build(JNIEnv *jniEnv, jclass /*thisClass*/)
{
    qdb::jni::env env(jniEnv);

    return env.instance().NewStringUTF(qdb_build());
}

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_version(JNIEnv *jniEnv, jclass /*thisClass*/)
{
    qdb::jni::env env(jniEnv);

    return env.instance().NewStringUTF(qdb_version());
}

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_error_1message(JNIEnv *jniEnv,
                                             jclass /*thisClass*/,
                                             jint err)
{
    qdb::jni::env env(jniEnv);

    return env.instance().NewStringUTF(qdb_error((qdb_error_t)err));
}

JNIEXPORT jlong JNICALL
Java_net_quasardb_qdb_jni_qdb_open_1tcp(JNIEnv * /*env*/, jclass /*thisClass*/)
{
    return (jlong)qdb_open_tcp();
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_connect(JNIEnv *jniEnv,
                                      jclass /*thisClass*/,
                                      jlong handle,
                                      jstring uri)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb::jni::log::swap_callback();

        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_connect((qdb_handle_t)handle,
                        qdb::jni::string::get_chars_utf8(env, uri)));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_secure_1connect(JNIEnv *jniEnv,
                                              jclass /*thisClass*/,
                                              jlong handle,
                                              jstring uri,
                                              jobject securityOptions)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb::jni::log::swap_callback();

        qdb_error_t err;
        jclass objectClass;
        jfieldID userNameField, userPrivateKeyField, clusterPublicKeyField;

        objectClass = env.instance().GetObjectClass(securityOptions);
        userNameField = env.instance().GetFieldID(objectClass, "user_name",
                                                  "Ljava/lang/String;");
        userPrivateKeyField = env.instance().GetFieldID(
            objectClass, "user_private_key", "Ljava/lang/String;");
        clusterPublicKeyField = env.instance().GetFieldID(
            objectClass, "cluster_public_key", "Ljava/lang/String;");

        jstring userName = (jstring)env.instance().GetObjectField(
            securityOptions, userNameField);
        jstring userPrivateKey = (jstring)env.instance().GetObjectField(
            securityOptions, userPrivateKeyField);
        jstring clusterPublicKey = (jstring)env.instance().GetObjectField(
            securityOptions, clusterPublicKeyField);

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_option_set_cluster_public_key(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, clusterPublicKey)));

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_option_set_user_credentials(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, userName),
                qdb::jni::string::get_chars_utf8(env, userPrivateKey)));

        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_connect((qdb_handle_t)handle,
                        qdb::jni::string::get_chars_utf8(env, uri)));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_close(JNIEnv *jniEnv,
                                    jclass /*thisClass*/,
                                    jlong handle)
{
    qdb::jni::env env(jniEnv);
    try
    {
        return qdb_close((qdb_handle_t)handle);
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_release(JNIEnv *jniEnv,
                                      jclass /*thisClass*/,
                                      jlong handle,
                                      jobject buffer)
{
    qdb::jni::env env(jniEnv);

    try
    {
        void *ptr = env.instance().GetDirectBufferAddress(buffer);
        qdb_release((qdb_handle_t)handle, ptr);
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_option_1set_1timeout(JNIEnv *jniEnv,
                                                   jclass /*thisClass*/,
                                                   jlong handle,
                                                   jint timeout)
{
    qdb::jni::env env(jniEnv);
    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_option_set_timeout((qdb_handle_t)handle, timeout));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jlong JNICALL
Java_net_quasardb_qdb_jni_qdb_option_1get_1client_1max_1in_1buf_1size(
    JNIEnv *jniEnv, jclass /*thisClass*/, jlong handle)
{

    qdb::jni::env env(jniEnv);
    try
    {
        qdb_size_t size;

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_option_get_client_max_in_buf_size((qdb_handle_t)handle, &size));

        return size;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return -1;
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_option_1set_1client_1max_1in_1buf_1size(
    JNIEnv *jniEnv, jclass /*thisClass*/, jlong handle, jlong size)
{

    qdb::jni::env env(jniEnv);
    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_option_set_client_max_in_buf_size((qdb_handle_t)handle, size));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_purge_1all(JNIEnv *jniEnv,
                                         jclass /*thisClass*/,
                                         jlong handle,
                                         jint timeout)
{
    qdb::jni::env env(jniEnv);
    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle, qdb_purge_all((qdb_handle_t)handle, timeout));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_trim_1all(JNIEnv *jniEnv,
                                        jclass /*thisClass*/,
                                        jlong handle,
                                        jint timeout)
{
    qdb::jni::env env(jniEnv);
    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle, qdb_trim_all((qdb_handle_t)handle, timeout));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_1wait_1for_1stabilization(JNIEnv *jniEnv,
                                                        jclass /*thisClass*/,
                                                        jlong handle,
                                                        jint timeout)
{
    qdb::jni::env env(jniEnv);
    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_wait_for_stabilization((qdb_handle_t)handle, timeout));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_remove(JNIEnv *jniEnv,
                                     jclass /*thisClass*/,
                                     jlong handle,
                                     jstring alias)
{
    qdb::jni::env env(jniEnv);
    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_remove((qdb_handle_t)handle,
                       qdb::jni::string::get_chars_utf8(env, alias)));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1type(JNIEnv *jniEnv,
                                        jclass /*thisClass*/,
                                        jlong handle,
                                        jstring alias,
                                        jobject type)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_entry_metadata_t metadata;
        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_get_metadata(
                (qdb_handle_t)handle,
                (alias == NULL ? (char const *)(NULL)
                               : qdb::jni::string::get_chars_utf8(env, alias)),
                &metadata));
        setInteger(env, type, metadata.type);
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1metadata(JNIEnv *jniEnv,
                                            jclass /*thisClass*/,
                                            jlong handle,
                                            jstring alias,
                                            jobject meta)
{
    qdb::jni::env env(jniEnv);

    try
    {
        void *metaPtr = env.instance().GetDirectBufferAddress(meta);
        qdb_size_t metaSize =
            (qdb_size_t)env.instance().GetDirectBufferCapacity(meta);
        if (metaSize != sizeof(qdb_entry_metadata_t))
        {
            // XXX(leon): hacks!
            jni::exception::throw_if_error((qdb_handle_t)handle,
                                           qdb_e_invalid_argument);
        }

        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_get_metadata((qdb_handle_t)handle,
                             qdb::jni::string::get_chars_utf8(env, alias),
                             (qdb_entry_metadata_t *)metaPtr));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_expires_1at(JNIEnv *jniEnv,
                                          jclass /*thisClass*/,
                                          jlong handle,
                                          jstring alias,
                                          jlong expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_expires_at((qdb_handle_t)handle,
                           qdb::jni::string::get_chars_utf8(env, alias),
                           expiry));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1expiry_1time(JNIEnv *jniEnv,
                                                jclass /*thisClass*/,
                                                jlong handle,
                                                jstring alias,
                                                jobject expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_entry_metadata_t metadata;
        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_get_metadata((qdb_handle_t)handle,
                             qdb::jni::string::get_chars_utf8(env, alias),
                             &metadata));
        setLong(env, expiry,
                static_cast<qdb_time_t>(metadata.expiry_time.tv_sec) * 1000 +
                    static_cast<qdb_time_t>(metadata.expiry_time.tv_nsec /
                                            1000000ull));
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}
