#include "../env.h"
#include "../exception.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/batch.h>

namespace jni = qdb::jni;

static qdb_operation_t & get_operation(jlong batch, jint index)
{
    return reinterpret_cast<qdb_operation_t *>(batch)[index];
}

extern "C" JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_init_1operations(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jint count, jobject batch)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_operation_t * ops = new qdb_operation_t[count];
        jni::exception::throw_if_error(
            (qdb_handle_t)handle, {qdb_e_alias_not_found}, qdb_init_operations(ops, count));
        setLong(env, batch, reinterpret_cast<jlong>(ops));
        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

extern "C" JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_delete_1batch(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batch)
{
    qdb::jni::env env(jniEnv);
    void * ptr = reinterpret_cast<void *>(batch);
    qdb_release((qdb_handle_t)handle, ptr);
    return qdb_e_ok;
}

extern "C" JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_run_1batch(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batch, jint count)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_operation_t * ops = reinterpret_cast<qdb_operation_t *>(batch);

        // returns size_t, not qdb_error_t!
        return (jint)qdb_run_batch((qdb_handle_t)handle, ops, count);
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

// -----------------------
// blob_compare_and_swap
// -----------------------

extern "C" JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1compare_1and_1swap(JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong batch,
    jint index,
    jstring alias,
    jobject newContent,
    jobject comparand,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    qdb_operation_t & op         = get_operation(batch, index);
    op.type                      = qdb_op_blob_cas;
    op.alias                     = alias ? env.instance().GetStringUTFChars(alias, NULL) : NULL;
    op.blob_cas.new_content      = env.instance().GetDirectBufferAddress(newContent);
    op.blob_cas.new_content_size = (qdb_size_t)env.instance().GetDirectBufferCapacity(newContent);
    op.blob_cas.comparand        = env.instance().GetDirectBufferAddress(comparand);
    op.blob_cas.comparand_size   = (qdb_size_t)env.instance().GetDirectBufferCapacity(comparand);
    op.blob_cas.expiry_time      = expiry;
}

extern "C" JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1compare_1and_1swap(JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batch,
    jint index,
    jstring alias,
    jobject originalContent)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_operation_t & op = get_operation(batch, index);
        jni::exception::throw_if_error(handle_, op.error);

        if (alias) env.instance().ReleaseStringUTFChars(alias, op.alias);
        setByteBuffer(env, handle_, originalContent, op.blob_cas.original_content,
            op.blob_cas.original_content_size);
        return op.error;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

// -----------------------
// blob_get
// -----------------------

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1get(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong batch, jint index, jstring alias)
{
    qdb::jni::env env(jniEnv);

    qdb_operation_t & op = get_operation(batch, index);
    op.type              = qdb_op_blob_get;
    op.alias             = alias ? env.instance().GetStringUTFChars(alias, NULL) : NULL;
}

extern "C" JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1get(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batch,
    jint index,
    jstring alias,
    jobject content)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_operation_t & op = get_operation(batch, index);
        jni::exception::throw_if_error(handle_, op.error);

        if (alias) env.instance().ReleaseStringUTFChars(alias, op.alias);
        setByteBuffer(env, handle_, content, op.blob_get.content, op.blob_get.content_size);
        return op.error;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

// -----------------------
// blob_get_and_update
// -----------------------

extern "C" JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1get_1and_1update(JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong batch,
    jint index,
    jstring alias,
    jobject content,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    qdb_operation_t & op = get_operation(batch, index);
    op.type              = qdb_op_blob_get_and_update;
    op.alias             = alias ? env.instance().GetStringUTFChars(alias, NULL) : NULL;
    op.blob_get_and_update.new_content = env.instance().GetDirectBufferAddress(content);
    op.blob_get_and_update.new_content_size =
        (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
    op.blob_get_and_update.expiry_time = expiry;
}

extern "C" JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1get_1and_1update(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batch,
    jint index,
    jstring alias,
    jobject content)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_operation_t & op = get_operation(batch, index);
        jni::exception::throw_if_error(handle_, op.error);

        if (alias) env.instance().ReleaseStringUTFChars(alias, op.alias);
        setByteBuffer(env, handle_, content, op.blob_get_and_update.original_content,
            op.blob_get_and_update.original_content_size);
        return op.error;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

// -----------------------
// blob_put
// -----------------------

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1put(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong batch,
    jint index,
    jstring alias,
    jobject content,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    qdb_operation_t & op     = get_operation(batch, index);
    op.type                  = qdb_op_blob_put;
    op.alias                 = alias ? env.instance().GetStringUTFChars(alias, NULL) : NULL;
    op.blob_put.content      = env.instance().GetDirectBufferAddress(content);
    op.blob_put.content_size = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
    op.blob_put.expiry_time  = expiry;
}

extern "C" JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1put(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batch, jint index, jstring alias)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_operation_t & op = get_operation(batch, index);
        jni::exception::throw_if_error((qdb_handle_t)handle, op.error);

        if (alias) env.instance().ReleaseStringUTFChars(alias, op.alias);
        return op.error;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

// -----------------------
// blob_update
// -----------------------

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1update(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong batch,
    jint index,
    jstring alias,
    jobject content,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    qdb_operation_t & op     = get_operation(batch, index);
    op.type                  = qdb_op_blob_update;
    op.alias                 = alias ? env.instance().GetStringUTFChars(alias, NULL) : NULL;
    op.blob_put.content      = env.instance().GetDirectBufferAddress(content);
    op.blob_put.content_size = (qdb_size_t)env.instance().GetDirectBufferCapacity(content);
    op.blob_put.expiry_time  = expiry;
}

extern "C" JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1update(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batch, jint index, jstring alias)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_operation_t & op = get_operation(batch, index);
        jni::exception::throw_if_error((qdb_handle_t)handle, op.error);

        if (alias) env.instance().ReleaseStringUTFChars(alias, op.alias);
        return op.error;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}
