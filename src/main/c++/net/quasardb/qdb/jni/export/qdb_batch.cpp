#include "../detail/native_ptr.h"
#include "../env.h"
#include "../exception.h"
#include "../string.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/batch.h>

namespace jni = qdb::jni;

static qdb_operation_t & get_operation(jlong batch, jint index)
{
    qdb_operation_t * batch_ = qdb::jni::native_ptr::from_java<qdb_operation_t *>(batch);
    return batch_[index];
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

extern "C" JNIEXPORT jlong JNICALL Java_net_quasardb_qdb_jni_qdb_init_1batch(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jint count)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_operation_t * ops = new qdb_operation_t[count];
        jni::exception::throw_if_error(
            (qdb_handle_t)handle, {qdb_e_alias_not_found}, qdb_init_operations(ops, count));

        return qdb::jni::native_ptr::to_java(ops);
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_release_1batch(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batch, jint count)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_     = qdb::jni::native_ptr::from_java<qdb_handle_t>(handle);
        qdb_operation_t * batch_ = qdb::jni::native_ptr::from_java<qdb_operation_t *>(batch);

        printf("qdb_release_batch, count = %d\n", count);

        for (jint idx = 0; idx < count; ++idx)
        {
            qdb_operation_t & op = get_operation(batch, idx);

            printf("got op with type: %d at index %d\n", op.type, idx);

            switch (op.type)
            {
            default:
                printf("warn: unrecognized op type: %d\n", op.type);
                break;
            }
        }

        qdb_release(handle_, batch_);
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
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

extern "C" JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_commit_1batch_1fast(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batch, jint count)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_     = qdb::jni::native_ptr::from_java<qdb_handle_t>(handle);
        qdb_operation_t * batch_ = qdb::jni::native_ptr::from_java<qdb_operation_t *>(batch);

        return qdb_run_batch(handle_, batch_, count);
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

extern "C" JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_commit_1batch_1transactional(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batch, jint count)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_     = qdb::jni::native_ptr::from_java<qdb_handle_t>(handle);
        qdb_operation_t * batch_ = qdb::jni::native_ptr::from_java<qdb_operation_t *>(batch);

        size_t fail_idx;

        return qdb_run_transaction(handle_, batch_, count, &fail_idx);
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
// string_put
// -----------------------

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1write_1string_1put(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batch,
    jint index,
    jstring alias,
    jstring content,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_     = qdb::jni::native_ptr::from_java<qdb_handle_t>(handle);
        qdb_operation_t * batch_ = qdb::jni::native_ptr::from_java<qdb_operation_t *>(batch);

        auto alias_   = jni::string::get_chars_utf8(env, handle_, alias);
        auto content_ = jni::string::get_chars_utf8(env, handle_, content);

        qdb_operation_t & op       = get_operation(batch, index);
        op.type                    = qdb_op_string_put;
        op.alias                   = alias ? env.instance().GetStringUTFChars(alias_, NULL) : NULL;
        op.string_put.content      = content_.copy(handle_);
        op.string_put.content_size = content_.size();
        op.string_put.expiry_time  = expiry;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

// -----------------------
// int_put
// -----------------------

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1write_1int_1put(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batch,
    jint index,
    jstring alias,
    jlong content,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_     = qdb::jni::native_ptr::from_java<qdb_handle_t>(handle);
        qdb_operation_t * batch_ = qdb::jni::native_ptr::from_java<qdb_operation_t *>(batch);

        auto alias_ = jni::string::get_chars_utf8(env, handle_, alias);

        qdb_operation_t & op   = get_operation(batch, index);
        op.type                = qdb_op_int_put;
        op.alias               = alias ? env.instance().GetStringUTFChars(alias_, NULL) : NULL;
        op.int_put.value       = content;
        op.int_put.expiry_time = expiry;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

// -----------------------
// double_put
// -----------------------

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1write_1double_1put(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batch,
    jint index,
    jstring alias,
    jdouble content,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_     = qdb::jni::native_ptr::from_java<qdb_handle_t>(handle);
        qdb_operation_t * batch_ = qdb::jni::native_ptr::from_java<qdb_operation_t *>(batch);

        auto alias_ = jni::string::get_chars_utf8(env, handle_, alias);

        qdb_operation_t & op      = get_operation(batch, index);
        op.type                   = qdb_op_double_put;
        op.alias                  = alias ? env.instance().GetStringUTFChars(alias_, NULL) : NULL;
        op.double_put.value       = content;
        op.double_put.expiry_time = expiry;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
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

// -----------------------
// string_update
// -----------------------

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1write_1string_1update(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batch,
    jint index,
    jstring alias,
    jstring content,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = qdb::jni::native_ptr::from_java<qdb_handle_t>(handle);

        auto alias_   = jni::string::get_chars_utf8(env, handle_, alias);
        auto content_ = jni::string::get_chars_utf8(env, handle_, content);

        qdb_operation_t & op     = get_operation(batch, index);
        op.type                  = qdb_op_string_update;
        op.alias                 = alias ? env.instance().GetStringUTFChars(alias_, NULL) : NULL;
        op.string_update.content = content_.copy(handle_);
        op.string_update.content_size = content_.size();
        op.string_update.expiry_time  = expiry;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

// -----------------------
// int_update
// -----------------------

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1write_1int_1update(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batch,
    jint index,
    jstring alias,
    jlong content,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_     = qdb::jni::native_ptr::from_java<qdb_handle_t>(handle);
        qdb_operation_t * batch_ = qdb::jni::native_ptr::from_java<qdb_operation_t *>(batch);

        auto alias_ = jni::string::get_chars_utf8(env, handle_, alias);

        qdb_operation_t & op      = get_operation(batch, index);
        op.type                   = qdb_op_int_update;
        op.alias                  = alias ? env.instance().GetStringUTFChars(alias_, NULL) : NULL;
        op.int_update.value       = content;
        op.int_update.expiry_time = expiry;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

// -----------------------
// double_update
// -----------------------

extern "C" JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_batch_1write_1double_1update(
    JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batch,
    jint index,
    jstring alias,
    jdouble content,
    jlong expiry)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_     = qdb::jni::native_ptr::from_java<qdb_handle_t>(handle);
        qdb_operation_t * batch_ = qdb::jni::native_ptr::from_java<qdb_operation_t *>(batch);

        auto alias_ = jni::string::get_chars_utf8(env, handle_, alias);

        qdb_operation_t & op   = get_operation(batch, index);
        op.type                = qdb_op_double_update;
        op.alias               = alias ? env.instance().GetStringUTFChars(alias_, NULL) : NULL;
        op.double_update.value = content;
        op.double_update.expiry_time = expiry;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}
