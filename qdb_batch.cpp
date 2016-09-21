#include "net_quasardb_qdb_jni_qdb.h"

#include "helpers.h"
#include <qdb/batch.h>

static qdb_operation_t &
get_operation(jlong batch, jint index) {
  return reinterpret_cast<qdb_operation_t *>(batch)[index];
}

extern "C" JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_init_1operations(JNIEnv *env, jclass thisClass,
                                               jlong handle, jint count,
                                               jobject batch) {
  qdb_operation_t *ops = new qdb_operation_t[count];
  qdb_error_t err = qdb_init_operations(ops, count);
  if (QDB_SUCCESS(err)) {
    setLong(env, batch, reinterpret_cast<jlong>(ops));
  } else {
    delete[] ops;
    setLong(env, batch, 0);
  }
  return err;
}

extern "C" JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_free_1operations(JNIEnv *env, jclass thisClass,
                                               jlong handle, jlong batch,
                                               jint count) {
  qdb_operation_t *ops = reinterpret_cast<qdb_operation_t *>(batch);
  qdb_error_t err = qdb_init_operations(ops, count);
  delete[] ops;
  return err;
}

extern "C" JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_run_1batch(JNIEnv *env, jclass thisClass,
                                         jlong handle, jlong batch,
                                         jint count) {
  qdb_operation_t *ops = reinterpret_cast<qdb_operation_t *>(batch);
  return qdb_run_batch((qdb_handle_t)handle, ops, count);
}

// -----------------------
// blob_compare_and_swap
// -----------------------

extern "C" JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1compare_1and_1swap(
    JNIEnv *env, jclass thisClass, jlong batch, jint index, jstring alias,
    jobject newContent, jobject comparand, jlong expiry) {
  qdb_operation_t &op = get_operation(batch, index);
  op.type = qdb_op_blob_cas;
  op.alias = alias ? env->GetStringUTFChars(alias, NULL) : NULL;
  op.blob_cas.new_content = env->GetDirectBufferAddress(newContent);
  op.blob_cas.new_content_size =
      (qdb_size_t)env->GetDirectBufferCapacity(newContent);
  op.blob_cas.comparand = env->GetDirectBufferAddress(comparand);
  op.blob_cas.comparand_size =
      (qdb_size_t)env->GetDirectBufferCapacity(comparand);
  op.blob_cas.expiry_time = expiry;
}

extern "C" JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1compare_1and_1swap(
    JNIEnv *env, jclass thisClass, jlong batch, jint index, jstring alias,
    jobject originalContent) {
  qdb_operation_t &op = get_operation(batch, index);
  if (alias)
    env->ReleaseStringUTFChars(alias, op.alias);
  setByteBuffer(env, originalContent, op.blob_cas.original_content,
                op.blob_cas.original_content_size);
  return op.error;
}

// -----------------------
// blob_get
// -----------------------

extern "C" JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1get(JNIEnv *env,
                                                      jclass thisClass,
                                                      jlong batch, jint index,
                                                      jstring alias) {
  qdb_operation_t &op = get_operation(batch, index);
  op.type = qdb_op_blob_get;
  op.alias = alias ? env->GetStringUTFChars(alias, NULL) : NULL;
}

extern "C" JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1get(JNIEnv *env,
                                                     jclass thisClass,
                                                     jlong batch, jint index,
                                                     jstring alias,
                                                     jobject content) {
  qdb_operation_t &op = get_operation(batch, index);
  if (alias)
    env->ReleaseStringUTFChars(alias, op.alias);
  setByteBuffer(env, content, op.blob_get.content, op.blob_get.content_size);
  return op.error;
}

// -----------------------
// blob_get_and_update
// -----------------------

extern "C" JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1get_1and_1update(
    JNIEnv *env, jclass thisClass, jlong batch, jint index, jstring alias,
    jobject content, jlong expiry) {
  qdb_operation_t &op = get_operation(batch, index);
  op.type = qdb_op_blob_get_and_update;
  op.alias = alias ? env->GetStringUTFChars(alias, NULL) : NULL;
  op.blob_get_and_update.new_content = env->GetDirectBufferAddress(content);
  op.blob_get_and_update.new_content_size =
      (qdb_size_t)env->GetDirectBufferCapacity(content);
  op.blob_get_and_update.expiry_time = expiry;
}

extern "C" JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1get_1and_1update(
    JNIEnv *env, jclass thisClass, jlong batch, jint index, jstring alias,
    jobject content) {
  qdb_operation_t &op = get_operation(batch, index);
  if (alias)
    env->ReleaseStringUTFChars(alias, op.alias);
  setByteBuffer(env, content, op.blob_get_and_update.original_content,
                op.blob_get_and_update.original_content_size);
  return op.error;
}

// -----------------------
// blob_put
// -----------------------

extern "C" JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1put(
    JNIEnv *env, jclass thisClass, jlong batch, jint index, jstring alias,
    jobject content, jlong expiry) {

  qdb_operation_t &op = get_operation(batch, index);
  op.type = qdb_op_blob_put;
  op.alias = alias ? env->GetStringUTFChars(alias, NULL) : NULL;
  op.blob_put.content = env->GetDirectBufferAddress(content);
  op.blob_put.content_size = (qdb_size_t)env->GetDirectBufferCapacity(content);
  op.blob_put.expiry_time = expiry;
}

extern "C" JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1put(JNIEnv *env,
                                                     jclass thisClass,
                                                     jlong batch, jint index,
                                                     jstring alias) {
  qdb_operation_t &op = get_operation(batch, index);
  if (alias)
    env->ReleaseStringUTFChars(alias, op.alias);
  return op.error;
}

// -----------------------
// blob_update
// -----------------------

extern "C" JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1write_1blob_1update(
    JNIEnv *env, jclass thisClass, jlong batch, jint index, jstring alias,
    jobject content, jlong expiry) {
  qdb_operation_t &op = get_operation(batch, index);
  op.type = qdb_op_blob_update;
  op.alias = alias ? env->GetStringUTFChars(alias, NULL) : NULL;
  op.blob_put.content = env->GetDirectBufferAddress(content);
  op.blob_put.content_size = (qdb_size_t)env->GetDirectBufferCapacity(content);
  op.blob_put.expiry_time = expiry;
}

extern "C" JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_batch_1read_1blob_1update(JNIEnv *env,
                                                        jclass thisClass,
                                                        jlong batch, jint index,
                                                        jstring alias) {
  qdb_operation_t &op = get_operation(batch, index);
  if (alias)
    env->ReleaseStringUTFChars(alias, op.alias);
  return op.error;
}
