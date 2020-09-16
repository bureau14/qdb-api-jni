#include <cassert>
#include <string.h> // memcpy
#include <qdb/ts.h>
#include <stdlib.h>
#include <iostream>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../env.h"
#include "../object.h"
#include "../exception.h"
#include "../log.h"
#include "../string.h"
#include "../util/helpers.h"
#include "../util/ts_helpers.h"
#include "../util/pinned_columns.h"
#include "../byte_array.h"
#include "../object_array.h"
#include "../primitive_array.h"

namespace jni = qdb::jni;

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1create(JNIEnv *jniEnv,
                                         jclass /*thisClass*/,
                                         jlong handle,
                                         jstring alias,
                                         jlong shard_size,
                                         jobjectArray columns)
{
    qdb::jni::env env(jniEnv);

    try
    {
        size_t column_count = env.instance().GetArrayLength(columns);
        qdb_ts_column_info_t *native_columns =
            new qdb_ts_column_info_t[column_count];

        columnsToNative(env, columns, native_columns, column_count);

        qdb::jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_create((qdb_handle_t)handle,
                          qdb::jni::string::get_chars_utf8(env, alias),
                          (qdb_uint_t)shard_size, native_columns,
                          column_count));
        releaseNative(native_columns, column_count);

        delete[] native_columns;
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {

        //! :XXX: memory leak for native_columns? use unique_ptr instead?

        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1remove(JNIEnv *jniEnv,
                                         jclass /*thisClass*/,
                                         jlong handle,
                                         jstring alias)
{
    qdb::jni::env env(jniEnv);

    try
    {
        return qdb::jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_remove((qdb_handle_t)handle,
                       qdb::jni::string::get_chars_utf8(env, alias)));
    }
    catch (jni::exception const &e)
    {

        //! :XXX: memory leak for native_columns? use unique_ptr instead?

        e.throw_new(env);
        return e.error();
    }
}


JNIEXPORT jlong JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1shard_1size(JNIEnv *jniEnv,
                                              jclass /*thisClass*/,
                                              jlong handle,
                                              jstring alias)
{
    qdb::jni::env env(jniEnv);
    try {
      qdb_uint_t shard_size {0};
      qdb::jni::exception::throw_if_error(
          (qdb_handle_t)handle,
          qdb_ts_shard_size((qdb_handle_t)handle,
                            qdb::jni::string::get_chars_utf8(env, alias),
                            &shard_size));


      assert(shard_size > 0);
      return (jlong)shard_size;


    }
    catch (jni::exception const &e)
    {

        //! :XXX: memory leak for native_columns? use unique_ptr instead?

        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1insert_1columns(JNIEnv *jniEnv,
                                                  jclass /*thisClass*/,
                                                  jlong handle,
                                                  jstring alias,
                                                  jobjectArray columns)
{
    qdb::jni::env env(jniEnv);

    try
    {
        size_t column_count = env.instance().GetArrayLength(columns);
        qdb_ts_column_info_t *native_columns =
            new qdb_ts_column_info_t[column_count];

        columnsToNative(env, columns, native_columns, column_count);

        jni::exception::throw_if_error(
            (qdb_handle_t)(handle),
            qdb_ts_insert_columns((qdb_handle_t)handle,
                                  qdb::jni::string::get_chars_utf8(env, alias),
                                  native_columns, column_count));
        releaseNative(native_columns, column_count);

        delete[] native_columns;
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {

        //! :XXX: memory leak for native_columns? use unique_ptr instead?

        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1list_1columns(JNIEnv *jniEnv,
                                                jclass /*thisClass*/,
                                                jlong handle,
                                                jstring alias,
                                                jobject columns)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_ts_column_info_t *native_columns;
        qdb_size_t column_count;

        jni::exception::throw_if_error(
            (qdb_handle_t)(handle),
            qdb_ts_list_columns((qdb_handle_t)handle,
                                qdb::jni::string::get_chars_utf8(env, alias),
                                &native_columns, &column_count));

        setReferenceValue(env, columns,
                          nativeToColumns(env, native_columns, column_count));

        qdb_release((qdb_handle_t)handle, native_columns);

        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {

        //! :XXX: memory leak for native_columns? use unique_ptr instead?

        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1table_1init(JNIEnv *jniEnv,
                                                     jclass /*thisClass*/,
                                                     jlong handle,
                                                     jobjectArray tableColumns,
                                                     jobject batchTable)
{
    qdb::jni::env env(jniEnv);

    try
    {
        size_t columnInfoCount = env.instance().GetArrayLength(tableColumns);
        qdb_ts_batch_column_info_t *columnInfo =
            batchColumnInfo(env, tableColumns);

        qdb_batch_table_t nativeBatchTable;

        qdb::jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_batch_table_init((qdb_handle_t)handle, columnInfo,
                                    columnInfoCount, &nativeBatchTable));

        batchColumnRelease(columnInfo, columnInfoCount);

        setLong(env, batchTable, reinterpret_cast<long>(nativeBatchTable));

        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1release_1columns_1memory(JNIEnv *jniEnv,
                                                                  jclass /*thisClass*/,
                                                                  jlong handle,
                                                                  jlong batchTable) {
    qdb::jni::env env(jniEnv);

    try
    {
      jni::exception::throw_if_error((qdb_handle_t)handle,
                                     qdb_ts_batch_release_columns_memory((qdb_batch_table_t)batchTable));


      return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1table_1extra_1columns(
    JNIEnv *jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jlong batchTable,
    jobjectArray tableColumns)
{
    qdb::jni::env env(jniEnv);

    try
    {
        size_t columnInfoCount = env.instance().GetArrayLength(tableColumns);
        qdb_ts_batch_column_info_t *columnInfo =
            batchColumnInfo(env, tableColumns);

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_batch_table_extra_columns((qdb_batch_table_t)batchTable,
                                             columnInfo, columnInfoCount));

        batchColumnRelease(columnInfo, columnInfoCount);

        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1table_1release(JNIEnv * /*env*/,
                                                        jclass /*thisClass*/,
                                                        jlong handle,
                                                        jlong batchTable)
{

    qdb_release((qdb_handle_t)handle, (qdb_batch_table_t)batchTable);
}



/**
 * Cast a java array of Writer.TableColumn[] to a native array of column types.
 * `out` is assumed to be pre-allocated and should be as long as the `in` length.
 */
void
pin_columns(qdb_batch_table_t table,
            qdb_ts_column_type_t * types) {
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1pinned_1push(JNIEnv *jniEnv,
                                                      jclass /*thisClass*/,
                                                      jlong handle,
                                                      jlong batchTable,
                                                      jintArray columnTypes,
                                                      jobjectArray rows)
{

    qdb::jni::env env(jniEnv);
    try
    {
        qdb::jni::log::swap_callback();

        // qdb_ts_column_type_t * column_types[column_count];
        // to_column_type_array(env, columns, column_types, column_count);
        auto column_types_guard = jni::primitive_array::get_array_critical<qdb_ts_column_type_t>(env, columnTypes);

        // Sanity check
        assert(column_types_guard.size() == env.instance().GetArrayLength(columnTypes));

        /**
         * Push a single shard using pinned columns. We first pin all the columns
         * that we expect.
         */
        size_t rowCount = env.instance().GetArrayLength(rows);
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}



JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1set_1pinned_1doubles(JNIEnv *jniEnv,
                                                              jclass /*thisClass*/,
                                                              jlong handle_,
                                                              jlong table_,
                                                              jlong shard_,
                                                              jint columnIndex,
                                                              jlongArray timeoffsets_,
                                                              jdoubleArray data_)
{
    // We do a fast copy and rely on this assumption
    static_assert(sizeof(jdouble) == sizeof(double));

    qdb::jni::env env(jniEnv);
    try
    {
       qdb_time_t * timeoffsets = NULL;
       double * data            = NULL;

       qdb_handle_t handle      = reinterpret_cast<qdb_handle_t>(handle_);
       qdb_batch_table_t table  = reinterpret_cast<qdb_batch_table_t>(table_);
       qdb_timespec_t shard     = qdb_timespec_t {shard_, 0};


       auto data_guard         = jni::primitive_array::get_array_critical<jdouble>(env, data_);
       auto timeoffsets_guard  = jni::primitive_array::get_array_critical<jlong>(env, timeoffsets_);

       assert(data_guard.size () == timeoffsets_guard.size());

       jni::column_pinner<double> pinner {};
       pinner.pin(env, handle, table, columnIndex, data_guard.size(), &shard, &timeoffsets, &data);


       // note: data_guard uses `jdouble` while data is in `double` -- see static assert above.
       pinner.copy(env,
                   timeoffsets_guard.get(), data_guard.get(),
                   timeoffsets, data,
                   data_guard.size());


       return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
      e.throw_new(env);
      return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1set_1pinned_1int64s(JNIEnv *jniEnv,
                                                             jclass /*thisClass*/,
                                                             jlong handle_,
                                                             jlong table_,
                                                             jlong shard_,
                                                             jint columnIndex,
                                                             jlongArray timeoffsets_,
                                                             jlongArray data_)
{
    static_assert(sizeof(jlong) == sizeof(qdb_int_t));
    static_assert(sizeof(jlong) == sizeof(int64_t));

    qdb::jni::env env(jniEnv);
    try
    {
       qdb_time_t * timeoffsets = NULL;
       qdb_int_t * data         = NULL;

       qdb_handle_t handle      = reinterpret_cast<qdb_handle_t>(handle_);
       qdb_batch_table_t table  = reinterpret_cast<qdb_batch_table_t>(table_);
       qdb_timespec_t shard     = qdb_timespec_t {shard_, 0};

       auto data_guard         = jni::primitive_array::get_array_critical<jlong>(env, data_);
       auto timeoffsets_guard  = jni::primitive_array::get_array_critical<jlong>(env, timeoffsets_);

       assert(data_guard.size () == timeoffsets_guard.size());

       jni::column_pinner<jlong, qdb_int_t> pinner {};
       pinner.pin(env, handle, table, columnIndex, data_guard.size(), &shard, &timeoffsets, &data);

       assert(timeoffsets != NULL);
       assert(data != NULL);

       // note: data_guard uses `jlong` while data is in `qdb_int_t` -- see static assert above.
       pinner.copy(env,
                   timeoffsets_guard.get(), data_guard.get(),
                   timeoffsets, data,
                   data_guard.size());


       return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
      e.throw_new(env);
      return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1set_1pinned_1timestamps(JNIEnv *jniEnv,
                                                                 jclass /*thisClass*/,
                                                                 jlong handle_,
                                                                 jlong table_,
                                                                 jlong shard_,
                                                                 jint columnIndex,
                                                                 jlongArray timeoffsets_,
                                                                 jobject values)
{
    static_assert(sizeof(jlong) == sizeof(qdb_time_t));

    qdb::jni::env env(jniEnv);
    try
    {
       qdb_time_t * timeoffsets = NULL;
       qdb_timespec_t * data    = NULL;

       qdb_handle_t handle      = reinterpret_cast<qdb_handle_t>(handle_);
       qdb_batch_table_t table  = reinterpret_cast<qdb_batch_table_t>(table_);
       qdb_timespec_t shard     = qdb_timespec_t {shard_, 0};


      /**
       * We retrieve a `Timespecs` class from the jvm in `values`, which contains two
       * primitive arrays sec and nsec. Our first task is to get native access to these
       * underlying arrays.
       */
      jclass timespecClass = jni::object::get_class(env, values);
      jfieldID secField = jni::introspect::lookup_field(env, timespecClass,
                                                       "sec", "[J");
      jfieldID nsecField = jni::introspect::lookup_field(env, timespecClass,
                                                        "nsec", "[J");
      jlongArray secArray = (jlongArray)env.instance().GetObjectField(values, secField);
      jlongArray nsecArray = (jlongArray)env.instance().GetObjectField(values, nsecField);

      assert(secArray != NULL);
      assert(nsecArray != NULL);

      /**
       * Pin the columns, same logic as other `set_pinned_*` functions. Difference is that
       * we have two value arrays (secArray and nsecArray), so we invoke copy2 on the pinner.
       */
      auto sec_guard         = jni::primitive_array::get_array_critical<jlong>(env, secArray);
      auto nsec_guard         = jni::primitive_array::get_array_critical<jlong>(env, nsecArray);
      auto timeoffsets_guard  = jni::primitive_array::get_array_critical<jlong>(env, timeoffsets_);

      assert(timeoffsets_guard.size() == sec_guard.size());
      assert(timeoffsets_guard.size() == nsec_guard.size());

      jni::column_pinner<jlong, qdb_timespec_t> pinner {};
      pinner.pin(env, handle, table, columnIndex, timeoffsets_guard.size(), &shard, &timeoffsets, &data);

      // note: (n)sec_guard uses `jlong` while data is in `qdb_time_t` -- see static assert above.
      pinner.copy2(env,
                   timeoffsets_guard.get(), sec_guard.get(), nsec_guard.get(),
                   timeoffsets, data,
                   timeoffsets_guard.size());

      return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
      e.throw_new(env);
      return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1set_1pinned_1blobs(JNIEnv *jniEnv,
                                                            jclass /*thisClass*/,
                                                            jlong handle_,
                                                            jlong table_,
                                                            jlong shard_,
                                                            jint columnIndex,
                                                            jlongArray timeoffsets_,
                                                            jobjectArray data_)
{
    qdb::jni::env env(jniEnv);
    try
    {
       qdb_time_t * timeoffsets = NULL;
       qdb_blob_t * data        = NULL;

       qdb_handle_t handle      = reinterpret_cast<qdb_handle_t>(handle_);
       qdb_batch_table_t table  = reinterpret_cast<qdb_batch_table_t>(table_);
       qdb_timespec_t shard     = qdb_timespec_t {shard_, 0};

       jni::object_array values(env, data_);
       auto timeoffsets_guard  = jni::primitive_array::get_array_critical<jlong>(env, timeoffsets_);

       assert(values.size () == timeoffsets_guard.size());

       jni::column_pinner<jni::object_array, qdb_blob_t> pinner {};
       pinner.pin(env, handle, table, columnIndex, timeoffsets_guard.size(), &shard, &timeoffsets, &data);

       assert(timeoffsets != NULL);
       assert(data != NULL);

       pinner.copy(env,
                   timeoffsets_guard.get(), values,
                   timeoffsets, data,
                   timeoffsets_guard.size());


       return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
      e.throw_new(env);
      return e.error();
    }
}


JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1set_1pinned_1strings(JNIEnv *jniEnv,
                                                              jclass /*thisClass*/,
                                                              jlong handle_,
                                                              jlong table_,
                                                              jlong shard_,
                                                              jint columnIndex,
                                                              jlongArray timeoffsets_,
                                                              jobjectArray data_)
{
    qdb::jni::env env(jniEnv);
    try
    {
       qdb_time_t * timeoffsets = NULL;
       qdb_string_t * data        = NULL;

       qdb_handle_t handle      = reinterpret_cast<qdb_handle_t>(handle_);
       qdb_batch_table_t table  = reinterpret_cast<qdb_batch_table_t>(table_);
       qdb_timespec_t shard     = qdb_timespec_t {shard_, 0};

       jni::object_array values(env, data_);
       auto timeoffsets_guard  = jni::primitive_array::get_array_critical<jlong>(env, timeoffsets_);

       assert(values.size () == timeoffsets_guard.size());

       jni::column_pinner<jni::object_array, qdb_string_t> pinner {};
       pinner.pin(env, handle, table, columnIndex, timeoffsets_guard.size(), &shard, &timeoffsets, &data);

       assert(timeoffsets != NULL);
       assert(data != NULL);

       pinner.copy(env,
                   timeoffsets_guard.get(), values,
                   timeoffsets, data,
                   timeoffsets_guard.size());


       return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
      e.throw_new(env);
      return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1push(JNIEnv *jniEnv,
                                              jclass /*thisClass*/,
                                              jlong handle,
                                              jlong batchTable)
{

    qdb::jni::env env(jniEnv);
    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_batch_push((qdb_batch_table_t)batchTable));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1push_1async(JNIEnv *jniEnv,
                                                     jclass /*thisClass*/,
                                                     jlong handle,
                                                     jlong batchTable)
{
    qdb::jni::env env(jniEnv);

    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_batch_push_async((qdb_batch_table_t)batchTable));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1push_1fast(JNIEnv *jniEnv,
                                                    jclass /*thisClass*/,
                                                    jlong handle,
                                                    jlong batchTable)
{
    qdb::jni::env env(jniEnv);

    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_batch_push_fast((qdb_batch_table_t)batchTable));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}


JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1start_1row(JNIEnv *jniEnv,
                                                    jclass /*thisClass*/,
                                                    jlong batchTable,
                                                    jlong sec,
                                                    jlong nsec)
{
  qdb::jni::env env(jniEnv);

  qdb_timespec_t ts { sec, nsec };
  return qdb_ts_batch_start_row((qdb_batch_table_t)(batchTable),
                                &ts);
}


JNIEXPORT jint JNICALL
JavaCritical_net_quasardb_qdb_jni_qdb_ts_1batch_1start_1row(jlong batchTable,
                                                            jlong sec,
                                                            jlong nsec)
{
  qdb_timespec_t ts { sec, nsec };
  return qdb_ts_batch_start_row((qdb_batch_table_t)(batchTable),
                                &ts);
}


/****
 * 'regular' column set.
 *
 * The functions below are here to keep the old functionality of 'regular' batch
 * row set functions. Due to performance concerns these functions are set to be
 * deprecated once we're certain the pinned column approach is stable.
 */

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1double(JNIEnv * jniEnv,
                                                          jclass /* thisClass */,
                                                          jlong batchTable,
                                                          jlong index,
                                                          double val) {
  qdb::jni::env env(jniEnv);
  return qdb_ts_batch_row_set_double((qdb_batch_table_t)(batchTable),
                                     index,
                                     val);
}


JNIEXPORT jint JNICALL
JavaCritical_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1double(jlong batchTable,
                                                                  jlong index,
                                                                  double val) {
  return qdb_ts_batch_row_set_double((qdb_batch_table_t)(batchTable),
                                     index,
                                     val);
}


JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1int64(JNIEnv * jniEnv,
                                                         jclass /* thisClass */,
                                                         jlong batchTable,
                                                         jlong index,
                                                         jlong val) {
  qdb::jni::env env(jniEnv);
  return qdb_ts_batch_row_set_int64((qdb_batch_table_t)(batchTable),
                                    index,
                                    val);
}


JNIEXPORT jint JNICALL
JavaCritical_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1int64(jlong batchTable,
                                                                 jlong index,
                                                                 jlong val) {
  return qdb_ts_batch_row_set_int64((qdb_batch_table_t)(batchTable),
                                    index,
                                    val);
}


JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1timestamp(JNIEnv * jniEnv,
                                                             jclass /* thisClass */,
                                                             jlong batchTable,
                                                             jlong index,
                                                             jlong sec,
                                                             jlong nsec) {
  qdb::jni::env env(jniEnv);

  qdb_timespec_t ts { sec, nsec };

  return qdb_ts_batch_row_set_timestamp((qdb_batch_table_t)(batchTable),
                                        index,
                                        &ts);
}


JNIEXPORT jint JNICALL
JavaCritical_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1timestamp(jlong batchTable,
                                                                     jlong index,
                                                                     jlong sec,
                                                                     jlong nsec) {
  qdb_timespec_t ts { sec, nsec };

  return qdb_ts_batch_row_set_timestamp((qdb_batch_table_t)(batchTable),
                                        index,
                                        &ts);
}



JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1blob(JNIEnv * jniEnv,
                                                        jclass /* thisClass */,
                                                        jlong batchTable,
                                                        jlong index,
                                                        jobject bb) {
  qdb::jni::env env(jniEnv);


  void * addr = (bb == NULL
                 ? NULL
                 : env.instance().GetDirectBufferAddress(bb));

  qdb_size_t bytes = (addr == NULL
                      ? 0
                      : (qdb_size_t)env.instance().GetDirectBufferCapacity(bb));

  return qdb_ts_batch_row_set_blob((qdb_batch_table_t)(batchTable),
                                   index,
                                   addr,
                                   bytes);
}


JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1string(JNIEnv * jniEnv,
                                                          jclass /* thisClass */,
                                                          jlong batchTable,
                                                          jlong index,
                                                          jbyteArray bb) {
  qdb::jni::env env(jniEnv);

  qdb_error_t result;
  if (bb == NULL) {
    result = qdb_ts_batch_row_set_string((qdb_batch_table_t)(batchTable),
                                         index,
                                         NULL,
                                         0);
  } else {
    jni::guard::byte_array barry(jni::byte_array::get_bytes(env, bb));

    result = qdb_ts_batch_row_set_string((qdb_batch_table_t)(batchTable),
                                         index,
                                         (char const *)barry.ptr(),
                                         barry.len());
  }

  return result;
}



JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1push_1truncate(JNIEnv *jniEnv,
                                                        jclass /*thisClass*/,
                                                        jlong handle,
                                                        jlong batchTable,
                                                        jobjectArray ranges)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_size_t rangeCount = env.instance().GetArrayLength(ranges);
        std::unique_ptr<qdb_ts_range_t> nativeTimeRanges (new qdb_ts_range_t[rangeCount]);
        timeRangesToNative(env, ranges, rangeCount, nativeTimeRanges.get());

        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_batch_push_truncate((qdb_batch_table_t)batchTable,
                                       nativeTimeRanges.get(),
                                       rangeCount));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}


/****
 * 'pinned' column set
 *
 * These functions use the pinned columns under the hood directly. It's an experimental
 * approach but scales *much* better accross vast numbers of columns in the batch writer
 * state (i.e. will scale to millions of columns in state).
 */

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1pinned_1double(JNIEnv * jniEnv,
                                                                  jclass /* thisClass */,
                                                                  jlong pinnedColumn,
                                                                  jlong rowIndex,
                                                                  double val) {
  qdb::jni::env env(jniEnv);

  return qdb_e_ok;
}



JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1local_1table_1init(JNIEnv *jniEnv,
                                                     jclass /*thisClass*/,
                                                     jlong handle,
                                                     jstring alias,
                                                     jobjectArray columns,
                                                     jobject localTable)
{
    qdb::jni::env env(jniEnv);

    try
    {
        size_t columnCount = env.instance().GetArrayLength(columns);
        qdb_ts_column_info_t *nativeColumns =
            new qdb_ts_column_info_t[columnCount];

        columnsToNative(env, columns, nativeColumns, columnCount);

        qdb_local_table_t nativeLocalTable;

        qdb::jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_local_table_init(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, alias), nativeColumns,
                columnCount, &nativeLocalTable));
        setLong(env, localTable, reinterpret_cast<long>(nativeLocalTable));

        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1local_1table_1release(JNIEnv * /*env*/,
                                                        jclass /*thisClass*/,
                                                        jlong handle,
                                                        jlong localTable)
{
    qdb_release((qdb_handle_t)handle, (qdb_local_table_t)localTable);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1table_1get_1ranges(JNIEnv *jniEnv,
                                                     jclass /*thisClass*/,
                                                     jlong handle,
                                                     jlong localTable,
                                                     jobjectArray ranges)
{
    qdb::jni::env env(jniEnv);

    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            tableGetRanges(env, (qdb_handle_t)handle,
                           (qdb_local_table_t)localTable, ranges));
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1table_1next_1row(JNIEnv *jniEnv,
                                                   jclass /*thisClass*/,
                                                   jlong handle,
                                                   jlong localTable,
                                                   jobjectArray columns,
                                                   jobject output)
{
    qdb::jni::env env(jniEnv);

    try
    {
        size_t columnCount = env.instance().GetArrayLength(columns);
        qdb_ts_column_info_t *nativeColumns = (qdb_ts_column_info_t *)(malloc(
            columnCount * sizeof(qdb_ts_column_info_t)));

        columnsToNative(env, columns, nativeColumns, columnCount);

        jobject row;

        qdb_error_t err = jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            tableGetRow(env, (qdb_handle_t)handle,
                        (qdb_local_table_t)localTable, nativeColumns,
                        columnCount, &row));

        if (err == qdb_e_iterator_end)
        {
            return err;
        }

        assert(row != NULL);
        setReferenceValue(env, output, row);

        return err;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1insert(JNIEnv *jniEnv,
                                                 jclass /*thisClass*/,
                                                 jlong handle,
                                                 jstring alias,
                                                 jstring column,
                                                 jobjectArray points)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_size_t points_count = env.instance().GetArrayLength(points);
        qdb_ts_double_point *values = (qdb_ts_double_point *)(malloc(
            points_count * sizeof(qdb_ts_double_point)));

        doublePointsToNative(env, points, points_count, values);

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_double_insert((qdb_handle_t)handle,
                                 qdb::jni::string::get_chars_utf8(env, alias),
                                 qdb::jni::string::get_chars_utf8(env, column),
                                 values, points_count));

        free(values);
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1get_1ranges(JNIEnv *jniEnv,
                                                      jclass /*thisClass*/,
                                                      jlong handle,
                                                      jstring alias,
                                                      jstring column,
                                                      jobjectArray ranges,
                                                      jobject points)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_size_t rangeCount = env.instance().GetArrayLength(ranges);
        std::unique_ptr<qdb_ts_range_t> nativeTimeRanges (new qdb_ts_range_t[rangeCount]);

        timeRangesToNative(env, ranges, rangeCount, nativeTimeRanges.get());

        qdb_ts_double_point *native_points;
        qdb_size_t point_count;

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_double_get_ranges(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, alias),
                qdb::jni::string::get_chars_utf8(env, column), nativeTimeRanges.get(),
                rangeCount, &native_points, &point_count));

        setReferenceValue(
            env, points,
            nativeToDoublePoints(env, native_points, point_count).release());

        qdb_release((qdb_handle_t)handle, native_points);
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1aggregate(JNIEnv *jniEnv,
                                                    jclass /*thisClass*/,
                                                    jlong handle,
                                                    jstring alias,
                                                    jstring column,
                                                    jobjectArray input,
                                                    jobject output)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_size_t count = env.instance().GetArrayLength(input);

        qdb_ts_double_aggregation_t *aggregates =
            new qdb_ts_double_aggregation_t[count];

        doubleAggregatesToNative(env, input, count, aggregates);

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_double_aggregate(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, alias),
                qdb::jni::string::get_chars_utf8(env, column), aggregates,
                count));

        setReferenceValue(
            env, output,
            nativeToDoubleAggregates(env, aggregates, count).release());

        delete[] aggregates;
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1blob_1insert(JNIEnv *jniEnv,
                                               jclass /*thisClass*/,
                                               jlong handle,
                                               jstring alias,
                                               jstring column,
                                               jobjectArray points)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_size_t pointsCount = env.instance().GetArrayLength(points);
        qdb_ts_blob_point *values = new qdb_ts_blob_point[pointsCount];

        blobPointsToNative(env, points, pointsCount, values);

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_blob_insert((qdb_handle_t)handle,
                               qdb::jni::string::get_chars_utf8(env, alias),
                               qdb::jni::string::get_chars_utf8(env, column),
                               values, pointsCount));

        delete[] values;
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1blob_1get_1ranges(JNIEnv *jniEnv,
                                                    jclass /*thisClass*/,
                                                    jlong handle,
                                                    jstring alias,
                                                    jstring column,
                                                    jobjectArray ranges,
                                                    jobject points)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_size_t rangeCount = env.instance().GetArrayLength(ranges);
        qdb_ts_range_t *nativeTimeRanges = new qdb_ts_range_t[rangeCount];
        timeRangesToNative(env, ranges, rangeCount, nativeTimeRanges);

        qdb_ts_blob_point *nativePoints;
        qdb_size_t pointCount;

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_blob_get_ranges(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, alias),
                qdb::jni::string::get_chars_utf8(env, column), nativeTimeRanges,
                rangeCount, &nativePoints, &pointCount));

        // Note that at this point, we're moving the `nativePoints` buffer to
        // our java ecosystem, and will be picked up to be cleared by the JVM
        // garbage collector. As such, we do NOT call `qdb_release` here
        setReferenceValue(
            env, points,
            nativeToBlobPoints(env, nativePoints, pointCount).release());

        delete[] nativeTimeRanges;
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1string_1get_1ranges(JNIEnv *jniEnv,
                                                      jclass /*thisClass*/,
                                                      jlong handle,
                                                      jstring alias,
                                                      jstring column,
                                                      jobjectArray ranges,
                                                      jobject points)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_size_t rangeCount = env.instance().GetArrayLength(ranges);
        qdb_ts_range_t *nativeTimeRanges = new qdb_ts_range_t[rangeCount];
        timeRangesToNative(env, ranges, rangeCount, nativeTimeRanges);

        qdb_ts_string_point *nativePoints;
        qdb_size_t pointCount;

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_string_get_ranges(
                (qdb_handle_t)handle,
                qdb::jni::string::get_chars_utf8(env, alias),
                qdb::jni::string::get_chars_utf8(env, column), nativeTimeRanges,
                rangeCount, &nativePoints, &pointCount));

        // Note that at this point, we're moving the `nativePoints` buffer to
        // our java ecosystem, and will be picked up to be cleared by the JVM
        // garbage collector. As such, we do NOT call `qdb_release` here
        setReferenceValue(
            env, points,
            nativeToStringPoints(env, nativePoints, pointCount).release());

        delete[] nativeTimeRanges;
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1blob_1aggregate(JNIEnv *jniEnv,
                                                  jclass /*thisClass*/,
                                                  jlong handle,
                                                  jstring alias,
                                                  jstring column,
                                                  jobjectArray input,
                                                  jobject output)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_size_t count = env.instance().GetArrayLength(input);

        qdb_ts_blob_aggregation_t *aggregates =
            new qdb_ts_blob_aggregation_t[count];
        blobAggregatesToNative(env, input, count, aggregates);

        jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_blob_aggregate((qdb_handle_t)handle,
                                  qdb::jni::string::get_chars_utf8(env, alias),
                                  qdb::jni::string::get_chars_utf8(env, column),
                                  aggregates, count));

        setReferenceValue(
            env, output,
            nativeToBlobAggregates(env, aggregates, count).release());

        delete[] aggregates;
        return qdb_e_ok;
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return e.error();
    }
}
