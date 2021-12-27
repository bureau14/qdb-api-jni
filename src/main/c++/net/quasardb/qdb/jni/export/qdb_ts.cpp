#include <cassert>
#include <string.h> // memcpy
#include <qdb/ts.h>
#include <stdlib.h>
#include <iostream>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../env.h"
#include "../detail/native_ptr.hpp"
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
#include "../byte_buffer.h"

#include "qdb_ts.h"

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

JNIEXPORT jobjectArray JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1list_1columns(JNIEnv *jniEnv,
                                                jclass /*thisClass*/,
                                                jlong handle,
                                                jstring alias)
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

        return nativeToColumns(env, native_columns, column_count).release();
    }
    catch (jni::exception const &e)
    {
        //! :XXX: memory leak for native_columns? use unique_ptr instead?
        e.throw_new(env);
        return NULL;
    }
}

JNIEXPORT jlong JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1table_1init(JNIEnv *jniEnv,
                                                     jclass /*thisClass*/,
                                                     jlong handle_,
                                                     jobjectArray tableColumns)
{
    qdb::jni::env env(jniEnv);

    qdb_handle_t handle   = reinterpret_cast<qdb_handle_t>(handle_);
    qdb_batch_table_t ret {nullptr};

    try
    {
        size_t columnInfoCount = env.instance().GetArrayLength(tableColumns);
        std::unique_ptr<qdb_ts_batch_column_info_t> columnInfo {batchColumnInfo(env,
                                                                                tableColumns)};

        qdb::jni::exception::throw_if_error(
            (qdb_handle_t)handle,
            qdb_ts_batch_table_init(handle,
                                    columnInfo.get(),
                                    columnInfoCount,
                                    &ret));

        assert(ret != nullptr);

        return jni::native_ptr::to_java(ret);
    }
    catch (jni::exception const &e)
    {
        if (ret != nullptr) {
          qdb_release(handle, ret);
        }
        e.throw_new(env);
        return -1;
    }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1batch_1release_1columns_1memory(JNIEnv *jniEnv,
                                                                  jclass /*thisClass*/,
                                                                  jlong handle_,
                                                                  jlong batchTable_) {
    qdb::jni::env env(jniEnv);

    qdb_handle_t handle          = reinterpret_cast<qdb_handle_t>(handle_);
    qdb_batch_table_t batchTable = reinterpret_cast<qdb_batch_table_t>(batchTable_);;

    try
    {

      jni::exception::throw_if_error(handle,
                                     qdb_ts_batch_release_columns_memory(batchTable));


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
        // qdb_ts_column_type_t * column_types[column_count];
        // to_column_type_array(env, columns, column_types, column_count);
        auto column_types_guard = jni::primitive_array::get_array_critical<qdb_ts_column_type_t>(env, columnTypes);

        // Sanity check
        assert(column_types_guard.size() == env.instance().GetArrayLength(columnTypes));

        /**
         * Push a single shard using pinned columns. We first pin all the columns
         * that we expect.
         */
        // size_t rowCount = env.instance().GetArrayLength(rows);

        // XXX(leon): nothing here?!
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



JNIEXPORT jlong JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1local_1table_1init(JNIEnv *jniEnv,
                                                     jclass /*thisClass*/,
                                                     jlong handle_,
                                                     jstring alias,
                                                     jobjectArray columns)
{
    qdb::jni::env env(jniEnv);


    qdb_handle_t handle = jni::native_ptr::from_java<qdb_handle_t>(handle_);
    qdb_local_table_t ret{nullptr};

    try
    {
        size_t columnCount = env.instance().GetArrayLength(columns);
        std::unique_ptr<qdb_ts_column_info_t> nativeColumns{new qdb_ts_column_info_t[columnCount]};

        columnsToNative(env, columns, nativeColumns.get(), columnCount);

        qdb::jni::exception::throw_if_error((qdb_handle_t)handle,
                                            qdb_ts_local_table_init((qdb_handle_t)handle,
                                                                    qdb::jni::string::get_chars_utf8(env, alias),
                                                                    nativeColumns.get(),
                                                                    columnCount,
                                                                    &ret));

        return jni::native_ptr::to_java(ret);
    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return -1;
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

JNIEXPORT jobject JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1table_1next_1row(JNIEnv *jniEnv,
                                                   jclass /*thisClass*/,
                                                   jlong handle,
                                                   jlong localTable,
                                                   jobjectArray columns)
{
    qdb::jni::env env(jniEnv);

    try
    {
        size_t columnCount = env.instance().GetArrayLength(columns);

        std::unique_ptr<qdb_ts_column_info_t> nativeColumns{new qdb_ts_column_info_t[columnCount]};

        columnsToNative(env, columns, nativeColumns.get(), columnCount);

        return tableGetRow(env, (qdb_handle_t)handle,
                           (qdb_local_table_t)localTable,
                           nativeColumns.get(),
                           columnCount);

    }
    catch (jni::exception const &e)
    {
        e.throw_new(env);
        return NULL;
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



qdb_exp_batch_push_table_t &
_table_from_tables(qdb_exp_batch_push_table_t * tables,
                   jlong tableNum) {

  return tables[tableNum];
}


qdb_exp_batch_push_table_t &
_table_from_tables(jlong batchTables,
                   jlong tableNum) {
  return _table_from_tables(reinterpret_cast<qdb_exp_batch_push_table_t *>(batchTables),
                            tableNum);
}


qdb_exp_batch_push_column_t &
_batch_column_from_tables(qdb_exp_batch_push_table_t * tables,
                          jlong tableNum,
                          jlong columnNum) {

  qdb_exp_batch_push_table_data_t & data = tables[tableNum].data;

  assert(columnNum < data.column_count);

  return const_cast<qdb_exp_batch_push_column_t &>(data.columns[columnNum]);
}

qdb_exp_batch_push_column_t &
_batch_column_from_tables(jlong batchTables,
                          jlong tableNum,
                          jlong columnNum) {
  return _batch_column_from_tables(reinterpret_cast<qdb_exp_batch_push_table_t *>(batchTables),
                                   tableNum,
                                   columnNum);
}

/**
 * Batch writer
 */


void
_timestamps_from_timespecs(int n,
                           jlong * sec,
                           jlong * nsec,
                           qdb_timespec_t * out) {
  // We do a fast copy and rely on this assumption
  static_assert(sizeof(jlong) == sizeof(qdb_time_t));

  for (int i = 0; i < n; ++i) {
    out[i].tv_sec  = sec[i];
    out[i].tv_nsec = nsec[i];
  }
}

void
_timestamps_from_timespecs(qdb::jni::env & env,
                           jobject values,
                           qdb_timespec_t * out) {
    jclass timespecClass = jni::object::get_class(env, values);
    jfieldID secField    = jni::introspect::lookup_field(env, timespecClass,
                                                      "sec", "[J");
    jfieldID nsecField   = jni::introspect::lookup_field(env, timespecClass,
                                                       "nsec", "[J");
    jlongArray secArray  = (jlongArray)env.instance().GetObjectField(values, secField);
    jlongArray nsecArray = (jlongArray)env.instance().GetObjectField(values, nsecField);

    auto sec_guard       = jni::primitive_array::get_array_critical<jlong>(env, secArray);
    auto nsec_guard      = jni::primitive_array::get_array_critical<jlong>(env, nsecArray);

    _timestamps_from_timespecs(sec_guard.size(),
                               sec_guard.get(),
                               nsec_guard.get(),
                               out);

}


JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1double(JNIEnv * jniEnv,
                                                                        jclass /* thisClass */,
                                                                        jlong batchTables,
                                                                        jlong tableNum,
                                                                        jlong columnNum,
                                                                        jstring name,
                                                                        jdoubleArray values)
{
    qdb::jni::env env(jniEnv);
    try {
      qdb_exp_batch_push_column_t & column = _batch_column_from_tables(batchTables,
                                                                       tableNum,
                                                                       columnNum);

      auto arr = jni::primitive_array::get_array_critical<double>(env, values);
      column.name = jni::string::get_chars_utf8(env, name).as_qdb();
      column.data_type = qdb_ts_column_double;
      column.data.doubles = arr.copy();

    } catch (jni::exception const & e) {
      e.throw_new(env);
    }
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1int64(JNIEnv * jniEnv,
                                                                       jclass /* thisClass */,
                                                                       jlong batchTables,
                                                                       jlong tableNum,
                                                                       jlong columnNum,
                                                                       jstring name,
                                                                       jlongArray values)
{
    qdb::jni::env env(jniEnv);
    try {
      qdb_exp_batch_push_column_t & column = _batch_column_from_tables(batchTables,
                                                                       tableNum,
                                                                       columnNum);

      auto arr = jni::primitive_array::get_array_critical<qdb_int_t>(env, values);

      column.name = jni::string::get_chars_utf8(env, name).as_qdb();
      column.data_type = qdb_ts_column_int64;
      column.data.ints = arr.copy();
    } catch (jni::exception const & e) {
      e.throw_new(env);
    }
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1blob(JNIEnv * jniEnv,
                                                                      jclass /* thisClass */,
                                                                      jlong batchTables,
                                                                      jlong tableNum,
                                                                      jlong columnNum,
                                                                      jstring name,
                                                                      jobjectArray values_)
{
    qdb::jni::env env(jniEnv);
    try {
      qdb_exp_batch_push_column_t & column = _batch_column_from_tables(batchTables,
                                                                       tableNum,
                                                                       columnNum);

      jni::object_array values(env, values_);

      column.name = jni::string::get_chars_utf8(env, name).as_qdb();
      column.data_type = qdb_ts_column_blob;

      auto ret = std::make_unique<qdb_blob_t[]>(values.size());

      for (qdb_size_t i = 0; i < values.size(); ++i) {
        jobject bb = values.get(i);
        jni::byte_buffer::as_qdb_blob(env, bb, ret[i]);
      }

      column.data.blobs = ret.release();

    } catch (jni::exception const & e) {
      e.throw_new(env);
    }
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1string(JNIEnv * jniEnv,
                                                                        jclass /* thisClass */,
                                                                        jlong batchTables,
                                                                        jlong tableNum,
                                                                        jlong columnNum,
                                                                        jstring name,
                                                                        jobjectArray values_)
{
    qdb::jni::env env(jniEnv);
    try {
      qdb_exp_batch_push_column_t & column = _batch_column_from_tables(batchTables,
                                                                       tableNum,
                                                                       columnNum);

      jni::object_array values(env, values_);

      column.name = jni::string::get_chars_utf8(env, name).as_qdb();
      column.data_type = qdb_ts_column_string;

      auto ret = std::make_unique<qdb_string_t[]>(values.size());

      for (qdb_size_t i = 0; i < values.size(); ++i) {
        jobject bb = values.get(i);
        jni::byte_buffer::as_qdb_string(env, bb, ret[i]);
      }

      column.data.strings = ret.release();

    } catch (jni::exception const & e) {
      e.throw_new(env);
    }
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1timestamp(JNIEnv * jniEnv,
                                                                           jclass /* thisClass */,
                                                                           jlong batchTables,
                                                                           jlong tableNum,
                                                                           jlong columnNum,
                                                                           jstring name,
                                                                           jobject values)
{
    qdb::jni::env env(jniEnv);
    try {
      qdb_exp_batch_push_table_t * xs    = reinterpret_cast<qdb_exp_batch_push_table_t *>(batchTables);
      qdb_exp_batch_push_table_t & table = xs[tableNum];

      qdb_size_t values_count            = table.data.row_count;
      auto timestamps                    = std::make_unique<qdb_timespec_t[]>(values_count);

      _timestamps_from_timespecs(env,
                                 values,
                                 timestamps.get());

      qdb_exp_batch_push_column_t & column = _batch_column_from_tables(xs,
                                                                       tableNum,
                                                                       columnNum);

      column.name = jni::string::get_chars_utf8(env, name).as_qdb();
      column.data_type = qdb_ts_column_timestamp;
      column.data.timestamps = timestamps.release();

    } catch (jni::exception const & e) {
      e.throw_new(env);
    }
}


JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1table_1data(JNIEnv * jniEnv,
                                                               jclass /* thisClass */,
                                                               jlong batchTables,
                                                               jlong tableNum,
                                                               jstring tableName,
                                                               jobject timespecs_) {
  qdb::jni::env env(jniEnv);
  try {

    qdb_exp_batch_push_table_t & table =  _table_from_tables(batchTables, tableNum);

    table.name = jni::string::get_chars_utf8(env, tableName).as_qdb();

    _timestamps_from_timespecs(env,
                               timespecs_,
                               const_cast<qdb_timespec_t *>(table.data.timestamps));

  } catch (jni::exception const & e) {
    e.throw_new(env);
  }
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1table_1set_1truncate_1ranges(JNIEnv * jniEnv,
                                                                           jclass /* thisClass */,
                                                                           jlong batchTables,
                                                                           jlong tableNum,
                                                                           jobjectArray ranges) {
  qdb::jni::env env(jniEnv);
  try {

    qdb_exp_batch_push_table_t & table =  _table_from_tables(batchTables, tableNum);

    jni::object_array arr(env, ranges);
    auto ranges = std::make_unique<qdb_ts_range_t[]>(arr.size());
    for (qdb_size_t i = 0; i < arr.size(); ++i) {
      timeRangeToNative(env, arr[i], ranges[i]);
    }


    table.truncate_range_count = arr.size();
    table.truncate_ranges = ranges.release();


  } catch (jni::exception const & e) {
    e.throw_new(env);
  }
}


JNIEXPORT jlong JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1prepare(JNIEnv * jniEnv,
                                                      jclass /* thisClass */,
                                                      jlongArray rowCount_,
                                                      jlongArray columnCount_) {
  qdb::jni::env env(jniEnv);
  try {
    auto row_guard = jni::primitive_array::get_array_critical<jlong>(env, rowCount_);
    auto column_guard = jni::primitive_array::get_array_critical<jlong>(env, columnCount_);

    assert(row_guard.size() == column_guard.size());

    auto ret = std::make_unique<qdb_exp_batch_push_table_t[]>(row_guard.size());

    for (qdb_size_t i = 0; i < row_guard.size(); ++i) {
      qdb_size_t row_count        = row_guard[i];
      qdb_size_t column_count     = column_guard[i];

      ret[i].data.row_count       = row_count;
      ret[i].data.column_count    = column_count;
      ret[i].data.timestamps      = new qdb_timespec_t[row_count]();
      ret[i].data.columns         = new qdb_exp_batch_push_column_t[column_count]();

      ret[i].truncate_ranges      = nullptr;
      ret[i].truncate_range_count = 0;
    }

    return reinterpret_cast<jlong>(ret.release());

  } catch (jni::exception const & e) {
    e.throw_new(env);
    return e.error();
  }
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1release(JNIEnv * jniEnv,
                                                      jclass /* thisClass */,
                                                      jlong tables,
                                                      jlong tables_count) {
  qdb_exp_batch_push_table_t * xs = reinterpret_cast<qdb_exp_batch_push_table_t *>(tables);

  for (jlong i = 0; i < tables_count; ++i) {
    for (jlong j = 0; j < xs[i].data.column_count; ++j) {
      delete xs[i].data.columns[j].name.data;

      switch (xs[i].data.columns[j].data_type) {
      case qdb_ts_column_double:
        delete[] xs[i].data.columns[j].data.doubles;
        break;
      case qdb_ts_column_int64:
        delete[] xs[i].data.columns[j].data.ints;
        break;
      case qdb_ts_column_blob:
        {
          for (jlong k = 0; k < xs[i].data.row_count; ++k) {
            if (xs[i].data.columns[j].data.blobs[k].content != nullptr) {
              free((void *)(xs[i].data.columns[j].data.blobs[k].content));
            }
          }
          delete[] xs[i].data.columns[j].data.blobs;
          break;
        }
      case qdb_ts_column_string:
        {
          for (jlong k = 0; k < xs[i].data.row_count; ++k) {
            if (xs[i].data.columns[j].data.strings[k].data != nullptr) {
              free((void *)(xs[i].data.columns[j].data.strings[k].data));
            }
          }
          delete[] xs[i].data.columns[j].data.strings;
          break;
        }
      case qdb_ts_column_timestamp:
        delete[] xs[i].data.columns[j].data.timestamps;
        break;
      default:
        throw new jni::exception(qdb_e_incompatible_type,
                                 "Unrecognized column type");
      }
    }

    if (xs[i].truncate_ranges != nullptr) {
      delete xs[i].truncate_ranges;
    }

    delete xs[i].name.data;
    delete[] xs[i].data.timestamps;
    delete[] xs[i].data.columns;
  }

  delete[] xs;
}

JNIEXPORT jlong JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1push(JNIEnv * jniEnv,
                                                   jclass /* thisClass */,
                                                   jlong handle,
                                                   jint pushMode,
                                                   jlong tables_,
                                                   jlong tables_count) {
  qdb::jni::env env(jniEnv);
  try {
    qdb_exp_batch_push_table_t * tables = reinterpret_cast<qdb_exp_batch_push_table_t *>(tables_);

    qdb_exp_batch_push_mode_t push_mode = qdb_exp_batch_push_transactional;

    // Needs to be kept in sync with the Writer.PushMode enum
    switch (pushMode) {
    case 0:
      push_mode = qdb_exp_batch_push_transactional;
      break;
    case 1:
      push_mode = qdb_exp_batch_push_async;
      break;
    case 2:
      push_mode = qdb_exp_batch_push_fast;
      break;
    case 3:
      push_mode = qdb_exp_batch_push_truncate;
      break;
    default:
        throw new jni::exception(qdb_e_incompatible_type,
                                 "Unrecognized push mode");
    };

    qdb::jni::exception::throw_if_error((qdb_handle_t)handle,
                                        qdb_exp_batch_push((qdb_handle_t)handle,
                                                           push_mode,
                                                           tables,
                                                           NULL,
                                                           tables_count));

    return qdb_e_ok;
  } catch (jni::exception const & e) {
    e.throw_new(env);
    return e.error();
  }

}
