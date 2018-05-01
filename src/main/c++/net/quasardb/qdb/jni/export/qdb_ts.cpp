#include <cassert>
#include <stdlib.h>
#include <qdb/ts.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../env.h"
#include "../string.h"
#include "../util/helpers.h"
#include "../util/ts_helpers.h"

namespace jni = qdb::jni;

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1create(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                         jstring alias, jlong shard_size, jobjectArray columns) {
  qdb::jni::env env(jniEnv);

  size_t column_count = env.instance().GetArrayLength(columns);
  qdb_ts_column_info_t * native_columns = new qdb_ts_column_info_t[column_count];

  columnsToNative(env, columns, native_columns, column_count);

  jint result = qdb_ts_create((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), (qdb_uint_t)shard_size, native_columns, column_count);
  releaseNative(native_columns, column_count);

  delete[] native_columns;
  return result;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1insert_1columns(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                  jstring alias, jobjectArray columns) {
  qdb::jni::env env(jniEnv);

  size_t column_count = env.instance().GetArrayLength(columns);
  qdb_ts_column_info_t * native_columns = new qdb_ts_column_info_t[column_count];

  columnsToNative(env, columns, native_columns, column_count);

  jint result = qdb_ts_insert_columns((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), native_columns, column_count);
  releaseNative(native_columns, column_count);

  delete[] native_columns;
  return result;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1list_1columns(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                jstring alias, jobject columns) {
  qdb::jni::env env(jniEnv);

  qdb_ts_column_info_t * native_columns;
  qdb_size_t column_count;

  qdb_error_t err = qdb_ts_list_columns((qdb_handle_t)handle, qdb::jni::string::get_chars_utf8(env, alias), &native_columns, &column_count);

  if (QDB_SUCCESS(err)) {
      setReferenceValue(env,
                        columns,
                        nativeToColumns(env, native_columns, column_count));
  }

  qdb_release((qdb_handle_t)handle, native_columns);

  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1local_1table_1init(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                     jstring alias, jobjectArray columns, jobject localTable) {
  qdb::jni::env env(jniEnv);

  size_t columnCount = env.instance().GetArrayLength(columns);
  qdb_ts_column_info_t * nativeColumns = new qdb_ts_column_info_t[columnCount];

  columnsToNative(env, columns, nativeColumns, columnCount);

  qdb_local_table_t nativeLocalTable;

  qdb_error_t err = qdb_ts_local_table_init((qdb_handle_t)handle,
                                            qdb::jni::string::get_chars_utf8(env, alias),
                                            nativeColumns,
                                            columnCount,
                                            &nativeLocalTable);
  if (QDB_SUCCESS(err)) {
    setLong(env, localTable, reinterpret_cast<long>(nativeLocalTable));
  }

  return err;
}

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1local_1table_1release(JNIEnv * /*env*/, jclass /*thisClass*/, jlong handle,
                                                        jlong localTable) {
  qdb_release((qdb_handle_t)handle,
              (qdb_local_table_t)localTable);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1table_1row_1append(JNIEnv * jniEnv, jclass /*thisClass*/, jlong localTable, jobject time, jobjectArray values) {
  qdb::jni::env env(jniEnv);

  qdb_size_t rowIndex;

  qdb_error_t err = tableRowAppend(env, (qdb_local_table_t)localTable, time, values, env.instance().GetArrayLength(values), &rowIndex);

  if (QDB_SUCCESS(err)) {
    // NOOP ?
  }

  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1push(JNIEnv * /*env*/, jclass /*thisClass*/, jlong localTable) {
  return qdb_ts_push((qdb_local_table_t)localTable);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1table_1get_1ranges(JNIEnv * jniEnv, jclass /*thisClass*/, jlong localTable, jobjectArray ranges) {
  qdb::jni::env env(jniEnv);

  int err = tableGetRanges(env, (qdb_local_table_t)localTable, ranges);

  if (QDB_SUCCESS(err)) {
    // NOOP ?
  }

  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1table_1next_1row(JNIEnv * jniEnv, jclass /*thisClass*/, jlong localTable, jobjectArray columns, jobject output) {
  qdb::jni::env env(jniEnv);

  size_t columnCount = env.instance().GetArrayLength(columns);
  qdb_ts_column_info_t * nativeColumns = (qdb_ts_column_info_t *)(malloc (columnCount * sizeof(qdb_ts_column_info_t)));

  columnsToNative(env, columns, nativeColumns, columnCount);

  jobject row;
  qdb_error_t err = tableGetRow(env, (qdb_local_table_t)localTable, nativeColumns, columnCount, &row);

  if (err == qdb_e_iterator_end) {
    return err;
  }

  if (QDB_SUCCESS(err)) {
    assert(row != NULL);
    setReferenceValue(env, output, row);
  }


  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1insert(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                 jstring alias, jstring column, jobjectArray points) {
  qdb::jni::env env(jniEnv);

  qdb_size_t points_count = env.instance().GetArrayLength(points);
  qdb_ts_double_point * values = (qdb_ts_double_point *)(malloc(points_count * sizeof(qdb_ts_double_point)));

  doublePointsToNative(env, points, points_count, values);

  qdb_error_t err = qdb_ts_double_insert((qdb_handle_t)handle,
                                         qdb::jni::string::get_chars_utf8(env, alias),
                                         qdb::jni::string::get_chars_utf8(env, column),
                                         values,
                                         points_count);

  free(values);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1get_1ranges(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                      jstring alias, jstring column, jobjectArray ranges, jobject points) {
  qdb::jni::env env(jniEnv);

  qdb_size_t rangeCount = env.instance().GetArrayLength(ranges);
  qdb_ts_range_t * nativeTimeRanges = (qdb_ts_range_t *)(malloc(rangeCount * sizeof(qdb_ts_range_t)));

  timeRangesToNative(env, ranges, rangeCount, nativeTimeRanges);

  qdb_ts_double_point * native_points;
  qdb_size_t point_count;

  qdb_error_t err = qdb_ts_double_get_ranges((qdb_handle_t)handle,
                                             qdb::jni::string::get_chars_utf8(env, alias),
                                             qdb::jni::string::get_chars_utf8(env, column),
                                             nativeTimeRanges,
                                             rangeCount,
                                             &native_points,
                                             &point_count);


  if (QDB_SUCCESS(err)) {
      setReferenceValue(env,
                        points,
                        nativeToDoublePoints(env, native_points, point_count).release());
  }

  qdb_release((qdb_handle_t)handle, native_points);

  free(nativeTimeRanges);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1aggregate(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                    jstring alias, jstring column, jobjectArray input, jobject output) {
  qdb::jni::env env(jniEnv);

  qdb_size_t count = env.instance().GetArrayLength(input);

  qdb_ts_double_aggregation_t * aggregates = new qdb_ts_double_aggregation_t[count];

  doubleAggregatesToNative(env, input, count, aggregates);

  qdb_error_t err = qdb_ts_double_aggregate((qdb_handle_t)handle,
                                            qdb::jni::string::get_chars_utf8(env, alias),
                                            qdb::jni::string::get_chars_utf8(env, column),
                                            aggregates,
                                            count);

  if (QDB_SUCCESS(err)) {
      setReferenceValue(env,
                        output,
                        nativeToDoubleAggregates(env, aggregates, count).release());
  }

  delete[] aggregates;
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1blob_1insert(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                 jstring alias, jstring column, jobjectArray points) {
  qdb::jni::env env(jniEnv);

  qdb_size_t pointsCount = env.instance().GetArrayLength(points);
  qdb_ts_blob_point * values = new qdb_ts_blob_point[pointsCount];


  blobPointsToNative(env, points, pointsCount, values);

  qdb_error_t err = qdb_ts_blob_insert((qdb_handle_t)handle,
                                       qdb::jni::string::get_chars_utf8(env, alias),
                                       qdb::jni::string::get_chars_utf8(env, column),
                                       values,
                                       pointsCount);

  delete[] values;
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1blob_1get_1ranges(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                    jstring alias, jstring column, jobjectArray ranges, jobject points) {
  qdb::jni::env env(jniEnv);

  qdb_size_t rangeCount = env.instance().GetArrayLength(ranges);
  qdb_ts_range_t * nativeTimeRanges = new qdb_ts_range_t[rangeCount];
  timeRangesToNative(env, ranges, rangeCount, nativeTimeRanges);

  qdb_ts_blob_point * nativePoints;
  qdb_size_t pointCount;

  qdb_error_t err = qdb_ts_blob_get_ranges((qdb_handle_t)handle,
                                           qdb::jni::string::get_chars_utf8(env, alias),
                                           qdb::jni::string::get_chars_utf8(env, column),
                                           nativeTimeRanges,
                                           rangeCount,
                                           &nativePoints,
                                           &pointCount);

  if (QDB_SUCCESS(err)) {
      // Note that at this point, we're moving the `nativePoints` buffer to
      // our java ecosystem, and will be picked up to be cleared by the JVM
      // garbage collector. As such, we do NOT call `qdb_release` here
      setReferenceValue(env,
                        points,
                        nativeToBlobPoints(env, nativePoints, pointCount).release());
  }

  delete[] nativeTimeRanges;
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1blob_1aggregate(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                                    jstring alias, jstring column, jobjectArray input, jobject output) {
  qdb::jni::env env(jniEnv);

  qdb_size_t count = env.instance().GetArrayLength(input);

  qdb_ts_blob_aggregation_t * aggregates = new qdb_ts_blob_aggregation_t[count];
  blobAggregatesToNative(env, input, count, aggregates);

  qdb_error_t err = qdb_ts_blob_aggregate((qdb_handle_t)handle,
                                            qdb::jni::string::get_chars_utf8(env, alias),
                                            qdb::jni::string::get_chars_utf8(env, column),
                                            aggregates,
                                            count);

  if (QDB_SUCCESS(err)) {
    setReferenceValue(env,
                      output,
                      nativeToBlobAggregates(env, aggregates, count).release());
  }

  delete[] aggregates;
  return err;
}
