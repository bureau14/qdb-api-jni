#include <qdb/ts.h>

#include "helpers.h"
#include "ts_helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1create(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                         jstring alias, jobjectArray columns) {
  size_t column_count = env->GetArrayLength(columns);
  qdb_ts_column_info * native_columns = new qdb_ts_column_info[column_count];

  columnsToNative(env, columns, native_columns, column_count);

  jint result = qdb_ts_create((qdb_handle_t)handle, StringUTFChars(env, alias), native_columns, column_count);
  releaseNative(native_columns, column_count);

  delete[] native_columns;
  return result;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1insert_1columns(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                  jstring alias, jobjectArray columns) {
  size_t column_count = env->GetArrayLength(columns);
  qdb_ts_column_info * native_columns = new qdb_ts_column_info[column_count];

  columnsToNative(env, columns, native_columns, column_count);

  jint result = qdb_ts_insert_columns((qdb_handle_t)handle, StringUTFChars(env, alias), native_columns, column_count);
  releaseNative(native_columns, column_count);

  delete[] native_columns;
  return result;
}


JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1list_1columns(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                jstring alias, jobject columns) {
  qdb_ts_column_info * native_columns;
  qdb_size_t column_count;

  qdb_error_t err = qdb_ts_list_columns((qdb_handle_t)handle, StringUTFChars(env, alias), &native_columns, &column_count);

  if (QDB_SUCCESS(err)) {
    jobjectArray array;
    nativeToColumns(env, native_columns, column_count, &array);
    setReferenceValue(env, columns, array);
  }

  qdb_release((qdb_handle_t)handle, native_columns);

  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1insert(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                 jstring alias, jstring column, jobjectArray points) {
  qdb_size_t points_count = env->GetArrayLength(points);
  qdb_ts_double_point * values = new qdb_ts_double_point[points_count];

  doublePointsToNative(env, points, points_count, values);

  qdb_error_t err = qdb_ts_double_insert((qdb_handle_t)handle,
                                         StringUTFChars(env, alias),
                                         StringUTFChars(env, column),
                                         values,
                                         points_count);

  fflush(stdout);

  delete[] values;
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1get_1ranges(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                      jstring alias, jstring column, jobjectArray ranges, jobject points) {
  qdb_size_t range_count = env->GetArrayLength(ranges);
  qdb_ts_range_t * native_ranges = new qdb_ts_range_t[range_count];
  rangesToNative(env, ranges, range_count, native_ranges);

  qdb_ts_double_point * native_points;
  qdb_size_t point_count;

  qdb_error_t err = qdb_ts_double_get_ranges((qdb_handle_t)handle,
                                             StringUTFChars(env, alias),
                                             StringUTFChars(env, column),
                                             native_ranges,
                                             range_count,
                                             &native_points,
                                             &point_count);

  if (QDB_SUCCESS(err)) {
    jobjectArray array;
    nativeToDoublePoints(env, native_points, point_count, &array);
    setReferenceValue(env, points, array);
    fflush(stdout);
  }

  qdb_release((qdb_handle_t)handle, native_points);

  delete[] native_ranges;
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1aggregate(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                    jstring alias, jstring column, jobjectArray input, jobject output) {
  qdb_size_t count = env->GetArrayLength(input);

  qdb_ts_double_aggregation_t * aggregates = new qdb_ts_double_aggregation_t[count];
  doubleAggregatesToNative(env, input, count, aggregates);

  qdb_error_t err = qdb_ts_double_aggregate((qdb_handle_t)handle,
                                            StringUTFChars(env, alias),
                                            StringUTFChars(env, column),
                                            aggregates,
                                            count);

  if (QDB_SUCCESS(err)) {
    jobjectArray array;
    nativeToDoubleAggregates(env, aggregates, count, &array);
    setReferenceValue(env, output, array);
  }

  delete[] aggregates;
  return err;
}
