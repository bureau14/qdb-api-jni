#include <stdlib.h>
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
  qdb_ts_double_point * values = (qdb_ts_double_point *)(malloc(points_count * sizeof(qdb_ts_double_point)));

  doublePointsToNative(env, points, points_count, values);

  for (qdb_size_t i = 0; i < points_count; ++i) {
    printf("[JNI] 1 inserting double point: %lf, ts: %ul.%ul\n", values[i].value, values[i].timestamp.tv_sec, values[i].timestamp.tv_nsec);
    fflush(stdout);
  }

  qdb_error_t err = qdb_ts_double_insert((qdb_handle_t)handle,
                                         StringUTFChars(env, alias),
                                         StringUTFChars(env, column),
                                         values,
                                         points_count);

  for (qdb_size_t i = 0; i < points_count; ++i) {
    printf("[JNI] 2 inserting double point: %lf, ts: %ul.%ul\n", values[i].value, values[i].timestamp.tv_sec, values[i].timestamp.tv_nsec);
    fflush(stdout);
  }

  free(values);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1get_1ranges(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                      jstring alias, jstring column, jobjectArray filteredRanges, jobject points) {
  qdb_size_t filteredRangeCount = env->GetArrayLength(filteredRanges);
  qdb_ts_filtered_range_t * nativeFilteredRanges = (qdb_ts_filtered_range_t *)(malloc(filteredRangeCount * sizeof(qdb_ts_filtered_range_t)));

  filteredRangesToNative(env, filteredRanges, filteredRangeCount, nativeFilteredRanges);

  qdb_ts_double_point * native_points;
  qdb_size_t point_count;

  qdb_error_t err = qdb_ts_double_get_ranges((qdb_handle_t)handle,
                                             StringUTFChars(env, alias),
                                             StringUTFChars(env, column),
                                             nativeFilteredRanges,
                                             filteredRangeCount,
                                             &native_points,
                                             &point_count);


  if (QDB_SUCCESS(err)) {
    for (qdb_size_t i = 0; i < point_count; ++i) {
      printf("[JNI] retrieved double point: %lf, ts: %ul.%ul\n", native_points[i].value, native_points[i].timestamp.tv_sec, native_points[i].timestamp.tv_nsec);
      fflush(stdout);
    }

    jobjectArray array;
    nativeToDoublePoints(env, native_points, point_count, &array);
    setReferenceValue(env, points, array);
  }

  qdb_release((qdb_handle_t)handle, native_points);

  free(nativeFilteredRanges);
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

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1blob_1insert(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                 jstring alias, jstring column, jobjectArray points) {
  qdb_size_t pointsCount = env->GetArrayLength(points);
  qdb_ts_blob_point * values = new qdb_ts_blob_point[pointsCount];


  blobPointsToNative(env, points, pointsCount, values);

  qdb_error_t err = qdb_ts_blob_insert((qdb_handle_t)handle,
                                       StringUTFChars(env, alias),
                                       StringUTFChars(env, column),
                                       values,
                                       pointsCount);

  delete[] values;
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1blob_1get_1ranges(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                    jstring alias, jstring column, jobjectArray filteredRanges, jobject points) {
  qdb_size_t filteredRangeCount = env->GetArrayLength(filteredRanges);
  qdb_ts_filtered_range_t * nativeFilteredRanges = new qdb_ts_filtered_range_t[filteredRangeCount];
  filteredRangesToNative(env, filteredRanges, filteredRangeCount, nativeFilteredRanges);

  qdb_ts_blob_point * nativePoints;
  qdb_size_t pointCount;

  qdb_error_t err = qdb_ts_blob_get_ranges((qdb_handle_t)handle,
                                           StringUTFChars(env, alias),
                                           StringUTFChars(env, column),
                                           nativeFilteredRanges,
                                           filteredRangeCount,
                                           &nativePoints,
                                           &pointCount);

  if (QDB_SUCCESS(err)) {
    jobjectArray array;

    // Note that at this point, we're moving the `nativePoints` buffer to
    // our java ecosystem, and will be picked up to be cleared by the JVM
    // garbage collector. As such, we do NOT call `qdb_release` here
    nativeToBlobPoints(env, nativePoints, pointCount, &array);
    setReferenceValue(env, points, array);
    fflush(stdout);
  }

  delete[] nativeFilteredRanges;
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1blob_1aggregate(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                    jstring alias, jstring column, jobjectArray input, jobject output) {
  qdb_size_t count = env->GetArrayLength(input);

  qdb_ts_blob_aggregation_t * aggregates = new qdb_ts_blob_aggregation_t[count];
  blobAggregatesToNative(env, input, count, aggregates);

  qdb_error_t err = qdb_ts_blob_aggregate((qdb_handle_t)handle,
                                            StringUTFChars(env, alias),
                                            StringUTFChars(env, column),
                                            aggregates,
                                            count);

  if (QDB_SUCCESS(err)) {
    jobjectArray array;
    nativeToBlobAggregates(env, aggregates, count, &array);
    setReferenceValue(env, output, array);
  }

  delete[] aggregates;
  return err;
}
