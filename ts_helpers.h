#pragma once

#include <jni.h>
#include <qdb/ts.h>

#include "helpers.h"

void timespecToNative(JNIEnv *, jobject, qdb_timespec_t *);
void nativeToTimespec(JNIEnv *, qdb_timespec_t, jobject *);
void nativeToByteBuffer(JNIEnv * env, void const * content, qdb_size_t content_length, jobject * output);

void columnsToNative(JNIEnv * env, jobjectArray columns, qdb_ts_column_info * native_columns, size_t column_count);
void releaseNative(qdb_ts_column_info * native_columns, size_t column_count);
void nativeToColumns(JNIEnv * env, qdb_ts_column_info * nativeColumns, size_t column_count, jobjectArray * columns);


void doublePointToNative(JNIEnv * env, jobject input, qdb_ts_double_point * native);
void doublePointsToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_point * native);
void nativeToDoublePoint(JNIEnv * env, qdb_ts_double_point native, jobject * output);
void nativeToDoublePoints(JNIEnv * env, qdb_ts_double_point * native, size_t count, jobjectArray * output);

void blobPointToNative(JNIEnv * env, jobject input, qdb_ts_blob_point * native);
void blobPointsToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_blob_point * native);
void nativeToBlobPoint(JNIEnv * env, qdb_ts_blob_point native, jobject * output);
void nativeToBlobPoints(JNIEnv * env, qdb_ts_blob_point * native, size_t count, jobjectArray * output);

void rangeToNative(JNIEnv *env, jobject input, qdb_ts_range_t * native);
void rangesToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_range_t * native);
void nativeToRange(JNIEnv * env, qdb_ts_range_t native, jobject * output);
void doubleAggregateToNative(JNIEnv *env, jobject input, qdb_ts_double_aggregation_t * native);
void doubleAggregatesToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_aggregation_t * native);
void nativeToDoubleAggregate(JNIEnv * env, qdb_ts_double_aggregation native, jobject * output);
void nativeToDoubleAggregates(JNIEnv * env, qdb_ts_double_aggregation * native, size_t count, jobjectArray * output);
