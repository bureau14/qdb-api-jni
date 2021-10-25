#pragma once

#include <jni.h>
#include <qdb/ts.h>

#include "../guard/local_ref.h"
#include "helpers.h"

namespace qdb
{
namespace jni
{
class env;
};
}; // namespace qdb

void timespecToNative(qdb::jni::env &, jobject, qdb_timespec_t &);
void timespecToNative(qdb::jni::env &, jobject, qdb_timespec_t *);

qdb::jni::guard::local_ref<jobject> nativeToTimespec(qdb::jni::env &,
                                                     qdb_timespec_t);

jobject nativeToByteBuffer(qdb::jni::env &env,
                           void const *content,
                           qdb_size_t content_length);
void
timeRangeToNative(qdb::jni::env &env, jobject input, qdb_ts_range_t &native);
void
timeRangeToNative(qdb::jni::env &env, jobject input, qdb_ts_range_t *native);
void timeRangesToNative(qdb::jni::env &env,
                        jobjectArray input,
                        size_t count,
                        qdb_ts_range_t *native);

qdb::jni::guard::local_ref<jobject> nativeToTimeRange(qdb::jni::env &env,
                                                      qdb_ts_range_t native);

qdb_ts_batch_column_info_t *batchColumnInfo(qdb::jni::env &env,
                                            jobjectArray tableColumns);

void batchColumnRelease(qdb_ts_batch_column_info_t *column_info,
                        qdb_size_t column_info_count);

void columnsToNative(qdb::jni::env &env,
                     jobjectArray columns,
                     qdb_ts_column_info_t *native_columns,
                     size_t column_count);
void releaseNative(qdb_ts_column_info_t *native_columns, size_t column_count);

qdb::jni::guard::local_ref<jobjectArray>
nativeToColumns(qdb::jni::env &env,
                qdb_ts_column_info_t *nativeColumns,
                size_t column_count);

void doublePointToNative(qdb::jni::env &env,
                         jobject input,
                         qdb_ts_double_point *native);
void doublePointsToNative(qdb::jni::env &env,
                          jobjectArray input,
                          size_t count,
                          qdb_ts_double_point *native);

qdb::jni::guard::local_ref<jobject>
nativeToDoublePoint(qdb::jni::env &env, qdb_ts_double_point native);

qdb::jni::guard::local_ref<jobjectArray> nativeToDoublePoints(
    qdb::jni::env &env, qdb_ts_double_point *native, size_t count);

void
blobPointToNative(qdb::jni::env &env, jobject input, qdb_ts_blob_point *native);
void blobPointsToNative(qdb::jni::env &env,
                        jobjectArray input,
                        size_t count,
                        qdb_ts_blob_point *native);

qdb::jni::guard::local_ref<jobject> nativeToBlobPoint(qdb::jni::env &env,
                                                      qdb_ts_blob_point native);

qdb::jni::guard::local_ref<jobjectArray>
nativeToBlobPoints(qdb::jni::env &env, qdb_ts_blob_point *native, size_t count);

qdb::jni::guard::local_ref<jobject>
nativeToStringPoint(qdb::jni::env &env, qdb_ts_string_point native);

qdb::jni::guard::local_ref<jobjectArray> nativeToStringPoints(
    qdb::jni::env &env, qdb_ts_string_point *native, size_t count);

void doubleAggregateToNative(qdb::jni::env &env,
                             jobject input,
                             qdb_ts_double_aggregation_t *native);
void doubleAggregatesToNative(qdb::jni::env &env,
                              jobjectArray input,
                              size_t count,
                              qdb_ts_double_aggregation_t *native);

qdb::jni::guard::local_ref<jobject>
nativeToDoubleAggregate(qdb::jni::env &env, qdb_ts_double_aggregation_t native);

qdb::jni::guard::local_ref<jobjectArray> nativeToDoubleAggregates(
    qdb::jni::env &env, qdb_ts_double_aggregation_t *native, size_t count);

void blobAggregateToNative(qdb::jni::env &env,
                           jobject input,
                           qdb_ts_blob_aggregation_t *native);
void blobAggregatesToNative(qdb::jni::env &env,
                            jobjectArray input,
                            size_t count,
                            qdb_ts_blob_aggregation_t *native);

qdb::jni::guard::local_ref<jobject>
nativeToBlobAggregate(qdb::jni::env &env, qdb_ts_blob_aggregation_t native);

qdb::jni::guard::local_ref<jobjectArray> nativeToBlobAggregates(
    qdb::jni::env &env, qdb_ts_blob_aggregation_t *native, size_t count);

qdb_error_t tableGetRanges(qdb::jni::env &env,
                           qdb_handle_t handle,
                           qdb_local_table_t localTable,
                           jobjectArray ranges);

qdb_error_t tableGetRow(qdb::jni::env &env,
                        qdb_handle_t handle,
                        qdb_local_table_t localTable,
                        qdb_ts_column_info_t *columns,
                        qdb_size_t columnCount,
                        jobject *output);
