#pragma once

#include "../guard/local_ref.h"
#include "helpers.h"
#include <qdb/ts.h>
#include <jni.h>

namespace qdb
{
namespace jni
{
class env;
class object_array;
}; // namespace jni
}; // namespace qdb

void doublePointToNative(qdb::jni::env & env, jobject input, qdb_ts_double_point * native);
void doublePointsToNative(
    qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_double_point * native);

qdb::jni::guard::local_ref<jobject> nativeToDoublePoint(
    qdb::jni::env & env, qdb_ts_double_point native);

qdb::jni::guard::local_ref<jobjectArray> nativeToDoublePoints(
    qdb::jni::env & env, qdb_ts_double_point * native, size_t count);

void blobPointToNative(qdb::jni::env & env, jobject input, qdb_ts_blob_point * native);
void blobPointsToNative(
    qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_blob_point * native);

qdb::jni::guard::local_ref<jobject> nativeToBlobPoint(
    qdb::jni::env & env, qdb_ts_blob_point native);

qdb::jni::guard::local_ref<jobjectArray> nativeToBlobPoints(
    qdb::jni::env & env, qdb_ts_blob_point * native, size_t count);

qdb::jni::guard::local_ref<jobject> nativeToStringPoint(
    qdb::jni::env & env, qdb_ts_string_point native);

qdb::jni::guard::local_ref<jobjectArray> nativeToStringPoints(
    qdb::jni::env & env, qdb_ts_string_point * native, size_t count);

void doubleAggregateToNative(
    qdb::jni::env & env, jobject input, qdb_ts_double_aggregation_t * native);
void doubleAggregatesToNative(
    qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_double_aggregation_t * native);

qdb::jni::guard::local_ref<jobject> nativeToDoubleAggregate(
    qdb::jni::env & env, qdb_ts_double_aggregation_t native);

qdb::jni::guard::local_ref<jobjectArray> nativeToDoubleAggregates(
    qdb::jni::env & env, qdb_ts_double_aggregation_t * native, size_t count);

void blobAggregateToNative(qdb::jni::env & env, jobject input, qdb_ts_blob_aggregation_t * native);
void blobAggregatesToNative(
    qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_blob_aggregation_t * native);

qdb::jni::guard::local_ref<jobject> nativeToBlobAggregate(
    qdb::jni::env & env, qdb_ts_blob_aggregation_t native);

qdb::jni::guard::local_ref<jobjectArray> nativeToBlobAggregates(
    qdb::jni::env & env, qdb_ts_blob_aggregation_t * native, size_t count);
