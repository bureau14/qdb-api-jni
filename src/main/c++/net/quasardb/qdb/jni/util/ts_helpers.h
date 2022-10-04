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

void doubleAggregateToNative(
    qdb::jni::env & env, jobject input, qdb_ts_double_aggregation_t * native);
void doubleAggregatesToNative(
    qdb::jni::env & env, jobjectArray input, size_t count, qdb_ts_double_aggregation_t * native);

qdb::jni::guard::local_ref<jobject> nativeToDoubleAggregate(
    qdb::jni::env & env, qdb_ts_double_aggregation_t native);

qdb::jni::guard::local_ref<jobjectArray> nativeToDoubleAggregates(
    qdb::jni::env & env, qdb_ts_double_aggregation_t * native, size_t count);
