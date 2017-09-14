#include "net_quasardb_qdb_jni_qdb.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "helpers.h"
#include <qdb/ts.h>

void
columnsToNative(JNIEnv * env, jobjectArray columns, qdb_ts_column_info * native_columns, size_t column_count) {
  jfieldID nameField, typeField;
  jclass objectClass;
  for (size_t i = 0; i < column_count; ++i) {
    jobject object = (jobject) (env->GetObjectArrayElement(columns, i));

    objectClass = env->GetObjectClass(object);
    nameField = env->GetFieldID(objectClass, "name", "Ljava/lang/String;");
    typeField = env->GetFieldID(objectClass, "type", "I");
    jstring name = (jstring)env->GetObjectField(object, nameField);

    native_columns[i].type = (qdb_ts_column_type)(env->GetIntField(object, typeField));

    // Is there a better way to do this ? Because we're using strdup here, we need a separate
    // release function which is fragile.
    native_columns[i].name = strdup(StringUTFChars(env, name));
  }
}

void
releaseNative(qdb_ts_column_info * native_columns, size_t column_count) {
  for (size_t i = 0; i < column_count; ++i) {
    free((void *)(native_columns[i].name));
  }
}

void
nativeToColumns(JNIEnv * env, qdb_ts_column_info * nativeColumns, size_t column_count, jobjectArray * columns) {
  jclass column_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_column_info");
  jmethodID constructor = env->GetMethodID(column_class, "<init>", "(Ljava/lang/String;I)V");

  *columns = env->NewObjectArray((jsize)column_count, column_class, NULL);

  for (size_t i = 0; i < column_count; i++) {
    env->SetObjectArrayElement(*columns, (jsize)i, env->NewObject(column_class,
                                                                  constructor,
                                                                  env->NewStringUTF(nativeColumns[i].name),
                                                                  nativeColumns[i].type));
  }
}

void
doublePointToNative(JNIEnv * env, jobject input, qdb_ts_double_point * native) {
  jfieldID timestampField, valueField;
  jclass objectClass;

  objectClass = env->GetObjectClass(input);

  timestampField = env->GetFieldID(objectClass, "timestamp", "Lnet/quasardb/qdb/jni/qdb_timespec;");
  valueField = env->GetFieldID(objectClass, "value", "D");

  timespecToNative(env, env->GetObjectField(input, timestampField), &(native->timestamp));
  native->value = env->GetDoubleField(input, valueField);
}

void
doublePointsToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_point * native) {
  qdb_ts_double_point * cur = native;
  for (size_t i = 0; i < count; ++i) {
    jobject point = (jobject)(env->GetObjectArrayElement(input, i));

    doublePointToNative(env, point, cur++);
  }
}

void
nativeToDoublePoint(JNIEnv * env, qdb_ts_double_point native, jobject * output) {
  jclass pointClass = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_point");
  jmethodID constructor = env->GetMethodID(pointClass, "<init>", "(Lnet/quasardb/qdb/jni/qdb_timespec;D)V");

  jobject timespec;
  nativeToTimespec(env, native.timestamp, &timespec);

  *output = env->NewObject(pointClass,
                           constructor,
                           timespec,
                           native.value);
}

void
nativeToDoublePoints(JNIEnv * env, qdb_ts_double_point * native, size_t count, jobjectArray * output) {
  jclass pointClass = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_point");

  *output = env->NewObjectArray((jsize)count, pointClass, NULL);

  for (size_t i = 0; i < count; i++) {
    jobject point;
    nativeToDoublePoint(env, native[i], &point);
    env->SetObjectArrayElement(*output, (jsize)i, point);
  }
}


void
rangeToNative(JNIEnv *env, jobject input, qdb_ts_range_t * native) {
  jfieldID beginField, endField;
  jclass objectClass;

  objectClass = env->GetObjectClass(input);

  beginField = env->GetFieldID(objectClass, "begin", "Lnet/quasardb/qdb/jni/qdb_timespec;");
  endField = env->GetFieldID(objectClass, "end", "Lnet/quasardb/qdb/jni/qdb_timespec;");

  timespecToNative(env, env->GetObjectField(input, beginField), &(native->begin));
  timespecToNative(env, env->GetObjectField(input, endField), &(native->end));
}

void
rangesToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_range_t * native) {
  qdb_ts_range_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
    jobject point = (jobject)(env->GetObjectArrayElement(input, i));

    rangeToNative(env, point, cur++);
  }
}

void
nativeToRange(JNIEnv * env, qdb_ts_range_t native, jobject * output) {
  jclass point_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_range");
  jmethodID constructor = env->GetMethodID(point_class, "<init>", "(Lnet/quasardb/qdb/jni/qdb_timespec;Lnet/quasardb/qdb/jni/qdb_timespec;)V");

  jobject begin;
  jobject end;

  nativeToTimespec(env, native.begin, &begin);
  nativeToTimespec(env, native.end, &end);

  *output = env->NewObject(point_class,
                           constructor,
                           begin,
                           end);
}

void
doubleAggregateToNative(JNIEnv *env, jobject input, qdb_ts_double_aggregation_t * native) {
  assert(input != NULL);

  jfieldID typeField, rangeField, countField, resultField;
  jclass objectClass;

  objectClass = env->GetObjectClass(input);
  typeField = env->GetFieldID(objectClass, "aggregation_type", "J");
  rangeField = env->GetFieldID(objectClass, "range", "Lnet/quasardb/qdb/jni/qdb_ts_range;");
  countField = env->GetFieldID(objectClass, "count", "J");
  resultField = env->GetFieldID(objectClass, "result", "Lnet/quasardb/qdb/jni/qdb_ts_double_point;");

  rangeToNative(env, env->GetObjectField(input, rangeField), &(native->range));
  doublePointToNative(env, env->GetObjectField(input, resultField), &(native->result));

  native->type = (qdb_ts_aggregation_type_t)(env->GetLongField(input, typeField));
  native->count = env->GetLongField(input, countField);

  fflush(stdout);
}

void
doubleAggregatesToNative(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_aggregation_t * native) {
  assert(input != NULL);

  qdb_ts_double_aggregation_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
    jobject aggregate = (jobject)(env->GetObjectArrayElement(input, i));

    doubleAggregateToNative(env, aggregate, cur++);
  }
}

void
nativeToDoubleAggregate(JNIEnv * env, qdb_ts_double_aggregation native, jobject * output) {
  jclass point_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_aggregation");
  jmethodID constructor = env->GetMethodID(point_class, "<init>", "(Lnet/quasardb/qdb/jni/qdb_ts_range;JJLnet/quasardb/qdb/jni/qdb_ts_double_point;)V");

  jobject range, result;

  nativeToRange(env, native.range, &range);
  nativeToDoublePoint(env, native.result, &result);

  jobject aggregate = env->NewObject(point_class,
                                     constructor,
                                     range,
                                     (jlong)native.type,
                                     (jlong)native.count,
                                     result);

  *output = aggregate;
}

void
nativeToDoubleAggregates(JNIEnv * env, qdb_ts_double_aggregation * native, size_t count, jobjectArray * output) {
  jclass aggregate_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_aggregation");
  assert (aggregate_class != NULL);

  *output = env->NewObjectArray((jsize)count, aggregate_class, NULL);

  for (size_t i = 0; i < count; i++) {
    jobject aggregate;

    nativeToDoubleAggregate(env, native[i], &aggregate);
    env->SetObjectArrayElement(*output, (jsize)i, aggregate);
  }
}

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
