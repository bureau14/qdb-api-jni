#include "net_quasardb_qdb_jni_qdb.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "helpers.h"
#include <qdb/ts.h>

void
columns_to_native(JNIEnv * env, jobjectArray columns, qdb_ts_column_info * native_columns, size_t column_count) {
  jfieldID name_field, type_field;
  jclass object_class;
  for (size_t i = 0; i < column_count; ++i) {
    jobject object = (jobject) (env->GetObjectArrayElement(columns, i));

    object_class = env->GetObjectClass(object);
    name_field = env->GetFieldID(object_class, "name", "Ljava/lang/String;");
    type_field = env->GetFieldID(object_class, "type", "I");
    jstring name = (jstring)env->GetObjectField(object, name_field);

    native_columns[i].type = (qdb_ts_column_type)(env->GetIntField(object, type_field));

    // Is there a better way to do this ? Because we're using strdup here, we need a separate
    // release function which is fragile.
    native_columns[i].name = strdup(StringUTFChars(env, name));
  }
}

void
release_native(qdb_ts_column_info * native_columns, size_t column_count) {
  for (size_t i = 0; i < column_count; ++i) {
    free((void *)(native_columns[i].name));
  }
}

void
native_to_columns(JNIEnv * env, qdb_ts_column_info * nativeColumns, size_t column_count, jobjectArray * columns) {
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
double_point_to_native(JNIEnv * env, jobject input, qdb_ts_double_point * native) {
  jfieldID timestamp_field, value_field;
  jclass object_class;
  jobject timespec;

  object_class = env->GetObjectClass(input);

  timestamp_field = env->GetFieldID(object_class, "timestamp", "Lnet/quasardb/qdb/jni/qdb_timespec;");
  value_field = env->GetFieldID(object_class, "value", "D");

  timespecToNative(env, env->GetObjectField(input, timestamp_field), &(native->timestamp));
  native->value = env->GetDoubleField(input, value_field);
}

void
double_points_to_native(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_point * native) {
  qdb_ts_double_point * cur = native;
  for (size_t i = 0; i < count; ++i) {
    jobject point = (jobject)(env->GetObjectArrayElement(input, i));

    double_point_to_native(env, point, cur++);
  }
}

void
native_to_double_point(JNIEnv * env, qdb_ts_double_point native, jobject * output) {
  jclass point_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_point");
  jmethodID constructor = env->GetMethodID(point_class, "<init>", "(Lnet/quasardb/qdb/jni/qdb_timespec;D)V");

  jobject timespec;
  nativeToTimespec(env, native.timestamp, &timespec);

  *output = env->NewObject(point_class,
                           constructor,
                           timespec,
                           native.value);
}

void
native_to_double_points(JNIEnv * env, qdb_ts_double_point * native, size_t count, jobjectArray * output) {
  jclass point_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_point");

  *output = env->NewObjectArray((jsize)count, point_class, NULL);

  for (size_t i = 0; i < count; i++) {
    jobject point;
    native_to_double_point(env, native[i], &point);
    env->SetObjectArrayElement(*output, (jsize)i, point);
  }
}


void
range_to_native(JNIEnv *env, jobject input, qdb_ts_range_t * native) {
  jfieldID begin_field, end_field;
  jclass object_class;

  object_class = env->GetObjectClass(input);

  begin_field = env->GetFieldID(object_class, "begin", "Lnet/quasardb/qdb/jni/qdb_timespec;");
  end_field = env->GetFieldID(object_class, "end", "Lnet/quasardb/qdb/jni/qdb_timespec;");
  timespecToNative(env, env->GetObjectField(input, begin_field), &(native->begin));
  timespecToNative(env, env->GetObjectField(input, end_field), &(native->end));
}

void
ranges_to_native(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_range_t * native) {
  qdb_ts_range_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
    jobject point = (jobject)(env->GetObjectArrayElement(input, i));

    range_to_native(env, point, cur++);
  }
}

void
native_to_range(JNIEnv * env, qdb_ts_range_t native, jobject * output) {
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
double_aggregate_to_native(JNIEnv *env, jobject input, qdb_ts_double_aggregation_t * native) {
  assert(input != NULL);

  jfieldID type_field, range_field, count_field, result_field;
  jclass object_class;

  object_class = env->GetObjectClass(input);
  type_field = env->GetFieldID(object_class, "aggregation_type", "J");
  range_field = env->GetFieldID(object_class, "range", "Lnet/quasardb/qdb/jni/qdb_ts_range;");
  count_field = env->GetFieldID(object_class, "count", "J");
  result_field = env->GetFieldID(object_class, "result", "Lnet/quasardb/qdb/jni/qdb_ts_double_point;");

  range_to_native(env, env->GetObjectField(input, range_field), &(native->range));
  double_point_to_native(env, env->GetObjectField(input, result_field), &(native->result));
  native->type = (qdb_ts_aggregation_type_t)(env->GetLongField(input, type_field));
  native->count = env->GetLongField(input, count_field);
}

void
double_aggregates_to_native(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_aggregation_t * native) {
  assert(input != NULL);

  qdb_ts_double_aggregation_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
    jobject aggregate = (jobject)(env->GetObjectArrayElement(input, i));

    double_aggregate_to_native(env, aggregate, cur++);
  }
}

void
native_to_double_aggregate(JNIEnv * env, qdb_ts_double_aggregation native, jobject * output) {
  jclass point_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_aggregation");
  jmethodID constructor = env->GetMethodID(point_class, "<init>", "(Lnet/quasardb/qdb/jni/qdb_ts_range;JJLnet/quasardb/qdb/jni/qdb_ts_double_point;)V");

  jobject range, result;

  native_to_range(env, native.range, &range);
  native_to_double_point(env, native.result, &result);

  *output = env->NewObject(point_class,
                           constructor,
                           range,
                           native.type,
                           native.count,
                           result);
}

void
native_to_double_aggregates(JNIEnv * env, qdb_ts_double_aggregation * native, size_t count, jobjectArray * output) {
  jclass aggregate_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_aggregation");
  assert (aggregate_class != NULL);

  *output = env->NewObjectArray((jsize)count, aggregate_class, NULL);

  for (size_t i = 0; i < count; i++) {
    jobject aggregate;
    native_to_double_aggregate(env, native[i], &aggregate);

    env->SetObjectArrayElement(*output, (jsize)i, aggregate);
  }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1create(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                         jstring alias, jobjectArray columns) {
  size_t column_count = env->GetArrayLength(columns);
  qdb_ts_column_info * native_columns = new qdb_ts_column_info[column_count];

  columns_to_native(env, columns, native_columns, column_count);

  jint result = qdb_ts_create((qdb_handle_t)handle, StringUTFChars(env, alias), native_columns, column_count);
  release_native(native_columns, column_count);

  delete[] native_columns;
  return result;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1insert_1columns(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                  jstring alias, jobjectArray columns) {
  size_t column_count = env->GetArrayLength(columns);
  qdb_ts_column_info * native_columns = new qdb_ts_column_info[column_count];

  columns_to_native(env, columns, native_columns, column_count);

  jint result = qdb_ts_insert_columns((qdb_handle_t)handle, StringUTFChars(env, alias), native_columns, column_count);
  release_native(native_columns, column_count);

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
    native_to_columns(env, native_columns, column_count, &array);
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

  double_points_to_native(env, points, points_count, values);

  qdb_error_t err = qdb_ts_double_insert((qdb_handle_t)handle,
                                         StringUTFChars(env, alias),
                                         StringUTFChars(env, column),
                                         values,
                                         points_count);

  delete[] values;
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1get_1ranges(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                      jstring alias, jstring column, jobjectArray ranges, jobject points) {
  qdb_size_t range_count = env->GetArrayLength(ranges);
  qdb_ts_range_t * native_ranges = new qdb_ts_range_t[range_count];
  ranges_to_native(env, ranges, range_count, native_ranges);

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
    native_to_double_points(env, native_points, point_count, &array);
    setReferenceValue(env, points, array);
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
  double_aggregates_to_native(env, input, count, aggregates);

  qdb_error_t err = qdb_ts_double_aggregate((qdb_handle_t)handle,
                                            StringUTFChars(env, alias),
                                            StringUTFChars(env, column),
                                            aggregates,
                                            count);

  if (QDB_SUCCESS(err)) {
    jobjectArray array;
    native_to_double_aggregates(env, aggregates, count, &array);
    setReferenceValue(env, output, array);
  }

  delete[] aggregates;
  return err;
}
