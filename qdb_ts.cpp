#include "net_quasardb_qdb_jni_qdb.h"


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

  printf("native, storing double with value %f and range time: %d.%d\n", native->value, native->timestamp.tv_sec, native->timestamp.tv_nsec);
}

void
double_points_to_native(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_point * native) {
  qdb_ts_double_point * cur = native;
  for (size_t i = 0; i < count; ++i) {
    jobject point = (jobject)(env->GetObjectArrayElement(input, i));

    printf("native: double_points_to_native, i: %d, count: %d\n", i, count);

    double_point_to_native(env, point, cur++);
  }
}

void
native_to_double_point(JNIEnv * env, qdb_ts_double_point native, jobject * output) {
  jclass point_class = env->FindClass("net/quasardb/qdb/jni/qdb_ts_double_point");
  jmethodID constructor = env->GetMethodID(point_class, "<init>", "(Lnet/quasardb/qdb/jni/qdb_timespec;D)V");

  printf("native: to double point, contructor: %p\n", constructor);
  fflush(stdout);

  jobject timespec;
  nativeToTimespec(env, native.timestamp, &timespec);

  printf("native, has timespec: %p\n", timespec);
  fflush(stdout);

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
    printf("native: iterating over result point %d\n", i);
    fflush(stdout);

    jobject point;
    native_to_double_point(env, native[i], &point);
    env->SetObjectArrayElement(*output, (jsize)i, point);
  }
}


void
range_to_native(JNIEnv *env, jobject input, qdb_ts_range_t * native) {
  // qdb_timespec -> tv_sec, tv_nsec
  jfieldID begin_field, end_field;
  jclass object_class;

  object_class = env->GetObjectClass(input);

  begin_field = env->GetFieldID(object_class, "begin", "Lnet/quasardb/qdb/jni/qdb_timespec;");
  end_field = env->GetFieldID(object_class, "end", "Lnet/quasardb/qdb/jni/qdb_timespec;");
  timespecToNative(env, env->GetObjectField(input, begin_field), &(native->begin));
  timespecToNative(env, env->GetObjectField(input, end_field), &(native->end));

  printf("native, range begin: %d.%d, range end: %d.%d\n", native->begin.tv_sec, native->begin.tv_nsec, native->end.tv_sec, native->end.tv_nsec);
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
aggregates_to_native(JNIEnv * env, jobjectArray input, size_t count, qdb_ts_double_aggregation_t * native) {
  printf("aggregates to native, count = %d\n", count);
  fflush(stdout);

  qdb_ts_double_aggregation_t * cur = native;
  for (size_t i = 0; i < count; ++i) {
    jobject point; // = (jobject)(env->GetObjectArrayElement(input, i));

    //range_to_native(env, point, cur++);
  }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1create(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                         jstring alias, jobjectArray columns) {
  size_t column_count = env->GetArrayLength(columns);
  qdb_ts_column_info native_columns[column_count];

  columns_to_native(env, columns, native_columns, column_count);

  jint result = qdb_ts_create((qdb_handle_t)handle, StringUTFChars(env, alias), native_columns, column_count);
  release_native(native_columns, column_count);

  return result;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1insert_1columns(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                  jstring alias, jobjectArray columns) {
  size_t column_count = env->GetArrayLength(columns);
  qdb_ts_column_info native_columns[column_count];

  columns_to_native(env, columns, native_columns, column_count);

  jint result = qdb_ts_insert_columns((qdb_handle_t)handle, StringUTFChars(env, alias), native_columns, column_count);
  release_native(native_columns, column_count);

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
  qdb_ts_double_point values[points_count];

  printf("native: ts_double_insert, points_count: %d\n", points_count);

  double_points_to_native(env, points, points_count, values);

  qdb_error_t err = qdb_ts_double_insert((qdb_handle_t)handle,
                                         StringUTFChars(env, alias),
                                         StringUTFChars(env, column),
                                         values,
                                         points_count);

  fflush(stdout);

  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1get_1ranges(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                      jstring alias, jstring column, jobjectArray ranges, jobject points) {
  qdb_size_t range_count = env->GetArrayLength(ranges);
  qdb_ts_range_t native_ranges[range_count];
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
    fflush(stdout);
  }

  qdb_release((qdb_handle_t)handle, native_points);

  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1double_1aggregate(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                                    jstring alias, jstring column, jobjectArray input, jobject output) {
  qdb_size_t count = env->GetArrayLength(input);
  qdb_ts_double_aggregation_t aggregates[count];
  aggregates_to_native(env, input, count, aggregates);

  qdb_error_t err = qdb_ts_double_aggregate((qdb_handle_t)handle,
                                            StringUTFChars(env, alias),
                                            StringUTFChars(env, column),
                                            aggregates,
                                            count);

  printf("native: retrieved aggregates for %u ranges\n", count);
  fflush(stdout);

  if (QDB_SUCCESS(err)) {
    jobjectArray array;
    //native_to_double_points(env, native_points, point_count, &array);
    setReferenceValue(env, output, array);
    printf("native: done!\n");
    fflush(stdout);
  }

  return err;
}
