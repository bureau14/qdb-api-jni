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

  fflush(stdout);
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

  double_points_to_native(env, points, points_count, values);

  qdb_error_t err = qdb_ts_double_insert((qdb_handle_t)handle,
                                         StringUTFChars(env, alias),
                                         StringUTFChars(env, column),
                                         values,
                                         points_count);

  return err;
}
