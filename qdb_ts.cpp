#include "net_quasardb_qdb_jni_qdb.h"


#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "helpers.h"
#include <qdb/ts.h>

void
column_info_to_native(JNIEnv * env, jobjectArray columns, qdb_ts_column_info * native_columns, size_t column_count) {
  jfieldID name_field, type_field;
  jclass object_class;
  for (size_t i = 0; i < column_count; ++i) {
    jobject object = (jobject) (env->GetObjectArrayElement(columns, i));

    object_class = env->GetObjectClass(object);
    name_field = env->GetFieldID(object_class, "name", "Ljava/lang/String;");
    type_field = env->GetFieldID(object_class, "type", "I");
    jstring name = (jstring)env->GetObjectField(object, name_field);

    native_columns[i].type = (qdb_ts_column_type)(env->GetIntField(object, type_field));
    native_columns[i].name = strdup(StringUTFChars(env, name));
  }

  fflush(stdout);
}

void
release_column_info(qdb_ts_column_info * native_columns, size_t column_count) {
  for (size_t i = 0; i < column_count; ++i) {
    free((void *)(native_columns[i].name));
  }
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1create(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                         jstring alias, jobjectArray columns) {
  size_t column_count = env->GetArrayLength(columns);
  qdb_ts_column_info native_columns[column_count];

  column_info_to_native(env, columns, native_columns, column_count);

  jint result = qdb_ts_create((qdb_handle_t)handle, StringUTFChars(env, alias), native_columns, column_count);
  release_column_info(native_columns, column_count);
  return result;
}
