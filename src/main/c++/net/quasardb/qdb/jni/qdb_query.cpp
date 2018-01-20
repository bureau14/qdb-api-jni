#include <cassert>
#include <stdlib.h>
#include <qdb/query.h>

#include "util/qdb_jni.h"
#include "ts/qdb_value.h"

#include "helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"

jobject
nativeToRow(JNIEnv * env, qdb_point_result_t const values[], qdb_size_t count) {

  jclass rowClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Row");
  jclass timespecClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Timespec");
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");

  jobjectArray outputValues = env->NewObjectArray(count, valueClass, NULL);

  printf("* NATIVE * iterating over %d columns!\n", count);
  fflush(stdout);

  jmethodID timespecConstructor = env->GetMethodID(timespecClass, "<init>", "()V");
  jobject timestamp = env->NewObject(timespecClass, timespecConstructor);;

  for (qdb_size_t i = 0; i < count; ++i) {
    jobject value =
      qdb::value::from_native(env, values[i]);

    env->SetObjectArrayElement(outputValues, i, value);
    env->DeleteLocalRef(value);
  }

  jmethodID rowConstructor = env->GetMethodID(rowClass, "<init>", "(Lnet/quasardb/qdb/ts/Timespec;[Lnet/quasardb/qdb/ts/Value;)V");
  jobject output = env->NewObject(rowClass, rowConstructor, timestamp, outputValues);

  env->DeleteLocalRef(timestamp);
  env->DeleteLocalRef(outputValues);
  return output;
}

qdb_error_t
nativeToTable(JNIEnv * env, qdb_table_result_t const & input, jclass tableClass, jobject & table) {

  // :TODO: cache!
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jclass rowClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Row");
  jmethodID valueConstructor = qdb::jni::lookup_methodID(env, valueClass, "<init>", "()V");

  printf("* NATIVE * converting result to table: %p\n", table);
  printf("* NATIVE * converting result to table, table_name = %s\n", input.table_name);
  printf("* NATIVE * converting result to table, columns_count = %d\n", input.columns_count);
  printf("* NATIVE * converting result to table, rows_count = %d\n", input.rows_count);
  fflush(stdout);

  jfieldID nameFieldId = qdb::jni::lookup_fieldID(env, tableClass,
                                                  "name", "Ljava/lang/String;");
  jstring name = env->NewStringUTF(input.table_name.data);
  env->SetObjectField(table, nameFieldId, name);
  env->DeleteLocalRef(name);

  jobjectArray output_rows = env->NewObjectArray(input.rows_count, rowClass, NULL);

  qdb_error_t err = qdb_e_ok;
  for (qdb_size_t i = 0; i < input.rows_count; ++i) {
    qdb_point_result_t const * input_row = input.rows[i];

    jobject output_row = nativeToRow(env, input.rows[i], input.columns_count);
    env->SetObjectArrayElement(output_rows, i, output_row);
    env->DeleteLocalRef(output_row);
  }

  jfieldID rowsFieldId = qdb::jni::lookup_fieldID(env, tableClass,
                                                  "rows", "[Lnet/quasardb/qdb/ts/Row;");
  env->SetObjectField(table, rowsFieldId, output_rows);

  env->DeleteLocalRef(output_rows);

  return err;
}

qdb_error_t
nativeToResult(JNIEnv * env, qdb_query_result_t const & input, jclass resultClass, jobject & result) {
  assert(result != NULL);

  // :TODO: cache!
  jclass tableClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Result$Table");
  jmethodID tableConstuctor = qdb::jni::lookup_methodID(env, tableClass, "<init>", "()V");
  jfieldID tablesFieldId = qdb::jni::lookup_fieldID(env,
                                                    resultClass,
                                                    "tables",
                                                    "[Lnet/quasardb/qdb/ts/Result$Table;");

  jobjectArray tables = env->NewObjectArray(input.tables_count, tableClass, NULL);
  env->SetObjectField(result, tablesFieldId, tables);

  qdb_error_t err = qdb_e_ok;
  for (qdb_size_t i = 0; i < input.tables_count; ++i) {
    jobject table = env->NewObject(tableClass, tableConstuctor);
    err = nativeToTable(env, input.tables[i], tableClass, table);

    if(QDB_FAILURE(err)) {
      break;
    }

    env->SetObjectArrayElement(tables, i, table);
    env->DeleteLocalRef(table);
  }

  env->DeleteLocalRef(tables);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_query_1execute(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                             jstring query, jobject outputReference) {

  jobject output = NULL;
  qdb_query_result_t * result;
  qdb_error_t err = qdb_exp_query((qdb_handle_t)(handle),
                                  StringUTFChars(env, query),
                                  &result);

  if (QDB_SUCCESS(err)) {
    assert(result != NULL);

    // :TODO: cache!
    jclass outputClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Result");
    jmethodID constructor = qdb::jni::lookup_methodID(env, outputClass, "<init>", "()V");
    output = env->NewObject(outputClass, constructor);
    assert(output != NULL);

    err = nativeToResult(env, *result, outputClass, output);
  }

  if (QDB_SUCCESS(err)) {
    assert(output != NULL);
    setReferenceValue(env, outputReference, output);
    env->DeleteLocalRef(output);
  }

  qdb_release((qdb_handle_t)handle, result);
  return err;
}
