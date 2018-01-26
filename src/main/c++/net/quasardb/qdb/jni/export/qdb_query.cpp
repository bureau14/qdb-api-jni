#include <cassert>
#include <stdlib.h>
#include <qdb/query.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../guard/local.h"
#include "../ts/qdb_value.h"
#include "../env.h"
#include "../util/helpers.h"
#include "../util/qdb_jni.h"


jobjectArray
nativeToRow(qdb::jni::env & env, qdb_point_result_t const values[], qdb_size_t count) {

  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jobjectArray outputValues = env.instance().NewObjectArray(count, valueClass, NULL);

  printf("* NATIVE * iterating over %d columns!\n", count);
  fflush(stdout);

  for (qdb_size_t i = 0; i < count; ++i) {
    jobject value =
      qdb::value::from_native(env, values[i]);

    env.instance().SetObjectArrayElement(outputValues, i, value);
    env.instance().DeleteLocalRef(value);
  }

  return outputValues;
}

jobjectArray
nativeToColumns(qdb::jni::env & env, qdb_string_t const columns[], qdb_size_t count) {

  jclass stringClass = qdb::jni::lookup_class(env, "java/lang/String");
  jobjectArray outputColumns = env.instance().NewObjectArray(count, stringClass, NULL);
  assert(outputColumns != NULL);

  for (qdb_size_t i = 0; i < count; ++i) {
    jstring column = env.instance().NewStringUTF(columns[i].data);
    env.instance().SetObjectArrayElement(outputColumns, i, column);
    env.instance().DeleteLocalRef(column);
  }

  return outputColumns;
}

qdb_error_t
nativeToTable(qdb::jni::env & env, qdb_table_result_t const & input, jclass tableClass, jobject & table) {
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jclass valuesClass = qdb::jni::lookup_class(env, "[Lnet/quasardb/qdb/ts/Value;");
  jmethodID valueConstructor = qdb::jni::lookup_methodID(env, valueClass, "<init>", "()V");

  printf("* NATIVE * converting result to table: %p\n", table);
  printf("* NATIVE * converting result to table, table_name = %s\n", input.table_name);
  printf("* NATIVE * converting result to table, columns_count = %d\n", input.columns_count);
  printf("* NATIVE * converting result to table, rows_count = %d\n", input.rows_count);
  fflush(stdout);

  jfieldID nameFieldId = qdb::jni::lookup_fieldID(env, tableClass,
                                                  "name", "Ljava/lang/String;");
  jfieldID columnsFieldId = qdb::jni::lookup_fieldID(env, tableClass,
                                                     "columns", "[Ljava/lang/String;");

  jstring name = env.instance().NewStringUTF(input.table_name.data);
  env.instance().SetObjectField(table, nameFieldId, name);
  env.instance().DeleteLocalRef(name);

  jobjectArray output_columns = nativeToColumns(env, input.columns_names, input.columns_count);
  env.instance().SetObjectField(table, columnsFieldId, output_columns);
  env.instance().DeleteLocalRef(output_columns);

  jobjectArray output_rows = env.instance().NewObjectArray(input.rows_count, valuesClass, NULL);

  qdb_error_t err = qdb_e_ok;
  for (qdb_size_t i = 0; i < input.rows_count; ++i) {
    qdb_point_result_t const * input_row = input.rows[i];

    jobjectArray output_row = nativeToRow(env, input.rows[i], input.columns_count);
    env.instance().SetObjectArrayElement(output_rows, i, output_row);
    env.instance().DeleteLocalRef(output_row);
  }

  jfieldID rowsFieldId = qdb::jni::lookup_fieldID(env, tableClass,
                                                  "rows", "[[Lnet/quasardb/qdb/ts/Value;");
  env.instance().SetObjectField(table, rowsFieldId, output_rows);
  env.instance().DeleteLocalRef(output_rows);

  return err;
}

qdb_error_t
nativeToResult(qdb::jni::env & env, qdb_query_result_t const & input, jclass resultClass, jobject & result) {

  jclass tableClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Result$Table");
  jmethodID tableConstuctor = qdb::jni::lookup_methodID(env, tableClass, "<init>", "()V");
  jfieldID tablesFieldId = qdb::jni::lookup_fieldID(env,
                                                    resultClass,
                                                    "tables",
                                                    "[Lnet/quasardb/qdb/ts/Result$Table;");

  jobjectArray tables = env.instance().NewObjectArray(input.tables_count, tableClass, NULL);
  env.instance().SetObjectField(result, tablesFieldId, tables);

  qdb_error_t err = qdb_e_ok;
  for (qdb_size_t i = 0; i < input.tables_count; ++i) {
    jobject table = env.instance().NewObject(tableClass, tableConstuctor);
    err = nativeToTable(env, input.tables[i], tableClass, table);

    if(QDB_FAILURE(err)) {
      break;
    }

    env.instance().SetObjectArrayElement(tables, i, table);
    env.instance().DeleteLocalRef(table);
  }

  env.instance().DeleteLocalRef(tables);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_query_1execute(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                             jstring query, jobject outputReference) {
  qdb::jni::env env(jniEnv);

  // :TODO: cache!
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
    output = env.instance().NewObject(outputClass, constructor);
    assert(output != NULL);

    err = nativeToResult(env, *result, outputClass, output);
  }

  if (QDB_SUCCESS(err)) {
    assert(output != NULL);
    setReferenceValue(env, outputReference, output);
    env.instance().DeleteLocalRef(output);
  }

  qdb_release((qdb_handle_t)handle, result);
  return err;
}
