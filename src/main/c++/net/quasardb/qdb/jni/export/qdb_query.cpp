#include <cassert>
#include <stdlib.h>
#include <qdb/query.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../guard/local_ref.h"
#include "../string.h"
#include "../object.h"
#include "../env.h"
#include "../debug.h"
#include "../introspect.h"
#include "../util/helpers.h"
#include "../ts/qdb_value.h"

namespace jni = qdb::jni;

jobjectArray
nativeToRow(qdb::jni::env & env, qdb_point_result_t const values[], qdb_size_t count) {

  jclass valueClass = qdb::jni::introspect::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jobjectArray outputValues = env.instance().NewObjectArray(count, valueClass, NULL);

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

    jobjectArray outputColumns = env.instance().NewObjectArray(count,
                                                               jni::string::lookup_class(env),
                                                               NULL);
    assert(outputColumns != NULL);

    for (qdb_size_t i = 0; i < count; ++i) {
        env.instance().SetObjectArrayElement(outputColumns, i,
                                             jni::string::create(env, columns[i].data));
    }

    return outputColumns;
}

qdb_error_t
nativeToTable(qdb::jni::env & env, qdb_table_result_t const & input, jclass tableClass, jobject table) {
  jclass valueClass = qdb::jni::introspect::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jclass valuesClass = qdb::jni::introspect::lookup_class(env, "[Lnet/quasardb/qdb/ts/Value;");
  jmethodID valueConstructor = qdb::jni::introspect::lookup_method(env, valueClass, "<init>", "()V");

  jfieldID columnsFieldId = qdb::jni::introspect::lookup_field(env, tableClass,
                                                               "columns", "[Ljava/lang/String;");

  env.instance().SetObjectField(table,
                                qdb::jni::string::lookup_field(env, tableClass, "name"),
                                qdb::jni::string::create(env, input.table_name.data));


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

  jfieldID rowsFieldId = qdb::jni::introspect::lookup_field(env, tableClass,
                                                            "rows", "[[Lnet/quasardb/qdb/ts/Value;");
  env.instance().SetObjectField(table, rowsFieldId, output_rows);
  env.instance().DeleteLocalRef(output_rows);

  return err;
}

qdb_error_t
nativeToResult(qdb::jni::env & env, qdb_query_result_t const & input, jclass resultClass, jobject result) {

  jclass tableClass = qdb::jni::introspect::lookup_class(env, "net/quasardb/qdb/ts/Result$Table");
  jmethodID tableConstuctor = qdb::jni::introspect::lookup_method(env, tableClass, "<init>", "()V");
  jfieldID tablesFieldId = qdb::jni::introspect::lookup_field(env,
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

  jni::guard::local_ref<jobject> output(env);
  qdb_query_result_t * result;

  qdb_error_t err = qdb_exp_query((qdb_handle_t)(handle),
                                  qdb::jni::string::get_chars(env, query),
                                  &result);

  if (QDB_SUCCESS(err)) {
    assert(result != NULL);

    jclass outputClass = jni::introspect::lookup_class(env, "net/quasardb/qdb/ts/Result");
    output = jni::object::create(env, outputClass, "()V");
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
