#include <cassert>
#include <stdlib.h>
#include <qdb/query.h>

#include "net_quasardb_qdb_jni_qdb.h"

#include "../local_frame.h"
#include "../guard/local_ref.h"
#include "../string.h"
#include "../object.h"
#include "../env.h"
#include "../debug.h"
#include "../introspect.h"
#include "../util/helpers.h"
#include "../ts/qdb_value.h"

namespace jni = qdb::jni;

jni::guard::local_ref<jobjectArray>
nativeToRow(qdb::jni::env & env, qdb_point_result_t const values[], qdb_size_t count) {
    jni::local_frame lf = jni::local_frame::push(env, count);

    jni::guard::local_ref<jobjectArray> output(
        jni::object::create_array(env,
                                  count,
                                  "net/quasardb/qdb/ts/Value"));

    for (qdb_size_t i = 0; i < count; ++i) {
        env.instance().SetObjectArrayElement(output,
                                             i,
                                             qdb::jni::ts::value::from_native(env, values[i]).release());
    }


    return std::move(lf.pop(output.release()));
}

jni::guard::local_ref<jobjectArray>
nativeToColumns(qdb::jni::env & env, qdb_string_t const columns[], qdb_size_t count) {

    jni::guard::local_ref<jobjectArray> output = jni::string::create_array(env, count);

    for (qdb_size_t i = 0; i < count; ++i) {
        env.instance().SetObjectArrayElement(output, i,
                                             jni::string::create_utf8(env, columns[i].data));
    }

    return std::move(output);
}

jni::guard::local_ref<jobject>
nativeToTable(qdb::jni::env & env, qdb_table_result_t const & input, jclass tableClass) {

    jni::guard::local_ref<jobject> table(
        jni::object::create(env,
                            tableClass,
                            "()V"));

    env.instance().SetObjectField(table,
                                  qdb::jni::string::lookup_field(env, tableClass, "name"),
                                  qdb::jni::string::create_utf8(env, input.table_name.data));
    env.instance().SetObjectField(table,
                                  qdb::jni::introspect::lookup_field(env, tableClass,
                                                                     "columns", "[Ljava/lang/String;"),
                                  nativeToColumns(env, input.columns_names, input.columns_count).release());

    jni::guard::local_ref<jobjectArray> rows (
        jni::object::create_array(env,
                                  input.rows_count,
                                  "[Lnet/quasardb/qdb/ts/Value;"));

    for (qdb_size_t i = 0; i < input.rows_count; ++i) {
        qdb_point_result_t const * input_row = input.rows[i];

        env.instance().SetObjectArrayElement(rows,
                                             i,
                                             nativeToRow(env, input.rows[i], input.columns_count).release());
    }

    env.instance().SetObjectField(table,
                                  qdb::jni::introspect::lookup_field(env,
                                                                     tableClass,
                                                                     "rows",
                                                                     "[[Lnet/quasardb/qdb/ts/Value;"),
                                  rows);

    return std::move(table);
}

jni::guard::local_ref<jobject>
nativeToResult(qdb::jni::env & env, qdb_query_result_t const & input, jclass resultClass) {

    jclass tableClass = qdb::jni::introspect::lookup_class(env, "net/quasardb/qdb/ts/Result$Table");

    jni::guard::local_ref<jobjectArray> tables (
        jni::object::create_array(env,
                                  input.tables_count,
                                  tableClass));

    for (qdb_size_t i = 0; i < input.tables_count; ++i) {
        env.instance().SetObjectArrayElement(tables,
                                             i,
                                             nativeToTable(env, input.tables[i], tableClass).release());
    }

    return std::move(
        jni::object::create(env,
                            resultClass,
                            "([Lnet/quasardb/qdb/ts/Result$Table;)V",
                            tables.release()));
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_query_1execute(JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle,
                                             jstring query, jobject outputReference) {
    qdb::jni::env env(jniEnv);
    qdb_query_result_t * result = NULL;
    qdb_error_t err = qdb_exp_query((qdb_handle_t)(handle),
                                    qdb::jni::string::get_chars_utf8(env, query),
                                    &result);

    if (QDB_SUCCESS(err)) {
        assert(result != NULL);

        setReferenceValue(env,
                          outputReference,
                          nativeToResult(env,
                                         *result,
                                         jni::introspect::lookup_class(env,
                                                                       "net/quasardb/qdb/ts/Result")).release());
    }

    qdb_release((qdb_handle_t)handle, result);
    return err;
}
