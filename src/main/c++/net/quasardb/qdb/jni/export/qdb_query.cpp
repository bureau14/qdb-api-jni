#include "../adapt/value.h"
#include "../debug.h"
#include "../env.h"
#include "../exception.h"
#include "../guard/local_ref.h"
#include "../introspect.h"
#include "../local_frame.h"
#include "../object.h"
#include "../string.h"
#include "../util/helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/query.h>
#include <cassert>
#include <stdlib.h>

namespace jni = qdb::jni;

jni::guard::local_ref<jobjectArray> nativeToValues(
    qdb::jni::env & env, qdb_point_result_t const values[], qdb_size_t count)
{
    // Takes native result row, and reutrns an array of Value objects.

    jni::local_frame lf = jni::local_frame::push(env, count);

    jni::guard::local_ref<jobjectArray> output(
        jni::object::create_array(env, count, "net/quasardb/qdb/ts/Value"));

    for (qdb_size_t i = 0; i < count; ++i)
    {
        env.instance().SetObjectArrayElement(
            output, i, qdb::jni::adapt::value::from_native(env, values[i]).release());
    }

    return lf.pop(output.release());
}

jni::guard::local_ref<jobjectArray> nativeToColumnNames(
    qdb::jni::env & env, qdb_string_t const columns[], qdb_size_t count)
{
    jni::guard::local_ref<jobjectArray> output = jni::string::create_array(env, count);

    for (qdb_size_t i = 0; i < count; ++i)
    {
        env.instance().SetObjectArrayElement(
            output, i, jni::string::create_utf8(env, columns[i].data));
    }

    return output;
}

jni::guard::local_ref<jobject> nativeToResult(
    qdb::jni::env & env, qdb_query_result_t const & input, jclass resultClass)
{
    jclass rowClass = qdb::jni::introspect::lookup_class(env, "net/quasardb/qdb/ts/Row");

    jni::guard::local_ref<jobjectArray> rows(
        jni::object::create_array(env, input.row_count, rowClass));

    for (qdb_size_t i = 0; i < input.row_count; ++i)
    {
        jni::guard::local_ref<jobject> row =
            jni::object::create(env, rowClass, "([Lnet/quasardb/qdb/ts/Value;)V",
                nativeToValues(env, input.rows[i], input.column_count).release());

        env.instance().SetObjectArrayElement(rows, i, row.release());
    }

    return jni::object::create(env, resultClass, "([Ljava/lang/String;[Lnet/quasardb/qdb/ts/Row;)V",
        nativeToColumnNames(env, input.column_names, input.column_count).release(), rows.release());
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_query_1execute(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring query, jobject outputReference)
{
    qdb::jni::env env(jniEnv);
    qdb_query_result_t * result = NULL;

    try
    {
        qdb_error_t err = qdb::jni::exception::throw_if_error(
            (qdb_handle_t)(handle), qdb_query((qdb_handle_t)(handle),
                                        qdb::jni::string::get_chars_utf8(env, query), &result));

        assert(result != NULL);

        setReferenceValue(env, outputReference,
            nativeToResult(
                env, *result, jni::introspect::lookup_class(env, "net/quasardb/qdb/ts/Result"))
                .release());

        qdb_release((qdb_handle_t)handle, result);

        return err;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}
