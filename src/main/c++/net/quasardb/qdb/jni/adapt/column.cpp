#include "column.h"

template <>
/* static */ qdb::jni::guard::local_ref<jobject> qdb::jni::adapt::column::to_java(
    jni::env & env, qdb_ts_column_info_ex_t const & x)
{
    jstring name_     = env.instance().NewStringUTF(x.name);
    jstring symtable_ = env.instance().NewStringUTF(x.symtable);

    if (x.type == qdb_ts_column_symbol)
    {
        return jni::object::create(env, "net/quasardb/qdb/ts/Column",
            "(Ljava/lang/String;ILjava/lang/String;)V", name_, x.type, symtable_);
    }
    else
    {
        return jni::object::create(
            env, "net/quasardb/qdb/ts/Column", "(Ljava/lang/String;I)V", name_, x.type);
    }
}

template <>
/* static */ qdb::jni::guard::local_ref<jobject> qdb::jni::adapt::column::to_java(
    jni::env & env, qdb_ts_column_info_t const & x)
{
    jstring name_ = env.instance().NewStringUTF(x.name);
    return jni::object::create(
        env, "net/quasardb/qdb/ts/Column", "(Ljava/lang/String;I)V", name_, x.type);
}

template <>
/* static */ qdb_ts_column_info_ex_t qdb::jni::adapt::column::to_qdb(
    qdb::jni::env & env, qdb_handle_t handle, jobject input)
{
    jclass object_class = jni::object::get_class(env, input);
    jfieldID name_field = jni::string::lookup_field(env, object_class, "name");
    jfieldID type_field = jni::introspect::lookup_field(
        env, object_class, "type", "Lnet/quasardb/qdb/ts/Column$Type;");
    jfieldID symbol_table_field = jni::string::lookup_field(env, object_class, "symbolTable");

    jni::guard::string_utf8 name = jni::string::from_field(env, handle, input, name_field);
    qdb_ts_column_type_t type =
        _column_type_from_type_enum(env, jni::object::from_field<jobject>(env, input, type_field));

    return qdb_ts_column_info_ex_t{name.copy(handle), type,
        (type == qdb_ts_column_symbol
                ? jni::string::from_field(env, handle, input, symbol_table_field).copy(handle)
                : nullptr)};
}

template <>
/* static */ qdb_ts_batch_column_info_t qdb::jni::adapt::column::to_qdb(
    qdb::jni::env & env, qdb_handle_t handle, jobject input)
{
    jclass object_class   = jni::object::get_class(env, input);
    jfieldID table_field  = jni::string::lookup_field(env, object_class, "table");
    jfieldID column_field = jni::string::lookup_field(env, object_class, "column");

    jni::guard::string_utf8 table  = jni::string::from_field(env, handle, input, table_field);
    jni::guard::string_utf8 column = jni::string::from_field(env, handle, input, column_field);

    return qdb_ts_batch_column_info_t{table.copy(handle), column.copy(handle),

        // elements_count_hint
        1U};
}

template <>
/* static */ qdb_ts_column_info_t qdb::jni::adapt::column::to_qdb(
    qdb::jni::env & env, qdb_handle_t handle, jobject input)
{
    qdb_ts_column_info_ex_t x = to_qdb<qdb_ts_column_info_ex_t>(env, handle, input);
    return qdb_ts_column_info_t{x.name, x.type};
}
