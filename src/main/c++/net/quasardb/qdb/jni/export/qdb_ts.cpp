#include "qdb_ts.h"
#include "../adapt/column.h"
#include "../adapt/local_table.h"
#include "../adapt/timerange.h"
#include "../adapt/timespec.h"
#include "../adapt/value_traits.h"
#include "../allocate.h"
#include "../byte_array.h"
#include "../byte_buffer.h"
#include "../debug.h"
#include "../detail/callback.h"
#include "../detail/native_ptr.h"
#include "../env.h"
#include "../exception.h"
#include "../guard/qdb_resource.h"
#include "../log.h"
#include "../object.h"
#include "../object_array.h"
#include "../primitive_array.h"
#include "../string.h"
#include "../util/helpers.h"
#include "../util/ts_helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"
#include <qdb/ts.h>
#include <cassert>
#include <iostream>
#include <stdlib.h>

namespace jni = qdb::jni;

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1create(JNIEnv * jniEnv,
    jclass /*thisClass*/,
    jlong handle,
    jstring alias,
    jlong shard_size,
    jobjectArray columns)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

        jni::object_array columns_{env, columns};
        std::vector<qdb_ts_column_info_ex_t> xs =
            jni::adapt::columns::to_qdb<qdb_ts_column_info_ex_t>(env, handle_, columns_);

        qdb::jni::exception::throw_if_error(handle_,
            qdb_ts_create_ex(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias),
                (qdb_uint_t)shard_size, xs.data(), xs.size()));

        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1remove(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        return qdb::jni::exception::throw_if_error(
            handle_, qdb_remove(handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias)));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jlong JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1shard_1size(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_uint_t shard_size{0};
        qdb::jni::exception::throw_if_error(
            handle_, qdb_ts_shard_size(handle_,
                         qdb::jni::string::get_chars_utf8(env, handle_, alias), &shard_size));

        assert(shard_size > 0);
        return (jlong)shard_size;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

// JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1insert_1columns(
//     JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jobjectArray columns)
// {
//     // std::cout << "55" << std::endl;
//     qdb::jni::env env(jniEnv);

//     try
//     {
//         // std::vector<qdb_ts_column_info_ex_t> columns_ =
//         //   jni::adapt::column::to_natives<qdb_ts_column_info_ex_t>(jni::object_array{env,
//         //                                                                          columns});

//         //   jni::exception::throw_if_error(
//         //       (qdb_handle_t)(handle),
//         //       qdb_ts_insert_columns_ex((qdb_handle_t)handle,
//         //                                qdb::jni::string::get_chars_utf8(env,
//         //                                alias), columns_.data(),
//         //                                columns_.size()));

//         return qdb_e_ok;
//     }
//     catch (jni::exception const & e)
//     {
//         e.throw_new(env);
//         return e.error();
//     }
// }

JNIEXPORT jobjectArray JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1list_1columns(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        jni::guard::qdb_resource<qdb_ts_column_info_ex_t *> xs{handle_};
        qdb_size_t n;

        jni::exception::throw_if_error(
            handle_, qdb_ts_list_columns_ex(
                         handle_, qdb::jni::string::get_chars_utf8(env, handle_, alias), &xs, &n));

        return jni::adapt::columns::to_java(env, xs.get(), n).release();
    }
    catch (jni::exception const & e)
    {
        //! :XXX: memory leak for native_columns? use unique_ptr instead?
        e.throw_new(env);
        return NULL;
    }
}

JNIEXPORT jlong JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1table_1init(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jobjectArray columns)
{
    qdb::jni::env env(jniEnv);

    qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
    jni::guard::qdb_resource<qdb_batch_table_t> ret{handle_};

    static_assert(sizeof(jlong) >= sizeof(qdb_batch_table_t));

    try
    {
        jni::object_array columns_{env, columns};
        std::vector<qdb_ts_batch_column_info_t> xs =
            jni::adapt::columns::to_qdb<qdb_ts_batch_column_info_t>(env, handle_, columns_);

        qdb::jni::exception::throw_if_error(
            handle_, qdb_ts_batch_table_init(handle_, xs.data(), xs.size(), &ret));

        return static_cast<jlong>(ret.release());
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return -1;
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1release_1columns_1memory(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong table)
{
    qdb::jni::env env(jniEnv);

    qdb_handle_t handle_     = reinterpret_cast<qdb_handle_t>(handle);
    qdb_batch_table_t table_ = reinterpret_cast<qdb_batch_table_t>(table);

    try
    {
        jni::exception::throw_if_error(handle_, qdb_ts_batch_release_columns_memory(table_));

        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1table_1extra_1columns(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong table, jobjectArray columns)
{
    qdb::jni::env env(jniEnv);

    qdb_handle_t handle_     = reinterpret_cast<qdb_handle_t>(handle);
    qdb_batch_table_t table_ = reinterpret_cast<qdb_batch_table_t>(table);

    try
    {
        jni::object_array columns_{env, columns};
        std::vector<qdb_ts_batch_column_info_t> xs =
            jni::adapt::columns::to_qdb<qdb_ts_batch_column_info_t>(env, handle_, columns_);

        jni::exception::throw_if_error(
            handle_, qdb_ts_batch_table_extra_columns(table_, xs.data(), xs.size()));

        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1table_1release(
    JNIEnv * /*env*/, jclass /*thisClass*/, jlong handle, jlong batchTable)
{
    // On 32-bit systems, `jlong` is, in fact, 64-bit, but pointers are 32-bit.
    // This implies that this cast _could_ technically be unsafe.
    //
    // Of course, because `jlong` is able to represent *more* than our pointers,
    // and that these were originally also pointers, implies that practically speaking,
    // this is safe.
    //
    // As such, these assertions are most likely to catch memory corruptions or
    // other bugs.
    static_assert(sizeof(jlong) >= sizeof(qdb_handle_t));
    static_assert(sizeof(jlong) >= sizeof(qdb_batch_table_t));

    qdb_handle_t handle_           = reinterpret_cast<qdb_handle_t>(handle);
    qdb_batch_table_t batch_table_ = reinterpret_cast<qdb_batch_table_t>(batchTable);

    qdb_release(handle_, batch_table_);
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1push(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batchTable)
{
    qdb::jni::env env(jniEnv);
    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle, qdb_ts_batch_push((qdb_batch_table_t)batchTable));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1push_1async(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batchTable)
{
    qdb::jni::env env(jniEnv);

    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle, qdb_ts_batch_push_async((qdb_batch_table_t)batchTable));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1push_1fast(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batchTable)
{
    qdb::jni::env env(jniEnv);

    try
    {
        return jni::exception::throw_if_error(
            (qdb_handle_t)handle, qdb_ts_batch_push_fast((qdb_batch_table_t)batchTable));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1start_1row(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong batchTable, jlong sec, jlong nsec)
{
    qdb::jni::env env(jniEnv);

    qdb_timespec_t ts{sec, nsec};
    return qdb_ts_batch_start_row((qdb_batch_table_t)(batchTable), &ts);
}

JNIEXPORT jint JNICALL JavaCritical_net_quasardb_qdb_jni_qdb_ts_1batch_1start_1row(
    jlong batchTable, jlong sec, jlong nsec)
{
    qdb_timespec_t ts{sec, nsec};
    return qdb_ts_batch_start_row((qdb_batch_table_t)(batchTable), &ts);
}

/****
 * 'regular' column set.
 *
 * The functions below are here to keep the old functionality of 'regular' batch
 * row set functions. Due to performance concerns these functions are set to be
 * deprecated once we're certain the pinned column approach is stable.
 */

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1double(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong batchTable, jlong index, double val)
{
    static_assert(sizeof(jint) >= sizeof(qdb_error_t));
    qdb::jni::env env(jniEnv);
    qdb_error_t err = qdb_ts_batch_row_set_double(
        jni::native_ptr::from_java<qdb_batch_table_t>(batchTable), index, val);
    return (jint)err;
}

JNIEXPORT jint JNICALL JavaCritical_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1double(
    jlong batchTable, jlong index, double val)
{
    return qdb_ts_batch_row_set_double((qdb_batch_table_t)(batchTable), index, val);
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1int64(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong batchTable, jlong index, jlong val)
{
    qdb::jni::env env(jniEnv);
    qdb_error_t err = qdb_ts_batch_row_set_int64((qdb_batch_table_t)(batchTable), index, val);

    return (jint)err;
}

JNIEXPORT jint JNICALL JavaCritical_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1int64(
    jlong batchTable, jlong index, jlong val)
{
    qdb_error_t err = qdb_ts_batch_row_set_int64((qdb_batch_table_t)(batchTable), index, val);
    return (jint)err;
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1timestamp(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong batchTable, jlong index, jlong sec, jlong nsec)
{
    qdb::jni::env env(jniEnv);
    qdb_timespec_t ts{sec, nsec};
    qdb_error_t err = qdb_ts_batch_row_set_timestamp((qdb_batch_table_t)(batchTable), index, &ts);

    return (jint)err;
}

JNIEXPORT jint JNICALL JavaCritical_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1timestamp(
    jlong batchTable, jlong index, jlong sec, jlong nsec)
{
    qdb_timespec_t ts{sec, nsec};

    qdb_error_t err = qdb_ts_batch_row_set_timestamp((qdb_batch_table_t)(batchTable), index, &ts);
    return err;
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1blob(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong batchTable, jlong index, jobject bb)
{
    qdb::jni::env env(jniEnv);

    void * addr      = (bb == NULL ? NULL : env.instance().GetDirectBufferAddress(bb));
    qdb_size_t bytes = (addr == NULL ? 0 : (qdb_size_t)env.instance().GetDirectBufferCapacity(bb));
    qdb_error_t err =
        qdb_ts_batch_row_set_blob((qdb_batch_table_t)(batchTable), index, addr, bytes);

    return err;
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1row_1set_1string(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong batchTable, jlong index, jbyteArray bb)
{
    qdb::jni::env env(jniEnv);

    qdb_error_t result;
    if (bb == NULL)
    {
        result = qdb_ts_batch_row_set_string((qdb_batch_table_t)(batchTable), index, NULL, 0);
    }
    else
    {
        jni::guard::byte_array barry(jni::byte_array::get_bytes(env, bb));

        result = qdb_ts_batch_row_set_string(
            (qdb_batch_table_t)(batchTable), index, (char const *)barry.ptr(), barry.len());
    }

    return (jint)result;
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1batch_1push_1truncate(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong batchTable, jobjectArray ranges)
{
    qdb::jni::env env(jniEnv);

    try
    {
        std::vector<qdb_ts_range_t> native_ranges =
            jni::adapt::timerange::to_qdb(env, jni::object_array{env, ranges});

        return jni::exception::throw_if_error(
            (qdb_handle_t)handle, qdb_ts_batch_push_truncate((qdb_batch_table_t)batchTable,
                                      native_ranges.data(), native_ranges.size()));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jlong JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1local_1table_1init(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jstring alias, jobjectArray columns)

{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_local_table_t ret{nullptr};
        static_assert(sizeof(jlong) >= sizeof(qdb_local_table_t));

        std::vector<qdb_ts_column_info_t> columns_ =
            jni::adapt::columns::to_qdb<qdb_ts_column_info_t>(
                env, handle_, jni::object_array{env, columns});

        auto alias_ = qdb::jni::string::get_chars_utf8(env, handle_, alias);

        qdb::jni::exception::throw_if_error(handle_,
            qdb_ts_local_table_init(handle_, alias_, columns_.data(), columns_.size(), &ret));

        return jni::native_ptr::to_java(ret);
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1local_1table_1release(
    JNIEnv * /*env*/, jclass /*thisClass*/, jlong handle, jlong localTable)
{
    qdb_release((qdb_handle_t)handle, (qdb_local_table_t)localTable);
}

JNIEXPORT jint JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1table_1get_1ranges(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong localTable, jobjectArray ranges)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_           = reinterpret_cast<qdb_handle_t>(handle);
        qdb_local_table_t local_table_ = reinterpret_cast<qdb_local_table_t>(localTable);

        std::vector<qdb_ts_range_t> ranges_ =
            jni::adapt::timerange::to_qdb(env, jni::object_array{env, ranges});

        return jni::exception::throw_if_error(
            handle_, qdb_ts_table_get_ranges(local_table_, ranges_.data(), ranges_.size()));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT jobject JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1table_1next_1row(
    JNIEnv * jniEnv, jclass /*thisClass*/, jlong handle, jlong localTable, jobjectArray columns)
{
    qdb::jni::env env(jniEnv);

    try
    {
        qdb_handle_t handle_           = reinterpret_cast<qdb_handle_t>(handle);
        qdb_local_table_t local_table_ = reinterpret_cast<qdb_local_table_t>(localTable);

        std::vector<qdb_ts_column_info_ex_t> columns_ =
            jni::adapt::columns::to_qdb<qdb_ts_column_info_ex_t>(
                env, handle_, jni::object_array{env, columns});

        auto ret = jni::adapt::local_table::next_row(env, handle_, local_table_, columns_);

        return ret.release();
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return nullptr;
    }
}

qdb_exp_batch_push_table_t & _table_from_tables(qdb_exp_batch_push_table_t * tables, jlong tableNum)
{
    return tables[tableNum];
}

qdb_exp_batch_push_table_t & _table_from_tables(jlong batchTables, jlong tableNum)
{
    return _table_from_tables(
        reinterpret_cast<qdb_exp_batch_push_table_t *>(batchTables), tableNum);
}

qdb_exp_batch_push_column_t & _batch_column_from_tables(
    qdb_exp_batch_push_table_t * tables, jlong tableNum, jlong columnNum)
{
    qdb_exp_batch_push_table_data_t & data = tables[tableNum].data;

    assert(columnNum < data.column_count);

    return const_cast<qdb_exp_batch_push_column_t &>(data.columns[columnNum]);
}

qdb_exp_batch_push_column_t & _batch_column_from_tables(
    jlong batchTables, jlong tableNum, jlong columnNum)
{
    return _batch_column_from_tables(
        reinterpret_cast<qdb_exp_batch_push_table_t *>(batchTables), tableNum, columnNum);
}

/**
 * Batch writer
 */

void _timestamps_from_timespecs(int n, jlong * sec, jlong * nsec, qdb_timespec_t * out)
{
    // We do a fast copy and rely on this assumption
    static_assert(sizeof(jlong) == sizeof(qdb_time_t));

    for (int i = 0; i < n; ++i)
    {
        out[i].tv_sec  = sec[i];
        out[i].tv_nsec = nsec[i];
    }
}

void _timestamps_from_timespecs(qdb::jni::env & env, jobject values, qdb_timespec_t * out)
{
    jclass timespecClass = jni::object::get_class(env, values);
    jfieldID secField    = jni::introspect::lookup_field(env, timespecClass, "sec", "[J");
    jfieldID nsecField   = jni::introspect::lookup_field(env, timespecClass, "nsec", "[J");
    jlongArray secArray  = (jlongArray)env.instance().GetObjectField(values, secField);
    jlongArray nsecArray = (jlongArray)env.instance().GetObjectField(values, nsecField);

    auto sec_guard  = jni::make_primitive_array<jlong>(env, secArray);
    auto nsec_guard = jni::make_primitive_array<jlong>(env, nsecArray);

    _timestamps_from_timespecs(sec_guard.size(), sec_guard.get(), nsec_guard.get(), out);
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1double(
    JNIEnv * jniEnv,
    jclass /* thisClass */,
    jlong handle,
    jlong batchTables,
    jlong tableNum,
    jlong columnNum,
    jstring name,
    jdoubleArray values)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_exp_batch_push_column_t & column =
            _batch_column_from_tables(batchTables, tableNum, columnNum);

        auto arr         = jni::make_primitive_array<double>(env, values);
        column.name      = jni::string::get_chars_utf8(env, handle_, name).as_qdb(handle_);
        column.data_type = qdb_ts_column_double;

        // NOTE(leon): column.data.doubles is heap-allocated and will remain around until
        //             java application calls qdb.ts_exp_batch_release()
        column.data.doubles = arr.copy(handle_);
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1int64(
    JNIEnv * jniEnv,
    jclass /* thisClass */,
    jlong handle,
    jlong batchTables,
    jlong tableNum,
    jlong columnNum,
    jstring name,
    jlongArray values)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_exp_batch_push_column_t & column =
            _batch_column_from_tables(batchTables, tableNum, columnNum);

        auto arr = jni::make_primitive_array<qdb_int_t>(env, values);

        column.name      = jni::string::get_chars_utf8(env, handle_, name).as_qdb(handle_);
        column.data_type = qdb_ts_column_int64;

        // NOTE(leon): column.data.ints is heap-allocated and will remain around until
        //             java application calls qdb.ts_exp_batch_release()
        column.data.ints = arr.copy(handle_);
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1blob(
    JNIEnv * jniEnv,
    jclass /* thisClass */,
    jlong handle,
    jlong batchTables,
    jlong tableNum,
    jlong columnNum,
    jstring name,
    jobjectArray values_)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_exp_batch_push_column_t & column =
            _batch_column_from_tables(batchTables, tableNum, columnNum);

        jni::object_array values(env, values_);

        column.name      = jni::string::get_chars_utf8(env, handle_, name).as_qdb(handle_);
        column.data_type = qdb_ts_column_blob;

        qdb_blob_t * ret = jni::allocate<qdb_blob_t>(handle_, values.size());

        for (qdb_size_t i = 0; i < values.size(); ++i)
        {
            jobject bb = values.get(i);
            jni::byte_buffer::as_qdb_blob(env, handle_, bb, ret[i]);
        }

        // NOTE(leon): column.data.blobs is heap-allocated and will remain around until
        //             java application calls qdb.ts_exp_batch_release()
        column.data.blobs = ret;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1string(
    JNIEnv * jniEnv,
    jclass /* thisClass */,
    jlong handle,
    jlong batchTables,
    jlong tableNum,
    jlong columnNum,
    jstring name,
    jobjectArray values_)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_exp_batch_push_column_t & column =
            _batch_column_from_tables(batchTables, tableNum, columnNum);

        jni::object_array values(env, values_);

        column.name      = jni::string::get_chars_utf8(env, handle_, name).as_qdb(handle_);
        column.data_type = qdb_ts_column_string;

        qdb_string_t * ret = jni::allocate<qdb_string_t>(handle_, values.size());

        for (qdb_size_t i = 0; i < values.size(); ++i)
        {
            jobject bb = values.get(i);
            jni::byte_buffer::as_qdb_string(env, handle_, bb, ret[i]);
        }

        // NOTE(leon): column.data.strings is heap-allocated and will remain around until
        //             java application calls qdb.ts_exp_batch_release()
        column.data.strings = ret;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1column_1from_1timestamp(
    JNIEnv * jniEnv,
    jclass /* thisClass */,
    jlong handle,
    jlong batchTables,
    jlong tableNum,
    jlong columnNum,
    jstring name,
    jobject values)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        qdb_exp_batch_push_table_t * xs =
            reinterpret_cast<qdb_exp_batch_push_table_t *>(batchTables);
        qdb_exp_batch_push_table_t & table = xs[tableNum];

        qdb_size_t values_count     = table.data.row_count;
        qdb_timespec_t * timestamps = jni::allocate<qdb_timespec_t>(handle_, values_count);

        _timestamps_from_timespecs(env, values, timestamps);

        qdb_exp_batch_push_column_t & column = _batch_column_from_tables(xs, tableNum, columnNum);

        column.name      = jni::string::get_chars_utf8(env, handle_, name).as_qdb(handle_);
        column.data_type = qdb_ts_column_timestamp;

        // NOTE(leon): column.data.timestamps is heap-allocated and will remain around until
        //             java application calls qdb.ts_exp_batch_release()
        column.data.timestamps = timestamps;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1set_1table_1data(
    JNIEnv * jniEnv,
    jclass /* thisClass */,
    jlong handle,
    jlong batchTables,
    jlong tableNum,
    jstring tableName,
    jobject timespecs_)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_               = reinterpret_cast<qdb_handle_t>(handle);
        qdb_exp_batch_push_table_t & table = _table_from_tables(batchTables, tableNum);

        table.name = jni::string::get_chars_utf8(env, handle_, tableName).as_qdb(handle_);

        _timestamps_from_timespecs(
            env, timespecs_, const_cast<qdb_timespec_t *>(table.data.timestamps));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1table_1set_1drop_1duplicates(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong batchTables, jlong tableNum)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_exp_batch_push_table_t & table = _table_from_tables(batchTables, tableNum);
        table.options                      = qdb_exp_batch_option_unique;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
};

JNIEXPORT void JNICALL
Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1table_1set_1drop_1duplicate_1columns(JNIEnv * jniEnv,
    jclass /* thisClass */,
    jlong handle,
    jlong batchTables,
    jlong tableNum,
    jobjectArray columns)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        jni::object_array columns_{env, columns};
        qdb_exp_batch_push_table_t & table = _table_from_tables(batchTables, tableNum);

        assert(table.options == qdb_exp_batch_option_unique);

        std::size_t n = std::size(columns_);

        table.where_duplicate_count = n;
        table.where_duplicate       = jni::allocate<qdb_string_t>(handle_, n);

        for (qdb_size_t i = 0; i < n; ++i)
        {
            jstring in{static_cast<jstring>(columns_.get(i))};
            jni::string::get_chars_utf8(env, handle_, in).as_qdb(handle_, table.where_duplicate[i]);
        };
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
};

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1table_1set_1truncate_1ranges(
    JNIEnv * jniEnv, jclass /* thisClass */, jlong batchTables, jlong tableNum, jobjectArray ranges)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_exp_batch_push_table_t & table = _table_from_tables(batchTables, tableNum);

        jni::object_array ranges_{env, ranges};

        std::unique_ptr<qdb_ts_range_t[]> ret = std::make_unique<qdb_ts_range_t[]>(ranges_.size());
        std::span<qdb_ts_range_t> ret_{ret.get(), ranges_.size()};

        jni::adapt::timerange::to_qdb(env, ranges_, ret_.begin());

        table.truncate_ranges      = ret.release();
        table.truncate_range_count = ranges_.size();
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
    }
}

JNIEXPORT jlong JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1prepare(JNIEnv * jniEnv,
    jclass /* thisClass */,
    jlong handle,
    jlongArray rowCount_,
    jlongArray columnCount_)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);
        auto row_guard       = jni::make_primitive_array<jlong>(env, rowCount_);
        auto column_guard    = jni::make_primitive_array<jlong>(env, columnCount_);

        assert(row_guard.size() == column_guard.size());

        auto ret = std::make_unique<qdb_exp_batch_push_table_t[]>(row_guard.size());

        for (qdb_size_t i = 0; i < row_guard.size(); ++i)
        {
            qdb_size_t row_count    = row_guard[i];
            qdb_size_t column_count = column_guard[i];

            ret[i].data.row_count    = row_count;
            ret[i].data.column_count = column_count;

            ret[i].data.timestamps = jni::allocate<qdb_timespec_t>(handle_, row_count);
            ret[i].data.columns = jni::allocate<qdb_exp_batch_push_column_t>(handle_, column_count);

            ret[i].truncate_ranges      = nullptr;
            ret[i].truncate_range_count = 0;
        }

        return reinterpret_cast<jlong>(ret.release());
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}

JNIEXPORT void JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1release(
    JNIEnv * /* jniEnv */, jclass /* thisClass */, jlong handle, jlong tables, jlong tables_count)
{
    qdb_handle_t handle_ = reinterpret_cast<qdb_handle_t>(handle);

    qdb_exp_batch_push_table_t * xs = reinterpret_cast<qdb_exp_batch_push_table_t *>(tables);

    for (jlong i = 0; i < tables_count; ++i)
    {
        for (jlong j = 0; j < xs[i].data.column_count; ++j)
        {
            qdb_release(handle_, xs[i].data.columns[j].name.data);

            switch (xs[i].data.columns[j].data_type)
            {
            case qdb_ts_column_double:
                qdb_release(handle_, xs[i].data.columns[j].data.doubles);
                break;
            case qdb_ts_column_int64:
                qdb_release(handle_, xs[i].data.columns[j].data.ints);
                break;
            case qdb_ts_column_blob:
            {
                for (jlong k = 0; k < xs[i].data.row_count; ++k)
                {
                    if (xs[i].data.columns[j].data.blobs[k].content != nullptr)
                    {
                        qdb_release(handle_, xs[i].data.columns[j].data.blobs[k].content);
                    }
                }

                qdb_release(handle_, xs[i].data.columns[j].data.blobs);
                break;
            }
            case qdb_ts_column_string:
            {
                for (jlong k = 0; k < xs[i].data.row_count; ++k)
                {
                    if (xs[i].data.columns[j].data.strings[k].data != nullptr)
                    {
                        qdb_release(handle_, xs[i].data.columns[j].data.strings[k].data);
                    }
                }

                qdb_release(handle_, xs[i].data.columns[j].data.strings);
                break;
            }
            case qdb_ts_column_timestamp:
                qdb_release(handle_, xs[i].data.columns[j].data.timestamps);
                break;
            default:
                throw new jni::exception(qdb_e_incompatible_type, "Unrecognized column type");
            }
        }

        if (xs[i].truncate_ranges != nullptr)
        {
            delete xs[i].truncate_ranges;
        }

        qdb_release(handle_, xs[i].name.data);
        qdb_release(handle_, xs[i].data.timestamps);
        qdb_release(handle_, xs[i].data.columns);
    }
    delete[] xs;
}

JNIEXPORT jlong JNICALL Java_net_quasardb_qdb_jni_qdb_ts_1exp_1batch_1push(JNIEnv * jniEnv,
    jclass /* thisClass */,
    jlong handle,
    jint pushMode,
    jlong tables_,
    jlong tables_count)
{
    qdb::jni::env env(jniEnv);
    try
    {
        qdb_exp_batch_push_table_t * tables =
            reinterpret_cast<qdb_exp_batch_push_table_t *>(tables_);

        qdb_exp_batch_push_mode_t push_mode = qdb_exp_batch_push_transactional;

        // Needs to be kept in sync with the Writer.PushMode enum
        switch (pushMode)
        {
        case 0:
            push_mode = qdb_exp_batch_push_transactional;
            break;
        case 1:
            push_mode = qdb_exp_batch_push_async;
            break;
        case 2:
            push_mode = qdb_exp_batch_push_fast;
            break;
        case 3:
            push_mode = qdb_exp_batch_push_truncate;
            break;
        default:
            throw new jni::exception(qdb_e_incompatible_type, "Unrecognized push mode");
        };

        qdb::jni::exception::throw_if_error((qdb_handle_t)handle,
            qdb_exp_batch_push((qdb_handle_t)handle, push_mode, tables, NULL, tables_count));

        return qdb_e_ok;
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return e.error();
    }
}
