#include <cassert>

#include "../env.h"
#include "../debug.h"
#include "../introspect.h"
#include "../util/ts_helpers.h"
#include "../guard/local_ref.h"
#include "../byte_buffer.h"
#include "../object.h"
#include "qdb_value.h"

/* static */ jobject
qdb::jni::ts::value::from_native(qdb::jni::env & env, qdb_point_result_t const & input) {
    // :TODO: cache!
    jclass valueClass = qdb::jni::introspect::lookup_class(env, "net/quasardb/qdb/ts/Value");
    jmethodID constructor = qdb::jni::introspect::lookup_static_method(env, valueClass,
                                                                       "createNull",
                                                                       "()Lnet/quasardb/qdb/ts/Value;");

    switch (input.type) {

        case qdb_query_result_int64:
            return _from_native_int64(env, input).release();
            break;

        case qdb_query_result_double:
            return _from_native_double(env, input).release();
            break;

        case qdb_query_result_timestamp:
            return _from_native_timestamp(env, input).release();
            break;

        case qdb_query_result_blob:
            return _from_native_blob(env, input).release();
            break;

        case qdb_query_result_none:
            return _from_native_null(env).release();
            break;

    };

    return _from_native_null(env);
}

/* static */ qdb::jni::guard::local_ref<jobject>
qdb::jni::ts::value::_from_native_int64(qdb::jni::env & env, qdb_point_result_t const & input) {
    return std::move(
        jni::object::call_static_method(env,
                                        "net/quasardb/qdb/ts/Value",
                                        "createInt64",
                                        "(J)Lnet/quasardb/qdb/ts/Value;",
                                        input.payload.int64_.value));
}

/* static */ qdb::jni::guard::local_ref<jobject>
qdb::jni::ts::value::_from_native_double(qdb::jni::env & env, qdb_point_result_t const & input) {
    return std::move(
        jni::object::call_static_method(env,
                                        "net/quasardb/qdb/ts/Value",
                                        "createDouble",
                                        "(D)Lnet/quasardb/qdb/ts/Value;",
                                        input.payload.double_.value));
}

/* static */ qdb::jni::guard::local_ref<jobject>
qdb::jni::ts::value::_from_native_timestamp(qdb::jni::env & env, qdb_point_result_t const & input) {
    return std::move(
        jni::object::call_static_method(env,
                                        "net/quasardb/qdb/ts/Value",
                                        "createTimestamp",
                                        "(Lnet/quasardb/qdb/ts/Timespec;)Lnet/quasardb/qdb/ts/Value;",
                                        nativeToTimespec(env, input.payload.timestamp.value).release()));
}

/* static */ qdb::jni::guard::local_ref<jobject>
qdb::jni::ts::value::_from_native_blob(qdb::jni::env & env, qdb_point_result_t const & input) {
    jclass valueClass = qdb::jni::introspect::lookup_class(env, "net/quasardb/qdb/ts/Value");
    jmethodID constructor = qdb::jni::introspect::lookup_static_method(env,
                                                                       valueClass,
                                                                       "createSafeBlob",
                                                                       "(Ljava/nio/ByteBuffer;)Lnet/quasardb/qdb/ts/Value;");

    return std::move(
        jni::object::call_static_method(env,
                                        "net/quasardb/qdb/ts/Value",
                                        "createSafeBlob",
                                        "(Ljava/nio/ByteBuffer;)Lnet/quasardb/qdb/ts/Value;",
                                        jni::byte_buffer::create_copy(env,
                                                                      input.payload.blob.content,
                                                                      input.payload.blob.content_length).release()));
}

/* static */ qdb::jni::guard::local_ref<jobject>
qdb::jni::ts::value::_from_native_null(qdb::jni::env & env) {
    return std::move(
        jni::object::call_static_method(env,
                                        "net/quasardb/qdb/ts/Value",
                                        "createNull",
                                        "()Lnet/quasardb/qdb/ts/Value;"));
}
