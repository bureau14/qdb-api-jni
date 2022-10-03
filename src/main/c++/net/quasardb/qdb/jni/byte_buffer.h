#pragma once

#include "introspect.h"
#include <qdb/client.h> // qdb_size_t
#include <qdb/ts.h>     // qdb_blob_t
#include "guard/local_ref.h"
#include <jni.h>

namespace qdb
{
namespace jni
{

class env;

/**
 * Wraps around all JNI operations that relate to byte buffer management.
 */
class byte_buffer
{
public:
    /**
     * Create new byte buffer. Ownership of the allocated memory is moved to the
     * ByteBuffer, and the memory address must remain valid for the lifetime of
     * the jobject.
     */
    static jni::guard::local_ref<jobject> create(qdb::jni::env & env, void * buffer, jsize len);

    /**
     * Create new byte buffer. Memory in buffer is copied.
     */
    static jni::guard::local_ref<jobject> create_copy(
        qdb::jni::env & env, qdb_handle_t handle, void const * buffer, jsize len);

    static jni::guard::local_ref<jobject> create_copy(
        qdb::jni::env & env, qdb_handle_t handle, qdb_blob_t blob)
    {
        return create_copy(env, handle, blob.content, static_cast<jsize>(blob.content_length));
    }

    static void get_address(
        qdb::jni::env & env, jobject bb, const void ** buffer, qdb_size_t * len);

    static void as_qdb_blob(qdb::jni::env & env, jobject bb, qdb_blob_t & out);

    static void as_qdb_string(qdb::jni::env & env, jobject bb, qdb_string_t & out);

    static inline qdb_blob_t to_qdb(qdb::jni::env & env, jobject in)
    {
        qdb_blob_t ret{nullptr, 0};
        get_address(env, in, &ret.content, &ret.content_length);
        return ret;
    }
};
}; // namespace jni
}; // namespace qdb
