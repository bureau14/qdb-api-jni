#pragma once

#include "memory.h"
#include <qdb/client.h> // qdb_size_t
#include <qdb/ts.h>     // qdb_blob_t
#include "guard/local_ref.h"
#include <jni.h>
#include <string.h>

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
     * Wraps around java.nio.ByteBuffer.allocate().
     *
     * Allocates JVM-managed bytebuffer of size `n`. This memory will be automatically released
     * by the JVM, so that we don't have to deal with the mess that is direct-allocated bytebuffers.
     */
    static jni::guard::local_ref<jobject> allocate(jni::env & env, jsize len);

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

    template <typename T>
    static inline void copy_into(
        qdb::jni::env & env, qdb_handle_t handle, jobject bb, T const ** xs, qdb_size_t * n)
    {
        if (bb == NULL)
        {
            *xs = nullptr;
            *n  = 0;
            return;
        }

        qdb_size_t n_    = static_cast<qdb_size_t>(env.instance().GetDirectBufferCapacity(bb));
        void const * src = env.instance().GetDirectBufferAddress(bb);
        char * xs_       = qdb::jni::memory::allocate<char>(handle, n_);

        memcpy(xs_, src, n_);

        *xs = xs_;
        *n  = n_;
    }

    static inline void as_qdb_blob(
        qdb::jni::env & env, qdb_handle_t handle, jobject bb, qdb_blob_t & out)
    {
        copy_into(env, handle, bb, &out.content, &out.content_length);
    }

    static inline void as_qdb_string(
        qdb::jni::env & env, qdb_handle_t handle, jobject bb, qdb_string_t & out)
    {
        copy_into(env, handle, bb, &out.data, &out.length);
    }

    static inline qdb_blob_t to_qdb(qdb::jni::env & env, jobject in)
    {
        qdb_blob_t ret{nullptr, 0};
        get_address(env, in, &ret.content, &ret.content_length);
        return ret;
    }
};
}; // namespace jni
}; // namespace qdb
