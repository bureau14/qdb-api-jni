#include "byte_buffer.h"
#include "env.h"
#include "introspect.h"
#include "object.h"
#include <string.h>

/* static */ qdb::jni::guard::local_ref<jobject> qdb::jni::byte_buffer::allocate(
    qdb::jni::env & env, jsize len)
{
    assert(len > 0);

    // Relevant: by invoking allocateDirect, the 'cleaner' property is set, which
    // is *not* set when invoking env.instance().NewDirectByteBuffer
    //
    // See: https://stackoverflow.com/questions/35363486#35364247
    return jni::object::call_static_method(
        env, "java/nio/ByteBuffer", "allocateDirect", "(I)Ljava/nio/ByteBuffer;", len);
}

/* static */ qdb::jni::guard::local_ref<jobject> qdb::jni::byte_buffer::create_copy(
    qdb::jni::env & env, qdb_handle_t handle, void const * src, jsize len)
{
    assert(src != NULL);
    assert(len > 0);

    // We'll first allocate a JVM-managed bytebuffer, and then copy our src
    // buffer into there. We specifically do not use JNI's AllocateDirectByteBuffer,
    // so that we can avoid the mess with deallocating the buffer (which is a mess).
    qdb::jni::guard::local_ref<jobject> bb = allocate(env, len);

    assert(env.instance().GetDirectBufferCapacity(bb) == len);

    // Get pointer to underlying buffer
    void * dest = env.instance().GetDirectBufferAddress(bb);

    memcpy(dest, src, len);

    return bb;
}

/* static */ void qdb::jni::byte_buffer::get_address(
    qdb::jni::env & env, jobject bb, const void ** buffer, qdb_size_t * len)
{
    *buffer = env.instance().GetDirectBufferAddress(bb);
    *len    = static_cast<qdb_size_t>(env.instance().GetDirectBufferCapacity(bb));
}

/* static */ void qdb::jni::byte_buffer::as_qdb_blob(
    qdb::jni::env & env, jobject bb, qdb_blob_t & out)
{
    if (bb == NULL)
    {
        out.content        = nullptr;
        out.content_length = 0;
        return;
    }

    qdb_size_t len   = static_cast<qdb_size_t>(env.instance().GetDirectBufferCapacity(bb));
    void const * src = env.instance().GetDirectBufferAddress(bb);
    void * dest      = malloc(len);

    assert(dest != NULL);
    memcpy(dest, src, len);

    out.content        = dest;
    out.content_length = len;
}

/* static */ void qdb::jni::byte_buffer::as_qdb_string(
    qdb::jni::env & env, jobject bb, qdb_string_t & out)
{
    if (bb == NULL)
    {
        out.data   = nullptr;
        out.length = 0;
        return;
    }

    qdb_size_t len   = static_cast<qdb_size_t>(env.instance().GetDirectBufferCapacity(bb));
    void const * src = env.instance().GetDirectBufferAddress(bb);
    void * dest      = malloc(len);

    assert(dest != NULL);
    memcpy(dest, src, len);

    out.data   = reinterpret_cast<char const *>(dest);
    out.length = len;
}
