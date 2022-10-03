#include "byte_buffer.h"
#include "allocate.h"
#include "env.h"
#include "object.h"
#include <iostream>
#include <string.h>

/* static */ qdb::jni::guard::local_ref<jobject> qdb::jni::byte_buffer::create(
    qdb::jni::env & env, void * buffer, jsize len)
{
    assert(buffer != NULL);
    assert(len > 0);

    return qdb::jni::guard::local_ref<jobject>(
        env, env.instance().NewDirectByteBuffer(buffer, len));
}

/* static */ qdb::jni::guard::local_ref<jobject> qdb::jni::byte_buffer::create_copy(
    qdb::jni::env & env, qdb_handle_t handle, void const * src, jsize len)
{
    assert(src != NULL);
    assert(len > 0);

    void * dest = (void *)jni::allocate<char>(handle, len);
    assert(dest != NULL);

    memcpy(dest, src, len);

    // XXX(leon): ownership of the memory area is "moved" from native to JVM. I believe
    //            that the ByteBuffer supports some kind of 'cleaner'; this is a utility
    //            object which is run when the object is garbage collected.
    //
    //            we may need to explore this further, see:
    //
    //            * https://stackoverflow.com/a/35364247/1764661
    //            * https://stackoverflow.com/a/6699007/1764661
    //            * https://docs.oracle.com/javase/6/docs/api/java/lang/ref/PhantomReference.html
    return create(env, dest, len);
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
