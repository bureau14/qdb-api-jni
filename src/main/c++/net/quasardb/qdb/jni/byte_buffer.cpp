#include <iostream>
#include "byte_buffer.h"
#include "env.h"
#include "object.h"
#include <string.h>

/* static */ qdb::jni::guard::local_ref<jobject>
qdb::jni::byte_buffer::create(qdb::jni::env &env, void *buffer, jsize len)
{
    assert(buffer != NULL);
    assert(len > 0);

    return std::move(qdb::jni::guard::local_ref<jobject>(
        env, env.instance().NewDirectByteBuffer(buffer, len)));
}

/* static */ qdb::jni::guard::local_ref<jobject>
qdb::jni::byte_buffer::create_copy(qdb::jni::env &env,
                                   void const *src,
                                   jsize len)
{
    assert(src != NULL);
    assert(len > 0);

    void *dest = malloc(len);
    assert(dest != NULL);

    memcpy(dest, src, len);

    // We do not free because ownership of the memory area is moved to the
    // ByteBuffer, which automatically frees the area when the GC decides
    // to clean it up.
    return create(env, dest, len);
}

/* static */  void
qdb::jni::byte_buffer::get_address(qdb::jni::env & env, jobject bb, const void ** buffer, qdb_size_t * len) {
  *buffer = env.instance().GetDirectBufferAddress(bb);
  *len = static_cast<qdb_size_t>(env.instance().GetDirectBufferCapacity(bb));
}


/* static */  void
qdb::jni::byte_buffer::as_qdb_blob(qdb::jni::env & env, jobject bb, qdb_blob_t & out) {
  if (bb == NULL) {
    out.content = nullptr;
    out.content_length = 0;
    return;
  }

  qdb_size_t len = static_cast<qdb_size_t>(env.instance().GetDirectBufferCapacity(bb));
  void const * src = env.instance().GetDirectBufferAddress(bb);
  void *dest = malloc(len);

  assert(dest != NULL);
  memcpy(dest, src, len);

  out.content = dest;
  out.content_length = len;
}


/* static */  void
qdb::jni::byte_buffer::as_qdb_string(qdb::jni::env & env, jobject bb, qdb_string_t & out) {
  if (bb == NULL) {
    out.data = nullptr;
    out.length = 0;
    return;
  }

  qdb_size_t len = static_cast<qdb_size_t>(env.instance().GetDirectBufferCapacity(bb));
  void const * src = env.instance().GetDirectBufferAddress(bb);
  void *dest = malloc(len);

  assert(dest != NULL);
  memcpy(dest, src, len);

  out.data = reinterpret_cast<char const *>(dest);
  out.length = len;
}
