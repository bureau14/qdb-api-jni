#pragma once

#include <jni.h>
#include <qdb/client.h> // qdb_size_t
#include "guard/local_ref.h"
#include "introspect.h"

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
    static jni::guard::local_ref<jobject>
    create(qdb::jni::env &env, void *buffer, jsize len);

    /**
     * Create new byte buffer. Memory in buffer is copied.
     */
    static jni::guard::local_ref<jobject>
    create_copy(qdb::jni::env &env, void const *buffer, jsize len);


    static void
    get_address(qdb::jni::env & env, jobject bb, const void ** buffer, qdb_size_t * len);

};
}; // namespace jni
}; // namespace qdb
