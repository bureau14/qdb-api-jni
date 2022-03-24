#pragma once

#include "introspect.h"
#include "guard/byte_array.h"
#include <jni.h>

namespace qdb
{
namespace jni
{

class env;

/**
 * Wraps around all JNI operations that relate to byte array management.
 */
class byte_array
{
public:
    /**
     * Get a reference to the underlying byte array.
     */
    static jni::guard::byte_array get_bytes(qdb::jni::env & env, jbyteArray bb)
    {
        return guard::byte_array(env, bb);
    }
};
}; // namespace jni
}; // namespace qdb
