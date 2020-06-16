#pragma once

#include <jni.h>
#include <qdb/client.h> // for qdb_string_t

#include "guard/local_ref.h"
#include "guard/array_critical.h"
#include "introspect.h"

namespace qdb
{
namespace jni
{

class env;

/**
 * Wraps around all JNI operations that relate to string management.
 */
class primitive_array
{
  public:

    /**
     * Returns characters in modified UTF-8 encoding. This might create
     * a copy of the underlying string.
     */
     template <typename T>
     static jni::guard::array_critical<T> get_array_critical(qdb::jni::env &env,
                                                             jarray arr) {
      jint len = env.instance().GetArrayLength(arr);
      T * xs = static_cast<T *>(env.instance().GetPrimitiveArrayCritical(arr, 0));
      assert(xs != NULL);
      return guard::array_critical<T>(env, arr, xs, len);

    }

};
}; // namespace jni
}; // namespace qdb
