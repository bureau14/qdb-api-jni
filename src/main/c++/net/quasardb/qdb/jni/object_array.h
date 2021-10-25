#pragma once

#include <jni.h>
#include <stdio.h>

#include "env.h"

namespace qdb
{
namespace jni
{

/**
 * Helper class that wraps around a jobjectArray, so that we
 * manage the access and that the memory can be released
 * back to the JVM as soon as possible.
 */
class object_array
{
  private:
    qdb::jni::env &_env;
    jobjectArray _arr;
    qdb_size_t _len;

  public:

    object_array(qdb::jni::env & env, jobjectArray & arr)
     : _env(env), _arr(arr)
    {
      _len = _env.instance().GetArrayLength(_arr);
    }

    object_array(object_array &&o) noexcept
      : _env(o._env), _arr(o._arr), _len(o._len)
    {
        // By setting the other ptr to NULL, we're now effectively
        // claiming ownership of the object array.
        o._arr = NULL;
    }

    ~object_array()
    {
        if (_arr != NULL)
        {
          _arr = NULL;
        }
    }

    object_array(object_array const &) = delete;
    object_array &operator=(object_array const &) = delete;

    constexpr qdb_size_t size() const {
      return _len;
    }

    jobject operator[](qdb_size_t i) const {
      return get(i);
    }

    jobject get(qdb_size_t i) const {
      return _env.instance().GetObjectArrayElement(_arr, static_cast<jsize>(i));
    }

};

}; // namespace jni
}; // namespace qdb
