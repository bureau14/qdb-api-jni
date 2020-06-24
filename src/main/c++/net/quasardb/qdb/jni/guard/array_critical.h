#pragma once

#include <jni.h>
#include <memory>
#include <stdio.h>

#include "../env.h"

namespace qdb
{
namespace jni
{
namespace guard
{

/**
 * Helper class that wraps around a primite array and manages resources.
 */
template <typename T>
class array_critical
{
  private:
    qdb::jni::env &_env;
    jarray &_arr;
    T *_ptr;
    qdb_size_t _len;

  public:
    /**
     * Constructor. Assumes `ptr` is acquired through
     * env->GetPrimitiveArrayCritical, and will ensure the reference
     * is released when necessary.
     */
    array_critical(qdb::jni::env &env, jarray & arr, T * ptr, qdb_size_t len)
    : _env(env), _arr(arr), _ptr(ptr), _len(len)
    {
    }

    array_critical(array_critical &&o) noexcept
        : _env(o._env), _arr(o._arr), _ptr(o._ptr), _len(o._len)
    {
        // By setting the other ptr to NULL, we're now effectively
        // claiming ownership of the char *.
        o._ptr = NULL;
    }

    ~array_critical()
    {
        if (_ptr != NULL)
        {
            _env.instance().ReleasePrimitiveArrayCritical(_arr,
                                                          _ptr,
                                                          0);
        }
    }

    array_critical(array_critical const &) = delete;
    array_critical &operator=(array_critical const &) = delete;

    operator T * () const
    {
        return _ptr;
    }

    T * get() const {
      return _ptr;
    }

    qdb_size_t size() const {
      return _len;
    }

};
}; // namespace guard
}; // namespace jni
}; // namespace qdb
