#pragma once

#include <jni.h>
#include <memory>
#include <span>
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

    std::span<T> _span;
    T *_ptr;
    qdb_size_t _len;

  public:
  typedef typename std::span<T>::iterator iterator;
  typedef typename std::span<T>::reference reference;
  typedef typename std::span<T>::const_reference const_reference;
  typedef size_t size_type;

  public:
    /**
     * Constructor. Assumes `ptr` is acquired through
     * env->GetPrimitiveArrayCritical, and will ensure the reference
     * is released when necessary.
     */
    array_critical(qdb::jni::env &env, jarray & arr, T * ptr, qdb_size_t len)
      : _env(env), _arr(arr), _span{ptr, len}, _ptr(ptr), _len(len)
    {
    }

    array_critical(array_critical &&o) noexcept
      : _env(o._env), _arr(o._arr), _span{o._span}, _ptr(o._ptr), _len(o._len)
    {
        // By setting the other ptr to NULL, we're now effectively
        // claiming ownership of the char *.
        o._ptr = NULL;
        o._span = std::span<T>{};
    }

    ~array_critical()
    {
        if (_span.data() != nullptr)
        {
            _env.instance().ReleasePrimitiveArrayCritical(_arr,
                                                          _span.data(),
                                                          0);
        }
    }

    array_critical(array_critical const &) = delete;
    array_critical &operator=(array_critical const &) = delete;

    operator T * () const
    {
        return _ptr;
    }

    constexpr iterator begin() const noexcept {
        return _span.begin();
    }

    constexpr iterator end() const noexcept {
        return _span.end();
    }

    constexpr reference operator[](size_type idx) const {
        return _span[idx];
    }


    T * get() const {
      return _span.data();
    }

    qdb_size_t size() const {
      return _span.size();
    }

    T * copy() const {
      T * ret = new T[_len];

      for (qdb_size_t i = 0; i < _len; ++i) {
          ret[i] = _ptr[i];
      }

      return ret;
    }

};
}; // namespace guard
}; // namespace jni
}; // namespace qdb
