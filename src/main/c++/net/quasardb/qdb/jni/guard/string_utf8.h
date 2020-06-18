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
 * Helper class that wraps around a jstring's char *, so that they
 * can be released back to the JVM as soon as possible.
 */
class string_utf8
{
  private:
    qdb::jni::env &_env;
    jstring &_str;
    char const *_ptr;
    size_t _len;

  public:
    /**
     * Constructor. Assumes `ptr` is acquired through
     * env->GetStringUTFChars, and will ensure the reference
     * is released when necessary.
     */
    string_utf8(qdb::jni::env &env, jstring &str, char const *ptr, size_t len)
        : _env(env), _str(str), _ptr(ptr), _len(len)
    {

    }

    string_utf8(string_utf8 &&o) noexcept
        : _env(o._env), _str(o._str), _ptr(o._ptr), _len(o._len)
    {
        // By setting the other ptr to NULL, we're now effectively
        // claiming ownership of the char *.
        o._ptr = NULL;
        o._len = 0;
    }

    ~string_utf8()
    {
        if (_ptr != NULL)
        {
            _env.instance().ReleaseStringUTFChars(_str, _ptr);
        }
    }

    string_utf8(string_utf8 const &) = delete;
    string_utf8 &operator=(string_utf8 const &) = delete;

    /**
     * Provide automatic casting to char const *, so that it can be
     * used as if it were a regular char array.
     */
    operator char const *() const
    {
        return _ptr;
    }

    size_t size() const {
        return _len;
    }

    /**
     * Returns pointer to the underlying string and releases the ownership.
     */
    char const * release()
    {
        char const * ptr = _ptr;
        _ptr = NULL;
        return ptr;
    }
};
}; // namespace guard
}; // namespace jni
}; // namespace qdb
