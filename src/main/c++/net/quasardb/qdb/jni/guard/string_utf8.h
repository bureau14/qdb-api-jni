#pragma once

#include "../allocate.h"
#include "../env.h"
#include <qdb/ts.h>
#include <cstring>
#include <jni.h>
#include <memory>
#include <stdio.h>
#include <type_traits>

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
    qdb::jni::env & _env;
    qdb_handle_t _handle;
    jstring & _str;
    char const * _ptr;
    size_t _len;

public:
    /**
     * Constructor. Assumes `ptr` is acquired through
     * env->GetStringUTFChars, and will ensure the reference
     * is released when necessary.
     */
    string_utf8(
        qdb::jni::env & env, qdb_handle_t handle, jstring & str, char const * ptr, size_t len)
        : _env(env)
        , _handle(handle)
        , _str(str)
        , _ptr(ptr)
        , _len(len)
    {}

    ~string_utf8()
    {
        if (_ptr != nullptr) [[likely]]
        {
            _env.instance().ReleaseStringUTFChars(_str, _ptr);
            _ptr = nullptr;
        }
    }

    string_utf8(string_utf8 && o)                = delete;
    string_utf8(string_utf8 const &)             = delete;
    string_utf8 & operator=(string_utf8 const &) = delete;
    string_utf8 & operator=(string_utf8 &&)      = delete;

    /**
     * Provide automatic casting to char const *, so that it can be
     * used as if it were a regular char array.
     */
    operator char const *() const
    {
        return _ptr;
    }

    /**
     * Provide automatic casting to jstring, so that it can be used
     * as a regular jvm object.
     */
    constexpr operator jstring() const
    {
        return _str;
    }

    /**
     * Provide automatic casting to string const &, so that it can be
     * used as if it were a regular string.
     */
    operator std::string() const
    {
        return std::string{_ptr, _len};
    }

    char const * get() const
    {
        return _ptr;
    }

    size_t size() const
    {
        return _len;
    }

    /**
     * Returns pointer to the underlying string and releases the ownership.
     */
    char const * release()
    {
        char const * ptr = _ptr;
        _ptr             = nullptr;
        return ptr;
    }

    /**
     */
    char * copy(qdb_handle_t handle) const
    {
        char * ret = jni::allocate<char>(handle, _len + 1);
        memcpy(ret, _ptr, _len);
        ret[_len] = '\0';
        return ret;
    }

    inline qdb_string_t as_qdb(qdb_handle_t handle) const
    {
        qdb_string_t ret;
        as_qdb(handle, ret);
        return ret;
    }

    inline void as_qdb(qdb_handle_t handle, qdb_ts_string_point & out) const
    {
        out.content        = copy(handle);
        out.content_length = _len;
    }

    inline void as_qdb(qdb_handle_t handle, qdb_string_t & out) const
    {
        out.data   = copy(handle);
        out.length = _len;
    }
};
}; // namespace guard
}; // namespace jni
}; // namespace qdb
