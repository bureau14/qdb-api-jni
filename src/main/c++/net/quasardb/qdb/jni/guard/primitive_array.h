#pragma once

#include "../env.h"
#include <jni.h>
#include <memory>
#include <span>
#include <stdio.h>

namespace qdb::jni::guard
{

/**
 * Helper class that wraps around a primite array and manages resources.
 */
template <typename T>
class primitive_array
{
private:
    qdb::jni::env & _env;
    jarray _arr;

    T * _ptr;
    qdb_size_t _n;

public:
    /**
     * Constructor. Assumes `ptr` is acquired through
     * env->GetPrimitiveArrayCritical, and will ensure the reference
     * is released when necessary.
     */
    constexpr primitive_array(
        qdb::jni::env & env, jarray const & arr, T * ptr, qdb_size_t n) noexcept
        : _env{env}
        , _arr{arr}
        , _ptr{ptr}
        , _n{n}
    {
        assert(_ptr != nullptr);
        assert(_arr != nullptr);
    }

    primitive_array(primitive_array const && o) = delete;

    ~primitive_array()
    {
        if (_ptr != nullptr && _arr != nullptr)
        {
            _env.instance().ReleasePrimitiveArrayCritical(_arr, _ptr, 0);
            _ptr = nullptr;
            _arr = nullptr;
        }
    }

    primitive_array(primitive_array const & o) = delete;
    primitive_array & operator=(primitive_array const & o) = delete;

    operator T *() noexcept
    {
        return get();
    }

    constexpr operator T *() const noexcept
    {
        return get();
    }

    T * get()
    {
        return _ptr;
    }

    constexpr T const * get() const noexcept
    {
        return _ptr;
    }

    T * release()
    {
        T * ptr = _ptr;
        _ptr    = nullptr;
        _arr    = nullptr;
        return ptr;
    }

    constexpr qdb_size_t size() const noexcept
    {
        return _n;
    }

    inline decltype(auto) to_range() const
    {
        return ranges::views::counted(_ptr, _n);
    };

    inline constexpr void copy(T * dst) const
    {
        T * cur = _ptr;
        T * end = _ptr + _n;

        // XXX(leon): this could (should) be replaced with a faster memcpy-like
        //            implementation, but for some reason it fails to reliably copy
        //            everything (??). memory areas should not overlap, so it should
        //            be safe.
        //
        //            maybe a hint at a deeper issue?
        while (cur != end)
        {
            *dst++ = *cur++;
        }
    }

    inline constexpr T * copy() const
    {
        T * ret = new T[_n];
        copy(ret);
        return ret;
    }
};

}; // namespace qdb::jni::guard
