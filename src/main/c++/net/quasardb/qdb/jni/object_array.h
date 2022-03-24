#pragma once

#include "env.h"
#include "introspect.h"
#include "guard/local_ref.h"
#include <range/v3/all.hpp>
#include <functional>
#include <iostream>
#include <iterator>
#include <jni.h>
#include <limits>
#include <stdio.h>

namespace qdb::jni
{

class object_array : public ranges::view_interface<qdb::jni::object_array>
{
public:
    class iterator
    {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using difference_type   = ranges::iter_difference_t<jobject>;

        using value_type = jobject;
        using reference  = value_type &;
        using pointer    = value_type *;

    public:
        constexpr iterator() = default;

        constexpr iterator(qdb::jni::env * env, jobjectArray xs, difference_type offset)
            : _env{env}
            , _xs{xs}
            , _offset{offset}
        {}

        constexpr iterator(qdb::jni::env * env, jobjectArray xs)
            : iterator{env, xs, 0}
        {}

    public:
        iterator & operator++()
        {
            _offset += 1;
            return *this;
        }

        iterator & operator--()
        {
            _offset -= 1;
            return *this;
        }

        iterator constexpr operator+(difference_type n) const
        {
            return iterator{_env, _xs, _offset + n};
        }

        iterator operator++(int)
        {
            iterator ret = *this;
            _offset += 1;

            return ret;
        }

        iterator operator--(int)
        {
            iterator ret = *this;
            _offset -= 1;

            return ret;
        }

        reference operator*() const
        {
            return _set_and_get_cur();
        }

        reference operator*()
        {
            return _set_and_get_cur();
        }

        pointer operator->()
        {
            reference cur = _set_and_get_cur();
            return &cur;
        }

        pointer operator->() const
        {
            reference cur = _set_and_get_cur();
            return &cur;
        }

        friend bool operator==(iterator const & lhs, iterator const & rhs)
        {
            return lhs._offset == rhs._offset;
        }

        friend bool operator!=(iterator const & lhs, iterator const & rhs)
        {
            return !(lhs == rhs);
        }

        friend bool operator<(iterator const & lhs, iterator const & rhs)
        {
            return lhs._offset < rhs._offset;
        }

        friend bool operator<=(iterator const & lhs, iterator const & rhs)
        {
            return lhs._offset <= rhs._offset;
        }

        friend bool operator>(iterator const & lhs, iterator const & rhs)
        {
            return lhs._offset > rhs._offset;
        }

        friend bool operator>=(iterator const & lhs, iterator const & rhs)
        {
            return lhs._offset >= rhs._offset;
        }

        friend difference_type operator-(iterator const & lhs, iterator const & rhs)
        {
            // If/else to avoid potential overflow; we could also use std::abs, but
            // if difference_type is an unsigned int, it could very easily overflow.
            return lhs._offset < rhs._offset ? rhs._offset - lhs._offset
                                             : lhs._offset - rhs._offset;
        }

    private:
        reference _set_and_get_cur() const
        {
            //! HACKS, but we want to 'cache' the currently set element inside
            //! this iterator,
            //         so that we can return pointers / references to these
            //         objects.
            //
            //         Unfortunately, JNI doesn't provide an easy mechanism for
            //         this, as you are supposed to pass `jobjects` (which are
            //         already pointers under the hood) by value, violating
            //         certain iterator requirements.
            iterator * this_ = const_cast<iterator *>(this);
            return this_->_set_and_get_cur();
        }

        reference _set_and_get_cur()
        {
            assert(_offset < std::numeric_limits<jsize>::max());
            _cur = _env->instance().GetObjectArrayElement(_xs, static_cast<jsize>(_offset));
            return _cur;
        }

    private:
        qdb::jni::env * _env;
        jobjectArray _xs;
        difference_type _offset;
        jobject _cur;
    };

    using const_iterator = iterator;
    using value_type     = iterator::value_type;
    using size_type      = std::size_t;

    object_array(qdb::jni::env & env, jobjectArray xs, iterator::difference_type n)
        : _env{env}
        , _xs{xs}
        , _n{n}
        , _begin{iterator{&env, xs, 0}}
        , _end{iterator{&env, xs, _n}}
    {}

    object_array(qdb::jni::env & env, jobjectArray xs)
        : object_array{env, xs, env.instance().GetArrayLength(xs)}
    {}

    constexpr auto begin() const
    {
        return _begin;
    }

    constexpr auto cbegin() const
    {
        return _begin;
    }

    constexpr auto end() const
    {
        return _end;
    }

    constexpr auto cend() const
    {
        return _end;
    }

    value_type get(iterator::difference_type i) const
    {
        assert(i < std::numeric_limits<jsize>::max());
        return _env.instance().GetObjectArrayElement(_xs, static_cast<jsize>(i));
    }

    void set(iterator::difference_type i, value_type const & x)
    {
        assert(i < std::numeric_limits<jsize>::max());
        _env.instance().SetObjectArrayElement(_xs, static_cast<jsize>(i), x);
    }

    constexpr size_type size() const
    {
        return std::distance(_begin, _end);
    }

    constexpr inline jobjectArray release() const
    {
        return _xs;
    }

private:
    qdb::jni::env & _env;
    jobjectArray _xs;
    iterator::difference_type _n;

    iterator _begin;
    iterator _end;
};

/**
 * Construct empty array of class `objectClass` with length `len`.
 */
inline object_array make_object_array(qdb::jni::env & env, jsize len, jclass objectClass)
{
    jobjectArray xs = env.instance().NewObjectArray(len, objectClass, NULL);
    return object_array{env, xs};
}

/**
 * Construct empty array of class `className` with length `len`. Automatically
 * resolves class.
 */
inline object_array make_object_array(qdb::jni::env & env, std::size_t len, char const * className)
{
    return make_object_array(env, len, introspect::lookup_class(env, className));
}

/**
 * Construct array of class `className` with the contents of range [begin, end).
 * Automatically resolves class.
 */
template <class R>
requires ranges::input_range<R>
inline object_array make_object_array(qdb::jni::env & env, jclass objectClass, R xs)
{
    object_array ret = make_object_array(env, ranges::size(xs), objectClass);

    std::size_t i = 0;
    for (jobject x : xs)
    {
        ret.set(i++, x);
    }

    return ret;
}

/**
 * Construct array of class `className` with the contents of range [begin, end).
 * Automatically resolves class.
 */
template <class R>
requires(ranges::input_range<R>) inline object_array
    make_object_array(qdb::jni::env & env, char const * className, R xs)
{
    return make_object_array(env, introspect::lookup_class(env, className), xs);
}

}; // namespace qdb::jni

// Necessary: our range actually models data that lives *outside* of the range,
// i.e. the JNI object array's lifetime may (will!) outlive the object_array
// lifetime.
template <>
inline constexpr bool ranges::enable_borrowed_range<qdb::jni::object_array> = true;

// Iterator-specific assertions, _should_ be included in the input/output range
// checks below as well, but provides some early/more clear hints in case
// something goes wrong.
static_assert(ranges::input_iterator<qdb::jni::object_array::iterator>);

static_assert(ranges::input_range<qdb::jni::object_array>);

// Ensures typeof(begin) == typeof(end)
static_assert(ranges::common_range<qdb::jni::object_array>);

// Ensures that we can (safely) convert our object range into any other view,
// without the risk of dangling pointers.
static_assert(ranges::viewable_range<qdb::jni::object_array>);

// Ensure std::difference(begin, end) is a constant time operation
static_assert(ranges::sized_range<qdb::jni::object_array>);

static_assert(ranges::bidirectional_range<qdb::jni::object_array>);

namespace std
{

using qdb::jni::object_array;

inline typename object_array::iterator begin(object_array & xs)
{
    return xs.begin();
}

inline typename object_array::iterator end(object_array & xs)
{
    return xs.end();
}

inline typename object_array::const_iterator cbegin(object_array const & xs)
{
    return xs.cbegin();
}

inline typename object_array::const_iterator cend(object_array const & xs)
{
    return xs.cend();
}

} // namespace std
