#pragma once

#include "../env.h"
#include <assert.h>
#include <jni.h>
#include <memory>
#include <stdio.h>

namespace qdb
{
namespace jni
{
namespace guard
{

/**
 * Helper guard responsible for releasing locally referenced memory
 * back to the JVM as soon as the object goes out of scope.
 *
 * Any references acquired in JNI code to objects that live on the
 * JVM are called "local references". Before invoking any native function,
 * the JVM reserves room for about 12 of these references on the stack,
 * after that the GC actually kicks in and all kinds of bad things happen.
 *
 * As such, we need a mechanism to release these references back to
 * JVM as soon as possible, which is what this class takes care of.
 */
template <typename JNIType>
class local_ref
{
private:
    qdb::jni::env & _env;
    JNIType _ref;

public:
    explicit local_ref(jni::env & env)
        : _env{env}
        , _ref{nullptr}
    {}
    explicit local_ref(jni::env & env, JNIType ref)
        : _env{env}
        , _ref{ref}
    {}

    local_ref(local_ref && o) noexcept
        : _env(o._env)
        , _ref(o._ref)
    {
        // By setting the other _ref to nullptr, we're now effectively
        // claiming ownership of the resource.
        o._ref = nullptr;
    }

    local_ref & operator=(local_ref && o)
    {
        _ref = o._ref;

        // By setting the other _ref to nullptr, we're now effectively
        // claiming ownership of the resource.
        o._ref = nullptr;

        return *this;
    }

    ~local_ref()
    {
        if (_ref != nullptr)
        {
            _env.instance().DeleteLocalRef(_ref);
        }
    }

    local_ref(local_ref const &) = delete;
    local_ref & operator=(local_ref const &) = delete;

    operator JNIType() const &
    {
        return _ref;
    }

    operator JNIType() const &&
    {
        return _ref;
    }

    JNIType & get()
    {
        return _ref;
    }

    JNIType release()
    {
        JNIType ref = _ref;
        _ref        = nullptr;
        return ref;
    }

    /**
     * Cast the underlying type, and return new reference. The old local_ref is
     * nulled and should be discarded.
     */
    template <typename T>
    inline local_ref<T> cast()
    {
        return local_ref<T>{_env, static_cast<T>(release())};
    }
};
}; // namespace guard
}; // namespace jni
}; // namespace qdb
