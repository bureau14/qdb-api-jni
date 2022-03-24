#pragma once

#include "log.h"
#include <cassert>
#include <jni.h>
#include <utility>

namespace qdb::jni
{

class vm;

/**
 * Provides safe access to the JNIEnv environment.
 *
 * A JNIEnv can be acquired in two ways: the most obvious way that the
 * JNIEnv is provided when the JVM invokes a native function. However, a
 * JNIEnv can also be resolved from the JavaVM, provided that the active
 * thread is attached to the JVM.
 *
 * Where a JNIEnv cannot be shared between threads, a JavaVM can be. This
 * class wraps around the necessary boilerplate to make this completely
 * transparent, at the cost of a small performance penalty during object
 * construction.
 *
 * If available, one should always initialise a qdb::jni::env from an
 * existing JNIEnv.
 */
class env
{
private:
    JNIEnv * _env;
    bool _flush_disabled;

public:
    /**
     * Initialise an env from a JNIEnv *. This is the most commonly used
     * method of initialisation, and will ensure qdb::jni::vm is initialised.
     */
    env(JNIEnv * e)
        : _env(e)
        , _flush_disabled(false)
    {}

    env(env const & e)  = delete;
    env(env const && e) = delete;

    /**
     * Initialise an env from a JavaVM &. This can be used in cases where
     * a JNIEnv * is not available, and this constructor will acquire a
     * JNIEnv * for the current thread from the JavaVM.
     *
     * \warning Requires the current thread to be attached to the JVM.
     */
    env(JavaVM & vm);

    env & operator=(env const & e) = delete;
    env & operator=(env const && e) = delete;

    /**
     * Initialise an env from a global JavaVM.
     */

    JNIEnv & instance()
    {
        assert(_env != NULL);
        return *_env;
    }

    ~env()
    {
        if (false == _flush_disabled)
        {
            log::flush(*this);
        }
    }

    /**
     * Disables flushing of logs in destructor.
     */
    void disable_flush()
    {
        _flush_disabled = true;
    }
};

}; // namespace qdb::jni
