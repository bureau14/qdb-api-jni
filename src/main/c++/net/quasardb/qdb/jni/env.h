#pragma once

#include <utility>
#include <cassert>
#include <jni.h>

namespace qdb {
  namespace jni {

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
    class env {
    private:
      JNIEnv * _env;

    public:
      /**
       * Initialise an env from a JNIEnv *. This is the most commonly used
       * method of initialisation, and will ensure qdb::jni::vm is initialised.
       */
      env(JNIEnv * e);

      /**
       * Initialise an env from a JavaVM &. This can be used in cases where
       * a JNIEnv * is not available, and this constructor will acquire a
       * JNIEnv * for the current thread from the JavaVM.
       *
       * \warning Requires the current thread to be attached to the JVM.
       */
      env(JavaVM & vm);

      /**
       * Initialise an env from the global JavaVM singleton. For this method to
       * work, this requires an earlier invocation of this class using either
       * a env(JavaVM &) constructor or env(JNIEnv *) constructor so that the
       * JavaVM singleton is properly initialised.
       */
      env();

      JNIEnv & instance() {
        assert(_env != NULL);
        return *_env;
      }

    protected:
      env (env const &) = delete;
      env (env const &&) = delete;
      env & operator=(env const &) = delete;
      env & operator=(env const &&) = delete;

    };
  };
};
