#pragma once

#include <utility>
#include <cassert>
#include <jni.h>

#include "vm.h"

namespace qdb {
  namespace jni {

    /**
     * Provides safe access to the JNIEnv environment.
     */
    class env {
    private:
      static JavaVM * _vm;

      JNIEnv * _env;

    public:
      env(JNIEnv * e) :
        _env(e) {
        _maybe_init(e);
      };

      env(JavaVM * vm) {
        _maybe_init(vm);

        void * e = NULL;

        // If the current thread is not attached to the VM (e.g. background, native
        // thread) this will return JNI_EDETACHED. In this case, the user should first
        // attach their native to the global VM, and only then try to resolve an
        // env.
        jint err = vm->GetEnv(&e, JNI_VERSION_1_6);
        assert(err == JNI_OK);
        assert(e != NULL);

        env((JNIEnv *)(e));
      }

      env() {
        assert(_vm != NULL);
        env(_vm);
      }

      JNIEnv & instance() {
        assert(_env != NULL);
        return *_env;
      }

    protected:
      env (env const &) = delete;
      env (env const &&) = delete;
      env & operator=(env const &) = delete;
      env & operator=(env const &&) = delete;

    private:
      static void
      _maybe_init(JNIEnv * e) {
        if (_vm == NULL) {
          JavaVM * vm = NULL;
          jint err = e->GetJavaVM(&vm);
          assert(err == 0);
          assert(vm != NULL);

          _init(vm);
        }

        assert(_vm != NULL);
      }

      static void
      _maybe_init(JavaVM * vm) {
        if (_vm == NULL) {
          _init(vm);
        }

        assert(_vm != NULL);
      }

      static void
      _init(JavaVM * vm) {
        // :TODO: this is actually a race condition when two threads race for
        //        initializing the _vm. This should eventually probably be fixed
        //        using the double checked locking optimisation thingie.

        assert(_vm == NULL);
        _vm = vm;
      }

    };
  };
};
