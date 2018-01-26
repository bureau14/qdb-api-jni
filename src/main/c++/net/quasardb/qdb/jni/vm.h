#pragma once

#include <utility>
#include <cassert>
#include <jni.h>

namespace qdb {
  namespace jni {

    /**
     * Singleton wrapper around a JavaVM pointer.
     */
    class vm {
    private:
      static JavaVM * _vm;

    public:

      static bool has_instance() {
        return _vm != NULL;
      }

      static JavaVM & instance() {
        assert(_vm != NULL);
        return *_vm;
      }

      static JavaVM & instance(JavaVM * vm) {
        _vm = vm;
        return instance();
      }

      static JavaVM & instance(JavaVM & vm) {
        return instance(&vm);
      }

      static JavaVM & instance(JNIEnv * env);

      static JavaVM & instance(JNIEnv & env) {
        return instance(&env);
      }
    };
  };
};
