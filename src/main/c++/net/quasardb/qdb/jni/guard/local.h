#pragma once

#include <memory>
#include <jni.h>

#include "../env.h"

namespace qdb {
  namespace jni {
    namespace guard {

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
      template <typename JNIType> class local {
      private:
        qdb::jni::env & _env;
        JNIType _ref;

      public:
        explicit local(jni::env & env, JNIType ref) : _env(env), _ref(ref) {}

        ~local() {
          if (_ref != NULL) {
            _env.instance().DeleteLocalRef(_ref);
          }
        }

        operator JNIType() const & {
          return _ref;
        }

        operator JNIType() const && = delete;

        JNIType const & get() {
          return _ref;
        }
      };
    };
  };
};
