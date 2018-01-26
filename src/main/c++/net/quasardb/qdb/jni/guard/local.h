#pragma once

#include <memory>
#include <jni.h>

namespace qdb {
  namespace jni {
    namespace guard {

      struct local_deleter {
        void operator()(jobject ref) noexcept;
      };

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
      template <typename JNIType> class local : public std::unique_ptr <JNIType, local_deleter> {
      public:
        local() {}
        local(JNIType ref) : std::unique_ptr<typename std::remove_pointer<JNIType>::type,
                                             local_deleter> (ref) {}

        operator JNIType() const & {
          return this->get();
        }

        operator JNIType() const && = delete;
      };

      template<class JNIType>
      const JNIType & get(const JNIType& x) noexcept {
        return x;
      }

      template<class JNIType>
      typename local<JNIType>::pointer get(const local<JNIType>& x) noexcept {
        return x.get();
      }

      template<class JNIType>
      const JNIType& release(const JNIType& x) noexcept {
        return x;
      }

      template<class JNIType>
      typename local<JNIType>::pointer release(local<JNIType>& x) noexcept {
        return x.release();
      }

      template<class JNIType>
      typename local<JNIType>::pointer release(local<JNIType>&& x) noexcept {
        return x.release();
      }
    };
  };
};
