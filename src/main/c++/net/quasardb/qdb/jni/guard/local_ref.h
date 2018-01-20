#pragma once

#include <jni.h>

namespace qdb {
  namespace jni {
    namespace guard {

      class local_ref_deleter {
      public:
        void operator() (jobject localref) noexcept;
      };

      // template <typename RefType>
      // class local_ref : public std::unique_ptr<typename std::remove_pointer<RefType>::type,
      //                                          local_ref_deleter> {
      // private:
      //   JNIEnv * _env;

      // public:
      //   local_ref(JNIEnv * env, RefType localRef)
      //     : std::unique_ptr<typename std::remove_pointer<PointerType>::type, ::qdb::jni::guard::local_ref_deleter>(localRef) {}
      //   explicit LocalRef(PointerType localRef)
      //     : std::unique_ptr<typename std::remove_pointer<PointerType>::type, LocalRefDeleter>(
      //                                                                                         localRef) {}
      //   operator PointerType() const & { return this->get(); }
      //   operator PointerType() && = delete;

      };
    };
  };
};
