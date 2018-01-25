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

      static JavaVM & instance();
      static JavaVM & instance(JavaVM *);

    };
  };
};
