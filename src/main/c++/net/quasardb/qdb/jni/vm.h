#pragma once

#include <utility>
#include <cassert>
#include <jni.h>

#include <boost/core/noncopyable.hpp>

namespace qdb {
  namespace jni {

    /**
     * Singleton wrapper around a JavaVM pointer.
     */
    class vm : private boost::noncopyable {
    private:
      static JavaVM * _vm;

    public:

      static JavaVM & instance();
      static JavaVM & instance(JavaVM *);

    };
  };
};
