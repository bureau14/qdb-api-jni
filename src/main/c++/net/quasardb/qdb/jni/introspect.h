#pragma once

#include <jni.h>

#include "guard/local.h"

namespace qdb {
  namespace jni {

    class env;

    class introspect {
    public:

      static jmethodID
      lookup_method_id(jni::env & env, jclass objectClass, char const * alias, char const * signature);

    };
  };
};
