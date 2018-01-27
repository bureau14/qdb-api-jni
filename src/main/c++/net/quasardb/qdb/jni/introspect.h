#pragma once

#include <jni.h>

namespace qdb {
  namespace jni {

    class env;

    class introspect {
    public:

      /**
       * Wraps around JNI API to safely lookup a class.
       */
      static jclass
      lookup_class(env & env, char const * alias);

      /**
       * Wraps around JNI API to safely lookup an class' field id.
       */
      static jfieldID
      lookup_field(env & env, jclass objectClass, char const * alias, char const * signature);

      /**
       * Wraps around JNI API to safely lookup an class' static field id.
       */
      static jfieldID
      lookup_static_field(env & env, jclass objectClass, char const * alias, char const * signature);

      /**
       * Wraps around JNI API to safely lookup an class' method id.
       */
      static jmethodID
      lookup_method(env & env, jclass objectClass, char const * alias, char const * signature);

      /**
       * Wraps around JNI API to safely lookup an class' static method id.
       */
      static jmethodID
      lookup_static_method(env & env, jclass objectClass, char const * alias, char const * signature);


    };
  };
};
