#pragma once

#include <string>
#include <jni.h>

namespace qdb {
  namespace jni {

    class env;

    void
    hexdump(env & env, void const * buf, size_t len);

    void
    println(env & env, std::string const & msg);

    void
    println(env & env, char const * msg);


    /**
     * Wraps around JNI API to safely lookup a class.
     */
    jclass
    lookup_class(env & env, char const * alias);

    /**
     * Wraps around JNI API to safely lookup an class' field id.
     */
    jfieldID
    lookup_fieldID(env & env, jclass objectClass, char const * alias, char const * signature);

    /**
     * Wraps around JNI API to safely lookup an class' static field id.
     */
    jfieldID
    lookup_staticFieldID(env & env, jclass objectClass, char const * alias, char const * signature);

    /**
     * Wraps around JNI API to safely lookup an class' method id.
     */
    jmethodID
    lookup_methodID(env & env, jclass objectClass, char const * alias, char const * signature);

    /**
     * Wraps around JNI API to safely lookup an class' static method id.
     */
    jmethodID
    lookup_staticMethodID(env & env, jclass objectClass, char const * alias, char const * signature);

  };
};
