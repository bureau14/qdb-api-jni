#pragma once

#include <string>
#include <jni.h>

namespace qdb {
  namespace jni {

    void
    hexdump(JNIEnv * env, void const * buf, size_t len);

    void
    println(JNIEnv * env, std::string const & msg);

    void
    println(JNIEnv * env, char const * msg);


    /**
     * Wraps around JNI API to safely lookup a class.
     */
    jclass
    lookup_class(JNIEnv * env, char const * alias);

    /**
     * Wraps around JNI API to safely lookup an class' field id.
     */
    jfieldID
    lookup_fieldID(JNIEnv *env, jclass objectClass, char const * alias, char const * signature);

    /**
     * Wraps around JNI API to safely lookup an class' static field id.
     */
    jfieldID
    lookup_staticFieldID(JNIEnv *env, jclass objectClass, char const * alias, char const * signature);

    /**
     * Wraps around JNI API to safely lookup an class' method id.
     */
    jmethodID
    lookup_methodID(JNIEnv *env, jclass objectClass, char const * alias, char const * signature);

    /**
     * Wraps around JNI API to safely lookup an class' static method id.
     */
    jmethodID
    lookup_staticMethodID(JNIEnv *env, jclass objectClass, char const * alias, char const * signature);

  };
};
