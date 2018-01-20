#pragma once

#include <jni.h>

namespace qdb {
  namespace jni {

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
