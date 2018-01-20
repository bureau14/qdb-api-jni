#include <cassert>

#include "qdb_jni.h"


jclass
qdb::jni::lookup_class(JNIEnv * env, char const * alias) {
  jclass c = env->FindClass(alias);
  assert(c != NULL);
  return c;
}

jfieldID
qdb::jni::lookup_fieldID(JNIEnv *env, jclass objectClass, char const * alias, char const * signature) {
  assert(env != NULL);
  assert(objectClass != NULL);
  jfieldID field = env->GetFieldID(objectClass, alias, signature);
  assert(field != NULL);
  return field;
}

jmethodID
qdb::jni::lookup_methodID(JNIEnv *env, jclass objectClass, char const * alias, char const * signature) {
  assert(env != NULL);
  assert(objectClass != NULL);
  jmethodID method = env->GetMethodID(objectClass, alias, signature);
  assert(method != NULL);
  return method;
}


jmethodID
qdb::jni::lookup_staticMethodID(JNIEnv *env, jclass objectClass, char const * alias, char const * signature) {
  assert(env != NULL);
  assert(objectClass != NULL);
  jmethodID method = env->GetStaticMethodID(objectClass, alias, signature);
  assert(method != NULL);
  return method;
}
