#include "env.h"
#include "introspect.h"

/* static */ jclass
qdb::jni::introspect::lookup_class(env & env, char const * alias) {
  jclass c = env.instance().FindClass(alias);
  assert(c != NULL);
  return c;
}

/* static */ jfieldID
qdb::jni::introspect::lookup_field(env & env, jclass objectClass, char const * alias, char const * signature) {
  assert(objectClass != NULL);
  jfieldID field = env.instance().GetFieldID(objectClass, alias, signature);
  assert(field != NULL);
  return field;
}

/* static */ jfieldID
qdb::jni::introspect::lookup_static_field(env & env, jclass objectClass, char const * alias, char const * signature) {
  assert(objectClass != NULL);
  jfieldID field = env.instance().GetStaticFieldID(objectClass, alias, signature);
  assert(field != NULL);
  return field;
}

/* static */ jmethodID
qdb::jni::introspect::lookup_method(env & env, jclass objectClass, char const * alias, char const * signature) {
  assert(objectClass != NULL);
  jmethodID method = env.instance().GetMethodID(objectClass, alias, signature);
  assert(method != NULL);
  return method;
}

/* static */ jmethodID
qdb::jni::introspect::lookup_static_method(env & env, jclass objectClass, char const * alias, char const * signature) {
  assert(objectClass != NULL);
  jmethodID method = env.instance().GetStaticMethodID(objectClass, alias, signature);
  assert(method != NULL);
  return method;
}
