#include "introspect.h"

/* static */ jmethodID
qdb::jni::introspect::lookup_method_id(qdb::jni::env & env, jclass objectClass, char const * alias, char const * signature) {
  assert(objectClass != NULL);

  jmethodID method = env.instance().GetStaticMethodID(objectClass, alias, signature);
  assert(method != NULL);

  return method;
}
