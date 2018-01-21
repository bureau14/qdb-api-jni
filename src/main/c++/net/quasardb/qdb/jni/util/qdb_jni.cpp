#include <string.h>
#include <cassert>

#include "qdb_jni.h"

void
qdb::jni::hexdump(JNIEnv * env, void const * buf_, size_t len) {
  char const * buf = (char const *)(buf);
  char const * const lut = "0123456789ABCDEF";

  std::string output;
  output.reserve(3 * len);
  output.append("* NATIVE * ");

  for (size_t i = 0; i < len; ++i){
    const unsigned char c = buf[i];
    output.push_back(lut[c >> 4]);
    output.push_back(lut[c & 15]);
    output.push_back(' ');
  }

  println(env, output);
}

void
qdb::jni::println(JNIEnv * env, std::string const & msg) {
  println(env, msg.c_str());
}

void
qdb::jni::println(JNIEnv * env, char const * msg) {
  jclass syscls = lookup_class(env, "java/lang/System");

  // Lookup the "out" field
  jfieldID fid = lookup_staticFieldID(env, syscls, "out", "Ljava/io/PrintStream;");
  jobject out = env->GetStaticObjectField(syscls, fid);

  // Get PrintStream class
  jclass pscls = lookup_class(env, "java/io/PrintStream");

  // Lookup printLn(String)
  jmethodID mid = lookup_methodID(env, pscls, "println", "(Ljava/lang/String;)V");

  // Invoke the method
  jstring str = env->NewStringUTF(msg);
  env->CallVoidMethod(out, mid, str);
  env->DeleteLocalRef(str);
}

jclass
qdb::jni::lookup_class(JNIEnv * env, char const * alias) {
  jclass c = env->FindClass(alias);
  assert(c != NULL);
  return c;
}

jfieldID
qdb::jni::lookup_staticFieldID(JNIEnv *env, jclass objectClass, char const * alias, char const * signature) {
  assert(env != NULL);
  assert(objectClass != NULL);
  jfieldID field = env->GetStaticFieldID(objectClass, alias, signature);
  assert(field != NULL);
  return field;
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
