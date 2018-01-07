#include "helpers.h"

void
setReferenceValue(JNIEnv *env, jobject reference, jobject value) {
  static jfieldID fid = 0;
  if (!fid) {
    jclass thisClass = env->GetObjectClass(reference);
    fid = env->GetFieldID(thisClass, "value", "Ljava/lang/Object;");
  }
  env->SetObjectField(reference, fid, value);
}

jobject
getReferenceValue(JNIEnv *env, jobject reference) {
  static jfieldID fid = 0;
  if (!fid) {
    jclass thisClass = env->GetObjectClass(reference);
    fid = env->GetFieldID(thisClass, "value", "Ljava/lang/Object;");
  }
  env->GetObjectField(reference, fid);
}

void
setByteBuffer(JNIEnv *env, jobject reference, const void *ptr, jlong size) {
  setReferenceValue(env, reference, ptr ? env->NewDirectByteBuffer((void *)ptr, size) : NULL);
}

void
setInteger(JNIEnv *env, jobject reference, jint value) {
  jclass integerClass = env->FindClass("java/lang/Integer");
  static jmethodID constructorId = 0;
  if (!constructorId) {
    constructorId = env->GetMethodID(integerClass, "<init>", "(I)V");
  }
  setReferenceValue(env, reference, env->NewObject(integerClass, constructorId, value));
}

void
setLong(JNIEnv *env, jobject reference, jlong value) {
  jclass longClass = env->FindClass("java/lang/Long");
  static jmethodID constructorId = 0;
  if (!constructorId) {
    constructorId = env->GetMethodID(longClass, "<init>", "(J)V");
  }
  setReferenceValue(env, reference, env->NewObject(longClass, constructorId, value));
}

void
setStringArray(JNIEnv *env, jobject reference, const char **strings, size_t count) {
  jclass stringClass = env->FindClass("java/lang/String");
  jobjectArray array = env->NewObjectArray((jsize)count, stringClass, NULL);
  for (size_t i = 0; i < count; i++) {
    env->SetObjectArrayElement(array, (jsize)i, env->NewStringUTF(strings[i]));
  }
  setReferenceValue(env, reference, array);
}

void
setString(JNIEnv *env, jobject reference, const char *value) {
  setReferenceValue(env, reference, env->NewStringUTF(value));
}
