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


void
timespecToNative(JNIEnv *env, jobject input, qdb_timespec_t * output) {
  // qdb_timespec -> tv_sec, tv_nsec
  jfieldID sec_field, nsec_field;
  jclass object_class;

  object_class = env->GetObjectClass(input);

  sec_field = env->GetFieldID(object_class, "tv_sec", "J");
  nsec_field = env->GetFieldID(object_class, "tv_nsec", "J");
  output->tv_sec = env->GetLongField(input, sec_field);
  output->tv_nsec = env->GetLongField(input, nsec_field);

}

void
nativeToTimespec(JNIEnv *env, qdb_timespec_t input, jobject * output) {
  jclass timespec_class = env->FindClass("net/quasardb/qdb/jni/qdb_timespec");
  jmethodID constructor = env->GetMethodID(timespec_class, "<init>", "(JJ)V");

  *output = env->NewObject(timespec_class,
                           constructor,
                           input.tv_sec,
                           input.tv_nsec);

}
