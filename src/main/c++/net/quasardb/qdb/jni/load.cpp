#include <jni.h>

extern "C" jint JNICALL JNI_OnLoad(JavaVM* vm, void*) {
  // Initialization stuff could go here
  return JNI_VERSION_1_6;
}
