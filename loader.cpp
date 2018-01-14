#include <stdio.h>

#include "net_quasardb_qdb_jni_qdb.h"

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv * env;
  if (vm->GetEnv((void **) &env, JNI_VERSION_1_6) != JNI_OK) {
        return JNI_ERR;
  } else {

  }

  return JNI_VERSION_1_6;
}
