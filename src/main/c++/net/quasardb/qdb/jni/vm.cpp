#include <stdio.h>
#include "vm.h"


qdb::jni::vm::vm(JNIEnv * env) {
  JavaVM * vm = NULL;
  jint err = env->GetJavaVM(&vm);
  assert(err == 0);
  assert(vm != NULL);

  _vm = vm;
}
