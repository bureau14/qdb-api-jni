#include <stdio.h>
#include "vm.h"

/* static */ JavaVM * qdb::jni::vm::_vm = NULL;


/* static */ JavaVM & qdb::jni::vm::instance(JNIEnv * env) {
  if (has_instance() == false) {
    JavaVM * vm = NULL;
    jint err = env->GetJavaVM(&vm);
    assert(err == 0);
    assert(vm != NULL);

    _vm = vm;
  }

  return instance();
}
