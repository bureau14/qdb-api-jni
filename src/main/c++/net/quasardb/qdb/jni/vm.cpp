#include "vm.h"
#include <stdio.h>

qdb::jni::vm::vm(JNIEnv * env)
{
    JavaVM * vm               = NULL;
    [[maybe_unused]] jint err = env->GetJavaVM(&vm);
    assert(err == 0);
    assert(vm != NULL);

    _vm = vm;
}
