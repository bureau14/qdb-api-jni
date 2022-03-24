#include "env.h"
#include "vm.h"

qdb::jni::env::env(JavaVM & vm)
{
    void * e = NULL;

    // If the current thread is not attached to the VM (e.g. background, native
    // thread) this will return JNI_EDETACHED. In this case, the user should
    // first attach their native to the global VM, and only then try to resolve
    // an env.
    [[maybe_unused]] jint err = vm.GetEnv(&e, JNI_VERSION_1_6);
    assert(err == JNI_OK);
    assert(e != NULL);

    env((JNIEnv *)(e));
}
