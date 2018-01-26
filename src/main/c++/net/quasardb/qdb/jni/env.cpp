#include "vm.h"
#include "env.h"

qdb::jni::env::env(JNIEnv * e) :
  _env(e) {
  // We don't use the return value, but this ensures that our global singleton
  // for the JavaVM is set.
  qdb::jni::vm::instance(*e);
};

qdb::jni::env::env(JavaVM & vm) {
  // We don't use the return value, but this ensures that our global singleton
  // for the JavaVM is set.
  qdb::jni::vm::instance(vm);

  void * e = NULL;

  // If the current thread is not attached to the VM (e.g. background, native
  // thread) this will return JNI_EDETACHED. In this case, the user should first
  // attach their native to the global VM, and only then try to resolve an
  // env.
  jint err = vm.GetEnv(&e, JNI_VERSION_1_6);
  assert(err == JNI_OK);
  assert(e != NULL);

  env((JNIEnv *)(e));
}

qdb::jni::env::env() : env(qdb::jni::vm::instance()) {}
