#include "../util/qdb_jni.h"
#include "qdb_value.h"

jobject
qdb::value::from_native(JNIEnv * env, qdb_point_result_t const & input) {
  jclass valueClass = qdb::jni::lookup_class(env, "net/quasardb/qdb/ts/Value");
  jmethodID constructor = qdb::jni::lookup_staticMethodID(env, valueClass,
                                                          "createNull",
                                                          "()Lnet/quasardb/qdb/ts/Value;");

  return env->CallStaticObjectMethod(valueClass, constructor);
}
