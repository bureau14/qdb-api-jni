#include "env.h"
#include "introspect.h"
#include "export/qdb_ts.h"
#include <jni.h>

namespace jni = qdb::jni;

extern "C" jint JNICALL JNI_OnLoad(JavaVM * vm, void * /* reserved */)
{
    JNIEnv * env;
    if (vm->GetEnv((void **)&env, JNI_VERSION_1_6) != JNI_OK) return JNI_ERR;

    jclass c = env->FindClass("net/quasardb/qdb/jni/qdb");
    if (!c) return JNI_ERR;

    // jint ret = jni::ts::register_natives(env, c);
    jint ret = 0;

    env->DeleteLocalRef(c);
    if (ret != 0)
    {
        printf("unable to register natives: %d", ret);
        return JNI_ERR;
    }

    // Initialization stuff could go here
    return JNI_VERSION_1_6;
}
