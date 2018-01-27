#include "env.h"
#include "string.h"

/* static */ qdb::jni::guard::string_utf8
qdb::jni::string::get_chars_utf8(qdb::jni::env & env, jstring str) {
    assert(str != NULL);

    return std::move(
        qdb::jni::guard::string_utf8(env,
                                     str,
                                     env.instance().GetStringUTFChars(str, NULL)));
}

/* static */ qdb::jni::guard::local_ref<jstring>
qdb::jni::string::create_utf8(jni::env & env, char const * str)  {
    return std::move(
        qdb::jni::guard::local_ref<jstring>(env,
                                            env.instance().NewStringUTF(str)));
}
