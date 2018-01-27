#include "env.h"
#include "string.h"

/* static */ qdb::jni::guard::string
qdb::jni::string::get_chars(qdb::jni::env & env, jstring str) {
    return std::move(
        qdb::jni::guard::string(env,
                                str,
                                env.instance().GetStringUTFChars(str, NULL)));
}
