#include "env.h"
#include "string.h"

/* static */ qdb::jni::guard::string
qdb::jni::string::get_chars(qdb::jni::env & env, jstring str) {
    if (str != NULL) {
        return std::move(
            qdb::jni::guard::string(env,
                                    str,
                                    env.instance().GetStringUTFChars(str, NULL)));
    } else {
        return std::move(
            qdb::jni::guard::string(env,
                                    str));
    }

}
