#pragma once

#include <jni.h>

#include "guard/string.h"

namespace qdb {
    namespace jni {

        class env;

        /**
         * Wraps around all JNI operations that relate to string management. We
         * use the UTF-8 encoding for all string operations.
         */
        class string {
        public:

            static jni::guard::string
            get_chars(qdb::jni::env & env, jstring str);


        };
    };
};
