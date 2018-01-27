#pragma once

#include <jni.h>

#include "guard/local_ref.h"
#include "guard/string.h"
#include "introspect.h"

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

            static jni::guard::local_ref<jstring>
            create(jni::env & env, char const * str);

            /**
             * Wraps around introspect functions to look up the string class. Avoids
             * some boilerplate for object signatures.
             */
            static jclass
            lookup_class(env & env) {
                return jni::introspect::lookup_class(env, "java/lang/String");
            }

            /**
             * Wraps around introspect functions to look up a string field. Avoids
             * some boilerplate for object signatures.
             */
            static jfieldID
            lookup_field(env & env, jclass objectClass, char const * alias) {
                return jni::introspect::lookup_field(env, objectClass, alias, "Ljava/lang/String;");
            }

        };
    };
};
