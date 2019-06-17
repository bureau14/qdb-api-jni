#pragma once

#include <jni.h>

#include "guard/local_ref.h"
#include "guard/string_utf8.h"
#include "guard/string_critical.h"
#include "introspect.h"

namespace qdb {
    namespace jni {

        class env;

        /**
         * Wraps around all JNI operations that relate to string management.
         */
        class string {
        public:

            /**
             * Returns characters in modified UTF-8 encoding. This might create
             * a copy of the underlying string.
             */
            static jni::guard::string_utf8
            get_chars_utf8(qdb::jni::env & env, jstring str);

            /**
             * Returns a 'critical' reference to the jstring's characters. If
             * possible, returns a copy-less raw pointer to the internal representation,
             * but this comes with severe restrictions (i.e. GC is completely halted,
             * and you cannot call JVM operations until the critical ref is released).
             *
             * However, whenever possible, try to see whether it's possible to
             * use this function instead of the others.
             */
            static jni::guard::string_critical
            get_chars_critical(qdb::jni::env & env, jstring str);


            static jni::guard::local_ref<jstring>
            create(jni::env & env, char const * str, jsize len);

            static jni::guard::local_ref<jstring>
            create_utf8(jni::env & env, char const * str);

            /**
             * Create new array of Strings with certain size.
             */
            static jni::guard::local_ref<jobjectArray>
            create_array(jni::env & env,
                         jsize size);

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
