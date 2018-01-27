#pragma once

#include <jni.h>

#include "guard/local_ref.h"
#include "introspect.h"
#include "env.h"

namespace qdb {
    namespace jni {

        class env;

        /**
         * Wraps around all JNI operations that relate to object management.
         */
        class object {
        public:

            /**
             * Creates new object.
             */
            static jni::guard::local_ref<jobject>
            create(jni::env & env, jclass objectClass, jmethodID constructor) {
                return std::move(
                    jni::guard::local_ref<jobject>(
                        env,
                        env.instance().NewObject(objectClass, constructor)));
            }

            /**
             * Create a new object by its class name and constructor signature.
             *
             * \param objectClass The class of the object you're trying to create
             * \param signature   Signature of constructor to use, e.g. "(JJ)V" for a constructor
             *                    that accepts two long integers.
             */
            static jni::guard::local_ref<jobject>
            create(jni::env & env, jclass objectClass, char const * signature) {
                return create(env,
                              objectClass,
                              introspect::lookup_method(env, objectClass, "<init>", signature));
            }

            /**
             * Create a new object by its class name and constructor signature.
             *
             * \param className A fully qualified class name, such as "net/quasardb/qdb/ts/Result
             * \param signature Signature of constructor to use, e.g. "(JJ)V" for a constructor
             *                  that accepts two long integers.
             */
            static jni::guard::local_ref<jobject>
            create(jni::env & env, char const * className, char const * signature) {
                return create(env,
                              introspect::lookup_class(env, className),
                              signature);
            }
        };
    };
};
