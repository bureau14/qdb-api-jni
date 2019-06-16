#pragma once

#include <assert.h>
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
             * Creates new object by its class and constructor.
             */
            template <typename ...Params>
            static jni::guard::local_ref<jobject>
            create(jni::env & env, jclass objectClass, jmethodID constructor, Params... params) {
                assert(objectClass != NULL);
                assert(constructor != NULL);

                return std::move(
                    jni::guard::local_ref<jobject>(
                        env,
                        env.instance().NewObject(objectClass, constructor, params...)));
            }

            /**
             * Create a new object by its class and constructor signature.
             *
             * \param objectClass The class of the object you're trying to create
             * \param signature   Signature of constructor to use, e.g. "(JJ)V" for a constructor
             *                    that accepts two long integers.
             */
            template <typename ...Params>
            static jni::guard::local_ref<jobject>
            create(jni::env & env, jclass objectClass, char const * signature,  Params... params) {
                assert(objectClass != NULL);

                return create(env,
                              objectClass,
                              introspect::lookup_method(env, objectClass, "<init>", signature),
                              params...);
            }

            /**
             * Create a new object by its class name and constructor signature.
             *
             * \param className A fully qualified class name, such as "net/quasardb/qdb/ts/Result
             * \param signature Signature of constructor to use, e.g. "(JJ)V" for a constructor
             *                  that accepts two long integers.
             */
            template <typename ...Params>
            static jni::guard::local_ref<jobject>
            create(jni::env & env, char const * className, char const * signature, Params... params) {
                return create(env,
                              introspect::lookup_class(env, className),
                              signature,
                              params...);
            }

            /**
             * Create a new object array with a specific size and type.
             */
            static jni::guard::local_ref<jobjectArray>
            create_array(jni::env & env, jsize size, jclass objectClass) {
                assert(objectClass != NULL);

                return std::move(
                    jni::guard::local_ref<jobjectArray>(
                        env,
                        env.instance().NewObjectArray(size, objectClass, NULL)));
            }

            /**
             * Create a new object array with a specific size and type. Automatically
             * looks up className using introspection, will throw assertion error when
             * not found.
             */
            static jni::guard::local_ref<jobjectArray>
            create_array(jni::env & env, jsize size, char const * className) {
                return create_array(env,
                                    size,
                                    introspect::lookup_class(env, className));
            }

            /**
             * Sets the element of a jobjectArray at a specific location.
             */


            /**
             * Calls static function that returns an object.
             */
            template <typename ...Params>
            static jni::guard::local_ref<jobject>
            call_static_method(jni::env & env, jclass objectClass, jmethodID method, Params... params) {
                assert(objectClass != NULL);
                assert(method != NULL);

                return std::move(
                    jni::guard::local_ref<jobject>(
                        env,
                        env.instance().CallStaticObjectMethod(objectClass,
                                                              method,
                                                              params...)));
            }

            /**
             * Calls static function that returns an object.
             */
            template <typename ...Params>
            static jni::guard::local_ref<jobject>
            call_static_method(jni::env & env, jclass objectClass, char const * alias, char const * signature, Params... params) {
                assert(objectClass != NULL);

                return call_static_method(env,
                                          objectClass,
                                          introspect::lookup_static_method(env,
                                                                           objectClass,
                                                                           alias,
                                                                           signature),
                                          params...);
            }

            /**
             * Calls static function that returns an object.
             */
            template <typename ...Params>
            static jni::guard::local_ref<jobject>
            call_static_method(jni::env & env, char const * className, char const * alias, char const * signature, Params... params) {
                return call_static_method(env,
                                          introspect::lookup_class(env, className),
                                          alias,
                                          signature,
                                          params...);
            }

            /**
             * Acquires an object from a static object field.
             */
            static jni::guard::local_ref <jobject>
            from_static_field(jni::env & env, jclass objectClass, jfieldID field) {
              assert (objectClass != NULL);
              assert (field != NULL);

              return std::move(
                  jni::guard::local_ref<jobject>(
                      env,
                      env.instance().GetStaticObjectField(objectClass, field)));
            }
        };
    };
};
