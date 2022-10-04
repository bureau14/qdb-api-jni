#pragma once

#include "env.h"
#include "introspect.h"
#include "string.h"
#include "guard/local_ref.h"
#include <assert.h>
#include <jni.h>

namespace qdb::jni::object
{

/**
 * Access to the class of a java object.
 */
inline jclass get_class(jni::env & env, jobject object)
{
    assert(object != nullptr);

    jclass result = env.instance().GetObjectClass(object);
    assert(result != nullptr);
    return result;
}

/**
 * Creates new object by its class and constructor.
 */
template <typename... Params>
inline jni::guard::local_ref<jobject> create(
    jni::env & env, jclass objectClass, jmethodID constructor, Params... params)
{
    assert(objectClass != nullptr);
    assert(constructor != nullptr);

    jobject ret = env.instance().NewObject(objectClass, constructor, params...);
    assert(ret != nullptr);

    return jni::guard::local_ref<jobject>(env, ret);
}

/**
 * Create a new object by its class and constructor signature.
 *
 * \param objectClass The class of the object you're trying to create
 * \param signature   Signature of constructor to use, e.g. "(JJ)V" for a
 * constructor that accepts two long integers.
 */
template <typename... Params>
inline jni::guard::local_ref<jobject> create(
    jni::env & env, jclass objectClass, char const * signature, Params... params)
{
    assert(objectClass != nullptr);

    return create(env, objectClass,
        introspect::lookup_method(env, objectClass, "<init>", signature), params...);
}

/**
 * Create a new object by its class name and constructor signature.
 *
 * \param className A fully qualified class name, such as
 * "net/quasardb/qdb/ts/Result \param signature Signature of constructor to
 * use, e.g. "(JJ)V" for a constructor that accepts two long integers.
 */
template <typename... Params>
inline jni::guard::local_ref<jobject> create(
    jni::env & env, char const * className, char const * signature, Params... params)
{
    return create(env, introspect::lookup_class(env, className), signature, params...);
}

/**
 * Create a new object array with a specific size and type.
 */
inline jni::guard::local_ref<jobjectArray> create_array(
    jni::env & env, jsize size, jclass objectClass)
{
    assert(objectClass != nullptr);

    return jni::guard::local_ref<jobjectArray>(
        env, env.instance().NewObjectArray(size, objectClass, nullptr));
}

/**
 * Create a new object array with a specific size and type. Automatically
 * looks up className using introspection, will throw assertion error when
 * not found.
 */
inline jni::guard::local_ref<jobjectArray> create_array(
    jni::env & env, jsize size, char const * className)
{
    return create_array(env, size, introspect::lookup_class(env, className));
}

/**
 * Calls function that returns an object.
 */
template <typename... Params>
inline jni::guard::local_ref<jobject> call_method(
    jni::env & env, jobject object, jmethodID method, Params... params)
{
    assert(object != nullptr);
    assert(method != nullptr);

    return jni::guard::local_ref<jobject>(
        env, env.instance().CallObjectMethod(object, method, params...));
}

/**
 * Calls function that returns an object.
 */
template <typename... Params>
inline jni::guard::local_ref<jobject> call_method(
    jni::env & env, jobject object, char const * alias, char const * signature, Params... params)
{
    assert(object != nullptr);
    return call_method(env, object,
        introspect::lookup_method(env, get_class(env, object), alias, signature), params...);
}

/**
 * Calls static function that returns an object.
 */
template <typename... Params>
inline jni::guard::local_ref<jobject> call_static_method(
    jni::env & env, jclass objectClass, jmethodID method, Params... params)
{
    assert(objectClass != nullptr);
    assert(method != nullptr);

    return jni::guard::local_ref<jobject>(
        env, env.instance().CallStaticObjectMethod(objectClass, method, params...));
}

/**
 * Calls static function that returns an object.
 */
template <typename... Params>
inline jni::guard::local_ref<jobject> call_static_method(jni::env & env,
    jclass objectClass,
    char const * alias,
    char const * signature,
    Params... params)
{
    assert(objectClass != nullptr);

    return call_static_method(env, objectClass,
        introspect::lookup_static_method(env, objectClass, alias, signature), params...);
}

/**
 * Calls static function that returns an object.
 */
template <typename... Params>
inline jni::guard::local_ref<jobject> call_static_method(jni::env & env,
    char const * className,
    char const * alias,
    char const * signature,
    Params... params)
{
    return call_static_method(
        env, introspect::lookup_class(env, className), alias, signature, params...);
}

/**
 * Acquires an object from a static object field.
 */
inline jni::guard::local_ref<jobject> from_static_field(
    jni::env & env, jclass objectClass, jfieldID field)
{
    assert(objectClass != nullptr);
    assert(field != nullptr);

    return jni::guard::local_ref<jobject>(
        env, env.instance().GetStaticObjectField(objectClass, field));
}

/**
 * Acquires an object from another object's field.
 */
template <typename JNIType>
inline jni::guard::local_ref<JNIType> from_field(jni::env & env, jobject object, jfieldID field)
{
    assert(object != nullptr);
    assert(field != nullptr);

    return jni::guard::local_ref<JNIType>(
        env, reinterpret_cast<JNIType>(env.instance().GetObjectField(object, field)));
}

/**
 * Acquires an object from another object's field.
 */
template <typename JNIType>
inline jni::guard::local_ref<JNIType> from_field(
    jni::env & env, jobject object, char const * alias, char const * signature)
{
    return from_field<JNIType>(env, object,
        introspect::lookup_field(env, object::get_class(env, object), alias, signature));
}

/**
 * Invokes `.toString()` on the object and returns the resulting string.
 * For debugging purposes.
 */
inline jni::guard::string_utf8 to_string(jni::env & env, qdb_handle_t handle, jobject object)
{
    return jni::string::get_chars_utf8(
        env, handle, call_method(env, object, "toString", "()Ljava/lang/String;"));
}

}; // namespace qdb::jni::object
