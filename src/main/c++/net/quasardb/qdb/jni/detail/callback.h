#pragma once

#include "../exception.h"
#include "native_ptr.h"
#include <qdb/client.h>
#include <jni.h>

namespace qdb
{
namespace jni
{
namespace callback
{

template <typename ReturnType>
inline jlong ptr_constructor(JNIEnv * env_,
    jlong handle_,
    std::function<ReturnType(qdb::jni::env &, qdb_handle_t handle)> fn)
{
    qdb::jni::env env(env_);
    qdb_handle_t handle = jni::native_ptr::from_java<qdb_handle_t>(handle_);

    try
    {
        return jni::native_ptr::to_java<ReturnType>(fn(env, handle));
    }
    catch (jni::exception const & e)
    {
        e.throw_new(env);
        return -1;
    }
};

}; // namespace callback
}; // namespace jni
}; // namespace qdb
