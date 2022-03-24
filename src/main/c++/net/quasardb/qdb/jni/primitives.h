#pragma once

#include "env.h"
#include <qdb/ts.h>
#include <jni.h>

namespace qdb
{
namespace jni
{
namespace primitives
{

template <typename T>
inline T get_int_as(env & env, jobject input, jfieldID field)
{
    static_assert(sizeof(T) >= sizeof(jint));
    return static_cast<T>(env.instance().GetIntField(input, field));
}

inline qdb_int_t get_int(env & env, jobject input, jfieldID field)
{
    return get_int_as<qdb_int_t>(env, input, field);
}

}; // namespace primitives
}; // namespace jni
}; // namespace qdb
