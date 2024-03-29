#pragma once

#include <qdb/client.h>
#include <jni.h>

namespace qdb
{
namespace jni
{
class env;
};
}; // namespace qdb

jobject getReferenceValue(qdb::jni::env & env, jobject reference);
void setReferenceValue(qdb::jni::env & env, jobject reference, jobject value);
void setByteBuffer(qdb::jni::env & env, qdb_handle_t handle, jobject, const void *, jsize);
void setLong(qdb::jni::env & env, jobject, jlong);
void setInteger(qdb::jni::env & env, jobject, jint);
void setString(qdb::jni::env & env, jobject, const char *);
void setStringArray(qdb::jni::env & env, jobject, const char **, size_t);
