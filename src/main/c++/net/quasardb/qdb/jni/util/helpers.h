#pragma once

#include <jni.h>
#include <qdb/client.h>

namespace qdb {
  namespace jni {
    class env;
  };
};

jobject getReferenceValue(qdb::jni::env & env, jobject reference);
void setReferenceValue(qdb::jni::env & env, jobject reference, jobject value);
void setByteBuffer(qdb::jni::env & env, jobject, const void *, jlong);
void setLong(qdb::jni::env & env, jobject, jlong);
void setInteger(qdb::jni::env & env, jobject, jint);
void setString(qdb::jni::env & env, jobject, const char *);
void setStringArray(qdb::jni::env & env, jobject, const char **, size_t);
