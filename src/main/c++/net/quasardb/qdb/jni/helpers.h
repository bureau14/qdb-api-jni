#pragma once

#include <jni.h>
#include <qdb/client.h>

#include "env.h"

jobject getReferenceValue(qdb::jni::env & env, jobject reference);
void setReferenceValue(qdb::jni::env & env, jobject reference, jobject value);
void setByteBuffer(qdb::jni::env & env, jobject, const void *, jlong);
void setLong(qdb::jni::env & env, jobject, jlong);
void setInteger(qdb::jni::env & env, jobject, jint);
void setString(qdb::jni::env & env, jobject, const char *);
void setStringArray(qdb::jni::env & env, jobject, const char **, size_t);

class StringUTFChars {
  qdb::jni::env & _env;
  jstring _str;
  const char *_ptr;

public:
  StringUTFChars(qdb::jni::env & env, jstring str) : _env(env), _str(str), _ptr(0) {
    if (str)
      _ptr = env.instance().GetStringUTFChars(str, NULL);
  }

  ~StringUTFChars() {
    if (_ptr)
      _env.instance().ReleaseStringUTFChars(_str, _ptr);
  }

  operator const char *() const {
    return _ptr;
  }
};
