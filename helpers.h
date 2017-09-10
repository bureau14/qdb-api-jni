#pragma once

#include <jni.h>
#include <qdb/client.h>

void setReferenceValue(JNIEnv *env, jobject reference, jobject value);
void setByteBuffer(JNIEnv *, jobject, const void *, jlong);
void setLong(JNIEnv *, jobject, jlong);
void setInteger(JNIEnv *, jobject, jint);
void setString(JNIEnv *, jobject, const char *);
void setStringArray(JNIEnv *, jobject, const char **, size_t);
void timespecToNatve(JNIEnv *, jobject, qdb_timespec_t *);

class StringUTFChars {
  JNIEnv *_env;
  jstring _str;
  const char *_ptr;

public:
  StringUTFChars(JNIEnv *env, jstring str) : _env(env), _str(str), _ptr(0) {
    if (str)
      _ptr = env->GetStringUTFChars(str, NULL);
  }

  ~StringUTFChars() {
    if (_ptr)
      _env->ReleaseStringUTFChars(_str, _ptr);
  }

  operator const char *() const {
    return _ptr;
  }
};
