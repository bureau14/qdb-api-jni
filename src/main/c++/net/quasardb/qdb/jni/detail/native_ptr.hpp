#pragma once

#include <jni.h>

#include <qdb/client.h>
#include <qdb/ts.h>

namespace qdb
{
namespace jni
{
namespace native_ptr
{
// QuasarDB's API allocates memory on the native heap (i.e. not managed by the JVM), and
// we often want to pass this around: qdb_handle_t being the most obvious example of this.
//
// In Java, we represent these pointers using a `long` type, and then in native code
// reinterpret this into the original type. The functions below facilitate this logic, and
// ensure corner cases are properly checked.

// Java -> JNI
template <typename T>
inline jlong to_java(T ptr) {
  static_assert(sizeof(jlong) >= sizeof(T), "T is a pointer that can be represented using a jlong");
  return reinterpret_cast<jlong>(ptr);
}

// JNI -> Java
template <typename T>
inline T from_java(jlong ptr) {
  return reinterpret_cast<T>(ptr);
}

};
};
};
