#pragma once

#include <jni.h>
#include <qdb/query.h>

namespace qdb {
  class value {

  public:
    static jobject
    from_native(JNIEnv * env, qdb_point_result_t const & input);


  private:
    static jobject
    _from_native_int64(JNIEnv * env, qdb_point_result_t const & input);

    static jobject
    _from_native_double(JNIEnv * env, qdb_point_result_t const & input);

    static jobject
    _from_native_timestamp(JNIEnv * env, qdb_point_result_t const & input);

    static jobject
    _from_native_blob(JNIEnv * env, qdb_point_result_t const & input);

    static jobject
    _from_native_null(JNIEnv * env);


  };
};
