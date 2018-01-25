#pragma once

#include <jni.h>
#include <qdb/query.h>


namespace qdb {
  namespace jni {
    class env;
  };
};

namespace qdb {
  class value {

  public:
    static jobject
    from_native(qdb::jni::env & env, qdb_point_result_t const & input);


  private:
    static jobject
    _from_native_int64(qdb::jni::env & env, qdb_point_result_t const & input);

    static jobject
    _from_native_double(qdb::jni::env & env, qdb_point_result_t const & input);

    static jobject
    _from_native_timestamp(qdb::jni::env & env, qdb_point_result_t const & input);

    static jobject
    _from_native_blob(qdb::jni::env & env, qdb_point_result_t const & input);

    static jobject
    _from_native_null(qdb::jni::env & env);


  };
};
