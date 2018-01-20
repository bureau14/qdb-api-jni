#pragma once

#include <jni.h>
#include <qdb/query.h>

namespace qdb {
  namespace value {

    jobject
    from_native(JNIEnv * env, qdb_point_result_t const & input);

  };
};
