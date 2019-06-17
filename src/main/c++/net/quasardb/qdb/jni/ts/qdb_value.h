#pragma once

#include <jni.h>
#include <qdb/query.h>

#include "../guard/local_ref.h"

namespace qdb {
    namespace jni {
        class env;
    };
};

namespace qdb {
    namespace jni {
        namespace ts {

            class value {

            public:
                static jni::guard::local_ref<jobject>
                from_native(qdb::jni::env & env, qdb_point_result_t const & input);


            private:
                static jni::guard::local_ref<jobject>
                _from_native_int64(qdb::jni::env & env, qdb_point_result_t const & input);

                static jni::guard::local_ref<jobject>
                _from_native_count(qdb::jni::env & env, qdb_point_result_t const & input);

                static jni::guard::local_ref<jobject>
                _from_native_double(qdb::jni::env & env, qdb_point_result_t const & input);

                static jni::guard::local_ref<jobject>
                _from_native_timestamp(qdb::jni::env & env, qdb_point_result_t const & input);

                static jni::guard::local_ref<jobject>
                _from_native_blob(qdb::jni::env & env, qdb_point_result_t const & input);

                static jni::guard::local_ref<jobject>
                _from_native_null(qdb::jni::env & env);

            };
        };
    };
};
