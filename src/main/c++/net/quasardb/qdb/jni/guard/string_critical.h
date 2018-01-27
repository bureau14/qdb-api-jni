#pragma once

#include <stdio.h>
#include <memory>
#include <jni.h>

#include "../env.h"

namespace qdb {
    namespace jni {
        namespace guard {

            /**
             * Helper class that wraps around a jstring's char *, so that they
             * can be released back to the JVM as soon as possible.
             */
            class string_critical {
            private:
                qdb::jni::env & _env;
                jstring & _str;
                jchar const * _ptr;

            public:
                /**
                 * Constructor. Assumes `ptr` is acquired through
                 * env->GetString_CriticalUTFChars, and will ensure the reference
                 * is released when necessary.
                 */
                string_critical(qdb::jni::env & env, jstring & str, jchar const * ptr) :
                    _env (env),
                    _str (str),
                    _ptr (ptr) {
                }

                string_critical(string_critical && o) noexcept
                    : _env(o._env),
                      _str(o._str),
                      _ptr(o._ptr) {
                    // By setting the other ptr to NULL, we're now effectively
                    // claiming ownership of the char *.
                    o._ptr = NULL;
                }

                ~string_critical() {
                    if (_ptr != NULL) {
                        _env.instance().ReleaseStringCritical(_str, _ptr);
                    }
                }

                string_critical(string_critical const &) = delete;
                string_critical & operator=(string_critical const &) = delete;

                /**
                 * Provide automatic casting to jchar const *, so that it can be
                 * used as if it were a regular jchar array.
                 */
                operator jchar const *() const {
                    return _ptr;
                }
            };
        };
    };
};
