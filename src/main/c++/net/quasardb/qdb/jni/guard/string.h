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
            class string {
            private:
                qdb::jni::env & _env;
                jstring & _str;
                char const * _ptr;

            public:
                string(qdb::jni::env & env, jstring & str, char const * ptr) :
                    _env (env),
                    _str (str),
                    _ptr (ptr) {
                }

                ~string() {
                    if (_ptr != NULL) {
                        _env.instance().ReleaseStringUTFChars(_str, _ptr);
                    }
                }

                string(string && o) noexcept
                    : _env(o._env),
                      _str(o._str),
                      _ptr(o._ptr) {
                    // By setting the other ptr to NULL, we're now effectively
                    // claiming ownership of the char *.
                    o._ptr = NULL;
                }

                string(string const &) = delete;
                string & operator=(string const &) = delete;

                /**
                 * Provide automatic casting to char const *, so that it can be
                 * used as if it were a regular char array.
                 */
                operator char const *() const {
                    return _ptr;
                }
            };
        };
    };
};
