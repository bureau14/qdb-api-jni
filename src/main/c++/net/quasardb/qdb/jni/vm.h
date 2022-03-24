#pragma once

#include <cassert>
#include <jni.h>
#include <utility>

namespace qdb
{
namespace jni
{

/**
 * Wrapper around a JavaVM pointer.
 *
 * TODO(leon): Subject to removal?
 */
class vm
{
private:
    JavaVM * _vm;

public:
    vm(JavaVM * p)
    {
        _vm = p;
    }

    vm(JavaVM & p)
        : vm(&p)
    {}

    vm(JNIEnv * env);
};
}; // namespace jni
}; // namespace qdb
