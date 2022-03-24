#pragma once

#include "env.h"
#include "guard/local_ref.h"
#include <jni.h>

namespace qdb
{
namespace jni
{

class env;

/**
 * Helper classes that pushes and pops a local frame to the JVM.
 *
 * By default, the JVM only allocates 16 registers for local references
 * for any native method to use. When you expect to create/reference more
 * objects than that, you either need to adjust the capacity of the current
 * frame, or push new local frames with enough capacity to the JVM and pop
 * them when you're done.
 *
 * The latter option is where this class comes into play, and matches
 * QuasarDB's access patterns very well. One pattern that is very common
 * is when traversing over arrays/rows:
 *
 *  - you initialise a jobjectArray with a certain size N
 *  - you push a new local frame with the same size N
 *  - you create and add N objects to the array
 *  - you pop the local frame, and return the array
 *
 *  This pattern ensures "perfect" local frame capacity for the operations
 *  you're planning to perform.
 */
class local_frame
{
private:
    jni::env & _env;
    bool _popped;

public:
    local_frame(jni::env & env)
        : _env(env)
        , _popped(false)
    {}

    ~local_frame()
    {
        if (_popped == false)
        {
            _env.instance().PopLocalFrame(NULL);
            _popped = true;
        }
    }

    /**
     * Pops the current active local frame, and returns a local reference
     * to `result` in the previous frame.
     *
     * This will effectively free all the local references in the current
     * frame.
     */
    template <typename JNIType>
    jni::guard::local_ref<JNIType> pop(JNIType result)
    {
        assert(_popped == false); // can't pop the same frame twice
        _popped = true;

        // Note how `result` is actually moved from the current frame to
        // the previous frame here -- both `result` and the return value
        // of this function refer to the same object in the JVM, but both
        // from different frames.
        return std::move(
            jni::guard::local_ref<JNIType>(_env, (JNIType)(_env.instance().PopLocalFrame(result))));
    }

    static local_frame push(jni::env & env, jsize size)
    {
        [[maybe_unused]] jint err = env.instance().PushLocalFrame(size);
        assert(err == 0);

        return local_frame(env);
    }
};
}; // namespace jni
}; // namespace qdb
