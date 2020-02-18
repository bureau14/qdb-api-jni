#pragma once

#include <algorithm>
#include <jni.h>
#include <qdb/client.h>
#include <qdb/error.h>
#include <string>

namespace qdb
{
namespace jni
{

class env;

/**
 * Wraps around all JNI operations that relate to string management.
 */
class exception
{
  public:
    explicit exception(qdb_error_t err, std::string msg) noexcept
        : _error{err}, _msg{msg}
    {
    }

    /**
     * Utility function which allows us to put an exception on the JNI
     * stack.
     */
    void throw_new(jni::env &env) const noexcept;

    /**
     * Utility function that throws an appropriate c++ exception in case
     * an error has occured. This function should be invoked right after
     * every qdb api call that returns a qdb_error_t.
     *
     * Returns the exact same error code as provided, to promote chained
     * usage (e.g. qdb_error_t result = throw_if_error(h, qdb_query(...))).
     */
    static qdb_error_t throw_if_error(qdb_handle_t h, qdb_error_t e);

    /**
     * Similar to `throw_if_error`, but accepts an additional list of
     * allowed exceptions.
     */
    static qdb_error_t
    throw_if_error(qdb_handle_t h,
                   std::initializer_list<qdb_error_t> xs,
                   qdb_error_t e)
    {

        // TODO(leon): we can probably do this entirely at compile-time, as the
        // initializer list should be static.
        auto compare = [e](qdb_error_t x) { return x == e; };
        auto result = std::any_of(xs.begin(), xs.end(), compare);

        if (result)
        {
            return e;
        }

        return throw_if_error(h, e);
    }

  public:
    qdb_error_t
    error() const noexcept
    {
        return _error;
    }

    std::string const &
    what() const noexcept
    {
        return _msg;
    }

  private:
    qdb_error_t _error{qdb_e_ok};
    std::string _msg;
};
}; // namespace jni
}; // namespace qdb
