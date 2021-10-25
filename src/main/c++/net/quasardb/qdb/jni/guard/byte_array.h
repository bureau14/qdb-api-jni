#pragma once

#include <jni.h>
#include <memory>
#include <stdio.h>

#include "../env.h"

namespace qdb
{
namespace jni
{
namespace guard
{

/**
 * Helper class that wraps around a jbyteArray, so that we
 * manage the access and that the memory can be released
 *  back to the JVM as soon as possible.
 */
class byte_array
{
  private:
    qdb::jni::env &_env;
    jbyteArray _bb;
    jbyte * _ptr;
    qdb_size_t _len;

  public:

    /**
     * Constructor. Assumes `ptr` is acquired through
     * env->GetStringUTFChars, and will ensure the reference
     * is released when necessary.
     */
    byte_array(qdb::jni::env &env, jbyteArray & bb)
      : _env(env), _bb(bb), _ptr(env.instance().GetByteArrayElements(bb, 0))
    {
      _len = _env.instance().GetArrayLength(_bb);
    }

    byte_array(byte_array &&o) noexcept
      : _env(o._env), _bb(o._bb), _ptr(o._ptr), _len(o._len)
    {
        // By setting the other ptr to NULL, we're now effectively
        // claiming ownership of the char *.
        o._ptr = NULL;
    }

    ~byte_array()
    {
        if (_ptr != NULL)
        {
          _env.instance().ReleaseByteArrayElements(_bb, _ptr, 0);
          _ptr = NULL;
        }
    }

    byte_array(byte_array const &) = delete;
    byte_array &operator=(byte_array const &) = delete;


    jbyte const * ptr() const {
      return _ptr;
    }

    qdb_size_t len() const {
      return _len;
    }


    /**
     */
    jbyte * copy() const {
      jbyte * ret = new jbyte[_len];
      memcpy(ret, _ptr, _len);
      return ret;
    }

    jbyte * copy_nullterminated() const {
      jbyte * ret = new jbyte[_len + 1];
      memcpy(ret, _ptr, _len);
      ret[_len] = '\0';
      return ret;
    }

    /**
     * Creates a copy of this byte array and returns it as a qdb_blob.
     */
    void as_qdb_blob(qdb_blob_t & out) const {
      out.content = copy();
      out.content_length = _len;
    }


    /**
     * Creates a copy of this byte array and returns it as a qdb_string_t.
     */
    qdb_string_t * as_qdb_string_ptr() const {
      return new qdb_string_t {(char *)(copy_nullterminated()),
                               _len};
    }


};
}; // namespace guard
}; // namespace jni
}; // namespace qdb
