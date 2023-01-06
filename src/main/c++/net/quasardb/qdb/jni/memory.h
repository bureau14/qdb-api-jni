#pragma once

#include "exception.h"
#include <qdb/client.h>
#include <assert.h>

namespace qdb::jni::memory
{

/**
 * Allocates buffer using QDB API's TBB buffers. Allocates enough memory to fit `n` objects of type
 * `T`, i.e. sizeof(T) * n size.
 */
template <typename T>
inline T * allocate(qdb_handle_t handle, qdb_size_t n)
{
    assert(n > 0);
    void * ret{nullptr};
    qdb::jni::exception::throw_if_error(handle, qdb_alloc_buffer(handle, n * sizeof(T), &ret));
    assert(ret != nullptr);
    return static_cast<T *>(ret);
}

}; // namespace qdb::jni::memory
