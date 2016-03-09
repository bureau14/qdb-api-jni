qdb_error_t qdb_deque_push_front(qdb_handle_t handle,  const char * alias,  const char * content, size_t content_length);

qdb_error_t qdb_deque_push_back(qdb_handle_t handle,   const char * alias,  const char * content, size_t content_length);

%inline%{

size_t qdb_deque_size(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    size_t res = 0;
    err->error = qdb_deque_size(handle, alias, &res);
    return res;
}

retval qdb_deque_get_at(qdb_handle_t handle, const char * alias, jlong index, error_carrier * err)
{
    retval res;
    const void * buf = NULL;
    err->error = qdb_deque_get_at(handle, alias, static_cast<long>(index), &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = static_cast<char *>(const_cast<void *>(buf));
    }
    return res;
}

retval qdb_deque_pop_front(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const void * buf = NULL;
    err->error = qdb_deque_pop_front(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = static_cast<char *>(const_cast<void *>(buf));
    }
    return res;
}

retval qdb_deque_pop_back(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const void * buf = NULL;
    err->error = qdb_deque_pop_back(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = static_cast<char *>(const_cast<void *>(buf));
    }
    return res;
}

retval qdb_deque_front(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const void * buf = NULL;
    err->error = qdb_deque_front(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = static_cast<char *>(const_cast<void *>(buf));
    }
    return res;
}

retval qdb_deque_back(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const void * buf = NULL;
    err->error = qdb_deque_back(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = static_cast<char *>(const_cast<void *>(buf));
    }
    return res;
}

%}
