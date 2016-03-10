qdb_error_t qdb_blob_put(qdb_handle_t handle, const char * alias, const char * content, size_t content_length, qdb_time_t expiry_time);

%inline%{

retval qdb_blob_get(qdb_handle_t handle,  const char * alias, error_carrier * err)
{
    retval res;
    const void * buf = res.buffer;
    err->error = qdb_blob_get(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = static_cast<char *>(const_cast<void *>(buf));
    }
    return res;
}

retval qdb_blob_get_and_remove(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const void * buf = res.buffer;
    err->error = qdb_blob_get_and_remove(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = static_cast<char *>(const_cast<void *>(buf));
    }
    return res;
}

retval qdb_blob_get_and_update(qdb_handle_t handle, const char * alias, const char * content, size_t content_length, qdb_time_t expiry_time, error_carrier * err)
{
    retval res;
    const void * buf = res.buffer;
    err->error = qdb_blob_get_and_update(handle, alias, content, content_length, expiry_time, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = static_cast<char *>(const_cast<void *>(buf));
    }
    return res;
}

retval qdb_blob_compare_and_swap(qdb_handle_t handle, const char * alias, const char * content, size_t content_length, const char * comparand, size_t comparand_length, qdb_time_t expiry_time, error_carrier * err)
{
    retval res;
    const void * buf = res.buffer;
    err->error = qdb_blob_compare_and_swap(handle, alias, content, content_length, comparand, comparand_length, expiry_time, &buf, &res.buffer_size);
    if (err->error == qdb_e_unmatched_content)
    {
        res.buffer = static_cast<char *>(const_cast<void *>(buf));
    }
    return res;
}

%}

qdb_error_t qdb_blob_update(qdb_handle_t handle, const char * alias, const char * content, size_t content_length, qdb_time_t expiry_time);
qdb_error_t qdb_blob_remove_if(qdb_handle_t handle, const char * alias, const char * comparand, size_t comparand_length);
